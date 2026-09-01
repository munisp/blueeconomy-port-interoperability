// booking-worker runs the Temporal worker for ECallUpBookingWorkflow and
// ECallUpCallUpWorkflow, plus the call-up queue sweeper (grace-window
// forfeiture chain and idempotent call-up workflow starts) across every
// active tenant — or only WORKER_TENANT_ID when that optional restriction is
// set. It fails closed unless Temporal, PostgreSQL and TigerBeetle are all
// configured.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/customs"
	"github.com/munisp/blueeconomy-port-interoperability/internal/declarations"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/ledger"
	"github.com/munisp/blueeconomy-port-interoperability/internal/queue"
	"github.com/munisp/blueeconomy-port-interoperability/internal/registry"
	"github.com/munisp/blueeconomy-port-interoperability/internal/securechain"
	"github.com/munisp/blueeconomy-port-interoperability/internal/telemetry"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
)

func main() {
	if err := run(); err != nil {
		log.Printf("booking-worker: %v", err)
		os.Exit(1)
	}
}

func run() error {
	address := requiredEnv("TEMPORAL_ADDRESS")
	namespace := requiredEnv("TEMPORAL_NAMESPACE")
	taskQueue := requiredEnv("TEMPORAL_TASK_QUEUE")
	databaseURL := requiredEnv("DATABASE_URL")
	clusterID := requiredEnv("TIGERBEETLE_CLUSTER_ID")
	addresses := strings.Split(requiredEnv("TIGERBEETLE_ADDRESSES"), ",")
	// WORKER_TENANT_ID is an optional restriction: when set the sweeper only
	// serves that tenant; when unset it sweeps every active tenant.
	tenantID := os.Getenv("WORKER_TENANT_ID")
	graceMinutes, err := strconv.Atoi(defaultEnv("CALLUP_GRACE_MINUTES", "90"))
	if err != nil || graceMinutes < 1 {
		return errors.New("CALLUP_GRACE_MINUTES must be a positive integer")
	}
	// FGN_SHARE_BASIS_POINTS must match the API service exactly: the refund
	// rail reproduces the operator/FGN split when a paid booking expires.
	fgnShareBPS, err := strconv.ParseInt(requiredEnv("FGN_SHARE_BASIS_POINTS"), 10, 64)
	if err != nil || fgnShareBPS <= 0 || fgnShareBPS >= 10000 {
		return errors.New("FGN_SHARE_BASIS_POINTS must be an integer between 1 and 9999")
	}
	sweepSeconds, err := strconv.Atoi(defaultEnv("QUEUE_SWEEP_INTERVAL_SECONDS", "60"))
	if err != nil || sweepSeconds < 5 {
		return errors.New("QUEUE_SWEEP_INTERVAL_SECONDS must be an integer >= 5")
	}
	// QUEUE_STALE_AFTER_HOURS bounds how long a QUEUED entry may wait before
	// the sweeper expires it; default 72h matches a long port weekend.
	staleHours, err := strconv.Atoi(defaultEnv("QUEUE_STALE_AFTER_HOURS", "72"))
	if err != nil || staleHours < 1 {
		return errors.New("QUEUE_STALE_AFTER_HOURS must be a positive integer")
	}

	settlement, err := ledger.NewTigerBeetle(clusterID, addresses)
	if err != nil {
		return fmt.Errorf("configure TigerBeetle ledger: %w", err)
	}
	defer settlement.Close()

	ctx := context.Background()
	// Telemetry: disabled without OTEL_EXPORTER_OTLP_ENDPOINT (boot
	// unaffected); enabled export is async/batched, collector-down is
	// drop-with-metric, never a worker failure.
	telemetryConfig, err := telemetry.LoadConfig("blueeconomy-port-interoperability")
	if err != nil {
		return fmt.Errorf("load telemetry config: %w", err)
	}
	telemetryPipeline, err := telemetry.Setup(ctx, telemetryConfig)
	if err != nil {
		return fmt.Errorf("setup telemetry: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), telemetry.ShutdownFlushTimeout)
		defer cancel()
		if err := telemetryPipeline.Shutdown(shutdownCtx); err != nil {
			log.Printf("telemetry shutdown: %v", err)
		}
	}()

	pool, err := telemetry.NewPGXPool(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("ping postgres: %w", err)
	}
	defer pool.Close()

	// Envelope provenance signing is mandatory: lifecycle events emitted by
	// the workflow activities are JWS-signed (EdDSA over the JCS envelope).
	envelopeSigner, err := events.SignerFromEnv()
	if err != nil {
		return fmt.Errorf("configure envelope signer: %w", err)
	}
	bookingStore := booking.NewStore(pool, envelopeSigner)
	activities, err := booking.NewActivities(bookingStore, settlement)
	if err != nil {
		return err
	}
	// Nigeria Customs cross-validation is mandatory wiring: without a
	// fail-closed declaration client, declaration-carrying bookings could
	// never leave VALIDATION_PENDING.
	customsToleranceBPS, err := strconv.ParseInt(defaultEnv("CUSTOMS_WEIGHT_TOLERANCE_BPS", "500"), 10, 64)
	if err != nil || customsToleranceBPS < 0 {
		return errors.New("CUSTOMS_WEIGHT_TOLERANCE_BPS must be a non-negative integer")
	}
	customsTimeout, err := time.ParseDuration(defaultEnv("CUSTOMS_TIMEOUT", "10s"))
	if err != nil || customsTimeout <= 0 {
		return errors.New("CUSTOMS_TIMEOUT must be a positive duration")
	}
	customsValidator, err := buildCustomsValidator(pool, envelopeSigner, customsTimeout)
	if err != nil {
		return fmt.Errorf("configure customs validator: %w", err)
	}
	if err := activities.SetCustomsValidator(customsValidator, customsToleranceBPS); err != nil {
		return fmt.Errorf("wire customs validator: %w", err)
	}
	queueStore, err := queue.NewStore(pool, bookingStore, envelopeSigner, time.Duration(graceMinutes)*time.Minute)
	if err != nil {
		return fmt.Errorf("configure queue store: %w", err)
	}
	// Workflow-driven booking expiry also promotes the queue head in-tx.
	bookingStore.SetCapacityListener(queueStore)
	// Refund rail: paid bookings expired by the workflow/sweeper path post a
	// compensating settlement transfer before reaching REFUNDED.
	if err := bookingStore.SetRefundPoster(settlement, fgnShareBPS); err != nil {
		return fmt.Errorf("wire refund rail: %w", err)
	}
	callUpActivities, err := queue.NewCallUpActivities(queueStore)
	if err != nil {
		return fmt.Errorf("configure call-up activities: %w", err)
	}
	// Secure Chain (WP-7) expiry sweep: the per-tenant workflow expires
	// ACTIVE chains past their TTL and revokes their open links.
	secureChainStore, err := securechain.NewStore(pool, envelopeSigner, securechain.Config{})
	if err != nil {
		return fmt.Errorf("configure secure-chain store: %w", err)
	}
	expiryActivities, err := securechain.NewExpiryActivities(secureChainStore)
	if err != nil {
		return fmt.Errorf("configure secure-chain expiry activities: %w", err)
	}
	if err != nil {
		return err
	}
	// Temporal OTel interceptors: workflow/activity spans join the service
	// trace and inbound workflow calls extract the caller's context
	// (OTEL_DESIGN §3 Temporal row); no-op spans when telemetry is disabled.
	tracingInterceptor, err := temporalotel.NewTracingInterceptor(temporalotel.TracerOptions{})
	if err != nil {
		return fmt.Errorf("build temporal tracing interceptor: %w", err)
	}
	temporalClient, err := client.Dial(client.Options{
		HostPort:     address,
		Namespace:    namespace,
		Interceptors: []interceptor.ClientInterceptor{tracingInterceptor},
	})
	if err != nil {
		return fmt.Errorf("dial temporal: %w", err)
	}
	defer temporalClient.Close()
	callUps, err := queue.NewTemporalCallUpOrchestrator(address, namespace, taskQueue)
	if err != nil {
		return fmt.Errorf("configure call-up orchestrator: %w", err)
	}
	defer callUps.Close()

	bookingWorker := worker.New(temporalClient, taskQueue, worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{tracingInterceptor},
	})
	bookingWorker.RegisterWorkflow(booking.ECallUpBookingWorkflow)
	bookingWorker.RegisterWorkflow(queue.ECallUpCallUpWorkflow)
	bookingWorker.RegisterWorkflow(securechain.SecureChainExpiryWorkflow)
	bookingWorker.RegisterActivity(expiryActivities)
	bookingWorker.RegisterActivity(activities)
	bookingWorker.RegisterActivity(callUpActivities)

	sweeper, err := queue.NewSweeper(pool, queueStore, callUps, tenantID, time.Duration(staleHours)*time.Hour)
	if err != nil {
		return fmt.Errorf("configure call-up sweeper: %w", err)
	}

	// Phase 12: expired-certificate sweep follows the same multi-tenant
	// sweeper pattern as the call-up sweep.
	registryStore, err := registry.NewStore(pool, envelopeSigner)
	if err != nil {
		return fmt.Errorf("configure registry store for certificate sweep: %w", err)
	}
	certificateSweeper, err := registry.NewCertificateSweeper(pool, registryStore,
		registry.Principal{ID: "registry-sweeper", Role: "registry-officer"}, tenantID)
	if err != nil {
		return fmt.Errorf("configure certificate sweeper: %w", err)
	}

	sweepCtx, stopSweep := context.WithCancel(context.Background())
	defer stopSweep()
	go sweepCallUps(sweepCtx, sweeper, time.Duration(sweepSeconds)*time.Second)
	go sweepCertificates(sweepCtx, certificateSweeper, time.Duration(sweepSeconds)*time.Second)

	log.Printf("booking-worker polling task queue %s", taskQueue)
	if err := bookingWorker.Run(worker.InterruptCh()); err != nil {
		return fmt.Errorf("run worker: %w", err)
	}
	return nil
}

// sweepCallUps runs the multi-tenant call-up sweeper immediately and then on
// every interval tick. Sweep failures are logged and retried on the next
// tick; unreconciled call-ups remain visible to the next pass.
func sweepCallUps(ctx context.Context, sweeper *queue.Sweeper, interval time.Duration) {
	sweep := func() {
		if err := sweeper.SweepOnce(ctx); err != nil {
			log.Printf("call-up sweep: %v", err)
		}
	}
	sweep()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}

// sweepCertificates runs the multi-tenant seafarer-certificate expiry sweep
// immediately and then on every interval tick. Sweep failures are logged and
// retried on the next tick; lapsed certificates remain visible to the next
// pass and the verification endpoint already reports them EXPIRED.
func sweepCertificates(ctx context.Context, sweeper *registry.CertificateSweeper, interval time.Duration) {
	sweep := func() {
		if err := sweeper.SweepOnce(ctx); err != nil {
			log.Printf("certificate expiry sweep: %v", err)
		}
	}
	sweep()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}

// buildCustomsValidator selects the customs cross-validation backend.
// CUSTOMS_VALIDATOR_BACKEND is "http" (default, backwards compatible) or
// "local" (the platform declaration engine); any other value fails closed at
// startup.
func buildCustomsValidator(pool *pgxpool.Pool, signer *events.Signer, timeout time.Duration) (customs.Validator, error) {
	switch backend := defaultEnv("CUSTOMS_VALIDATOR_BACKEND", "http"); backend {
	case "http":
		return customs.NewHTTPValidator(customs.HTTPConfig{
			BaseURL:        requiredEnv("CUSTOMS_BASE_URL"),
			BearerToken:    os.Getenv("CUSTOMS_BEARER_TOKEN"),
			ClientCertFile: os.Getenv("CUSTOMS_CLIENT_CERT_FILE"),
			ClientKeyFile:  os.Getenv("CUSTOMS_CLIENT_KEY_FILE"),
			CACertFile:     os.Getenv("CUSTOMS_CA_CERT_FILE"),
			Timeout:        timeout,
		})
	case "local":
		return customs.NewLocalValidator(declarations.NewStore(pool, signer))
	default:
		return nil, fmt.Errorf("CUSTOMS_VALIDATOR_BACKEND must be \"http\" or \"local\", got %q", backend)
	}
}

func requiredEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("%s must be set", name)
	}
	return value
}

func defaultEnv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
