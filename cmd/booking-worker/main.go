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
	"github.com/munisp/blueeconomy-port-interoperability/internal/ledger"
	"github.com/munisp/blueeconomy-port-interoperability/internal/queue"
	"go.temporal.io/sdk/client"
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
	sweepSeconds, err := strconv.Atoi(defaultEnv("QUEUE_SWEEP_INTERVAL_SECONDS", "60"))
	if err != nil || sweepSeconds < 5 {
		return errors.New("QUEUE_SWEEP_INTERVAL_SECONDS must be an integer >= 5")
	}

	settlement, err := ledger.NewTigerBeetle(clusterID, addresses)
	if err != nil {
		return fmt.Errorf("configure TigerBeetle ledger: %w", err)
	}
	defer settlement.Close()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("ping postgres: %w", err)
	}
	defer pool.Close()

	bookingStore := booking.NewStore(pool)
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
	customsValidator, err := buildCustomsValidator(pool, customsTimeout)
	if err != nil {
		return fmt.Errorf("configure customs validator: %w", err)
	}
	if err := activities.SetCustomsValidator(customsValidator, customsToleranceBPS); err != nil {
		return fmt.Errorf("wire customs validator: %w", err)
	}
	queueStore, err := queue.NewStore(pool, bookingStore, time.Duration(graceMinutes)*time.Minute)
	if err != nil {
		return fmt.Errorf("configure queue store: %w", err)
	}
	// Workflow-driven booking expiry also promotes the queue head in-tx.
	bookingStore.SetCapacityListener(queueStore)
	callUpActivities, err := queue.NewCallUpActivities(queueStore)
	if err != nil {
		return err
	}
	temporalClient, err := client.Dial(client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		return fmt.Errorf("dial temporal: %w", err)
	}
	defer temporalClient.Close()
	callUps, err := queue.NewTemporalCallUpOrchestrator(address, namespace, taskQueue)
	if err != nil {
		return fmt.Errorf("configure call-up orchestrator: %w", err)
	}
	defer callUps.Close()

	bookingWorker := worker.New(temporalClient, taskQueue, worker.Options{})
	bookingWorker.RegisterWorkflow(booking.ECallUpBookingWorkflow)
	bookingWorker.RegisterWorkflow(queue.ECallUpCallUpWorkflow)
	bookingWorker.RegisterActivity(activities)
	bookingWorker.RegisterActivity(callUpActivities)

	sweeper, err := queue.NewSweeper(pool, queueStore, callUps, tenantID)
	if err != nil {
		return fmt.Errorf("configure call-up sweeper: %w", err)
	}

	sweepCtx, stopSweep := context.WithCancel(context.Background())
	defer stopSweep()
	go sweepCallUps(sweepCtx, sweeper, time.Duration(sweepSeconds)*time.Second)

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

// buildCustomsValidator selects the customs cross-validation backend.
// CUSTOMS_VALIDATOR_BACKEND is "http" (default, backwards compatible) or
// "local" (the platform declaration engine); any other value fails closed at
// startup.
func buildCustomsValidator(pool *pgxpool.Pool, timeout time.Duration) (customs.Validator, error) {
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
		return customs.NewLocalValidator(declarations.NewStore(pool))
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
