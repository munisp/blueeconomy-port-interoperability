package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/nswsecurity"
	"github.com/munisp/blueeconomy-port-interoperability/internal/payments"
	"github.com/munisp/blueeconomy-port-interoperability/internal/portcall"
	"github.com/munisp/blueeconomy-port-interoperability/internal/queue"
	"github.com/munisp/blueeconomy-port-interoperability/internal/server"
	"github.com/munisp/blueeconomy-port-interoperability/internal/telemetry"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

func main() {
	if err := run(); err != nil {
		log.Printf("port-interoperability: %v", err)
		os.Exit(1)
	}
}

func run() error {
	databaseURL := requiredEnv("DATABASE_URL")
	migrationPaths := requiredEnv("MIGRATION_PATH")
	port := requiredEnv("PORT")
	authMode := requiredEnv("AUTH_MODE")
	if authMode != server.AuthModeLoopbackTrustedProxy {
		return fmt.Errorf("AUTH_MODE must be %q until a verified Ministry OIDC edge is configured", server.AuthModeLoopbackTrustedProxy)
	}
	tenantGateway := tenantctx.Verifier{
		Key:      []byte(requiredEnv("TENANT_GATEWAY_KEY")),
		Issuer:   requiredEnv("TENANT_GATEWAY_ISS"),
		Audience: requiredEnv("TENANT_GATEWAY_AUD"),
	}
	if !tenantGateway.Ready() {
		return errors.New("TENANT_GATEWAY_KEY must be at least 32 bytes and issuer/audience must be set")
	}
	nswVerifier, err := nswsecurity.New(nswsecurity.Policy{
		JWKSURL:           requiredEnv("NSW_JWKS_URL"),
		PinnedJWKSHA256:   requiredEnv("NSW_JWKS_PIN_SHA256"),
		AllowedAlgorithms: map[string]bool{"RS256": true},
		AllowedKIDs:       allowedKIDs(requiredEnv("NSW_ALLOWED_KIDS")),
		ExpectedIssuer:    requiredEnv("NSW_ISSUER"),
		ExpectedAudience:  requiredEnv("NSW_AUDIENCE"),
	})
	if err != nil {
		return fmt.Errorf("configure NSW ingress verifier: %w", err)
	}
	replayTTLMinutes, err := strconv.Atoi(defaultEnv("NSW_REPLAY_TTL_MINUTES", "1440"))
	if err != nil || replayTTLMinutes < 1 {
		return errors.New("NSW_REPLAY_TTL_MINUTES must be a positive integer")
	}
	fgnShareBPS, err := strconv.ParseInt(requiredEnv("FGN_SHARE_BASIS_POINTS"), 10, 64)
	if err != nil {
		return fmt.Errorf("FGN_SHARE_BASIS_POINTS must be an integer: %w", err)
	}
	paymentsGateway, err := payments.NewMojaloop(requiredEnv("MOJALOOP_BASE_URL"), requiredEnv("MOJALOOP_BEARER_TOKEN"), nil)
	if err != nil {
		return fmt.Errorf("configure Mojaloop gateway: %w", err)
	}
	orchestrator, err := booking.NewTemporalOrchestrator(requiredEnv("TEMPORAL_ADDRESS"), requiredEnv("TEMPORAL_NAMESPACE"), requiredEnv("TEMPORAL_TASK_QUEUE"))
	if err != nil {
		return fmt.Errorf("configure Temporal orchestrator: %w", err)
	}
	defer orchestrator.Close()
	callUps, err := queue.NewTemporalCallUpOrchestrator(requiredEnv("TEMPORAL_ADDRESS"), requiredEnv("TEMPORAL_NAMESPACE"), requiredEnv("TEMPORAL_TASK_QUEUE"))
	if err != nil {
		return fmt.Errorf("configure Temporal call-up orchestrator: %w", err)
	}
	defer callUps.Close()
	graceMinutes, err := strconv.Atoi(defaultEnv("CALLUP_GRACE_MINUTES", "90"))
	if err != nil || graceMinutes < 1 {
		return errors.New("CALLUP_GRACE_MINUTES must be a positive integer")
	}

	paths := strings.Split(migrationPaths, ",")
	migrations := make([][]byte, 0, len(paths))
	for _, migrationPath := range paths {
		migrationPath = strings.TrimSpace(migrationPath)
		if migrationPath == "" {
			return errors.New("MIGRATION_PATH contains an empty path")
		}
		migration, readErr := os.ReadFile(filepath.Clean(migrationPath))
		if readErr != nil {
			return fmt.Errorf("read migration %q: %w", migrationPath, readErr)
		}
		migrations = append(migrations, migration)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	telemetryConfig, err := telemetry.LoadConfig("blueeconomy-port-interoperability")
	if err != nil {
		return fmt.Errorf("load telemetry config: %w", err)
	}
	pipeline, err := telemetry.Setup(ctx, telemetryConfig)
	if err != nil {
		return fmt.Errorf("setup telemetry: %w", err)
	}
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := pipeline.Shutdown(shutdownContext); err != nil {
			log.Printf("telemetry shutdown: %v", err)
		}
	}()
	if pipeline.Enabled() {
		log.Printf("telemetry: OTLP gRPC traces exporting to %s; Prometheus metrics on GET /metrics", telemetryConfig.Endpoint)
	} else {
		log.Printf("telemetry: tracing disabled (OTEL_EXPORTER_OTLP_ENDPOINT not set); running with an explicit no-op tracer, Prometheus metrics on GET /metrics")
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("ping postgres: %w", err)
	}
	defer pool.Close()
	for index, migration := range migrations {
		if _, err := pool.Exec(ctx, string(migration)); err != nil {
			return fmt.Errorf("apply migration %d: %w", index+1, err)
		}
	}
	bookingStore := booking.NewStore(pool)
	queueStore, err := queue.NewStore(pool, bookingStore, time.Duration(graceMinutes)*time.Minute)
	if err != nil {
		return fmt.Errorf("configure queue store: %w", err)
	}
	// Call-up engine hook: booking slot releases promote the head of the
	// terminal queue in the same transaction.
	bookingStore.SetCapacityListener(queueStore)
	handler, err := server.New(server.Config{
		Store:               portcall.NewStore(pool),
		Bookings:            bookingStore,
		Queues:              queueStore,
		Payments:            paymentsGateway,
		Orchestrator:        orchestrator,
		CallUps:             callUps,
		AuthMode:            authMode,
		TenantGateway:       tenantGateway,
		NSWVerifier:         nswVerifier,
		Pool:                pool,
		FGNShareBasisPoints: fgnShareBPS,
		NSWReplayTTL:        time.Duration(replayTTLMinutes) * time.Minute,
		Telemetry:           pipeline,
	})
	if err != nil {
		return fmt.Errorf("build server: %w", err)
	}
	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownContext)
	}()
	log.Printf("port-interoperability listening on :%s", port)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

func allowedKIDs(value string) map[string]time.Time {
	kids := map[string]time.Time{}
	for _, kid := range strings.Split(value, ",") {
		kid = strings.TrimSpace(kid)
		if kid != "" {
			kids[kid] = time.Time{}
		}
	}
	return kids
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
