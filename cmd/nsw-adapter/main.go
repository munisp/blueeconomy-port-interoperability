// nsw-adapter drains NSW-relevant platform outbox events (booking
// created/paid, gate decisions, port-call clearance decisions, queue
// call-ups) and hands them to the NSW operator endpoint at-least-once as
// RS256-signed messages over pinned-CA HTTPS. Per-event delivery state lives
// in the nsw_delivery ledger (PENDING/DELIVERED/FAILED_PERMANENT). The
// process fails closed: without the signing key, pinned CA and HTTPS endpoint
// it refuses to start.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/nswadapter"
	"github.com/munisp/blueeconomy-port-interoperability/internal/nswsecurity"
	"github.com/munisp/blueeconomy-port-interoperability/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		log.Printf("nsw-adapter: %v", err)
		os.Exit(1)
	}
}

func run() error {
	databaseURL := requiredEnv("DATABASE_URL")
	signingKey, err := nswsecurity.LoadRSAPrivateKeyFile(requiredEnv("NSW_SIGNING_KEY_FILE"))
	if err != nil {
		return fmt.Errorf("configure NSW outbound signing key: %w", err)
	}
	tokenTTL, err := time.ParseDuration(defaultEnv("NSW_TOKEN_TTL", "5m"))
	if err != nil || tokenTTL <= 0 {
		return errors.New("NSW_TOKEN_TTL must be a positive duration")
	}
	signer, err := nswsecurity.NewSigner(signingKey,
		requiredEnv("NSW_SIGNING_KID"),
		defaultEnv("NSW_OUTBOUND_ISSUER", "s1-port-interoperability"),
		requiredEnv("NSW_OUTBOUND_AUDIENCE"),
		defaultEnv("NSW_OUTBOUND_SUBJECT", "nsw-adapter"),
		tokenTTL)
	if err != nil {
		return fmt.Errorf("configure NSW outbound signer: %w", err)
	}

	config := nswadapter.Config{
		EndpointURL: requiredEnv("NSW_ENDPOINT_URL"),
		CACertFile:  requiredEnv("NSW_CA_CERT_FILE"),
		ContentType: defaultEnv("NSW_CONTENT_TYPE", nswadapter.ContentTypeJSON),
	}
	if config.Timeout, err = durationEnv("NSW_TIMEOUT", "10s"); err != nil {
		return err
	}
	if config.PollInterval, err = durationEnv("NSW_POLL_INTERVAL", "5s"); err != nil {
		return err
	}
	if config.BackoffBase, err = durationEnv("NSW_BACKOFF_BASE", "5s"); err != nil {
		return err
	}
	if config.BackoffMax, err = durationEnv("NSW_BACKOFF_MAX", "10m"); err != nil {
		return err
	}
	if config.MaxAttempts, err = intEnv("NSW_MAX_ATTEMPTS", "8"); err != nil {
		return err
	}
	if config.BatchSize, err = intEnv("NSW_BATCH_SIZE", "100"); err != nil {
		return err
	}
	var maxBody int64
	if maxBody, err = int64Env("NSW_MAX_BODY_BYTES", "0"); err != nil {
		return err
	}
	config.MaxBodyBytes = maxBody
	if err := config.Validate(); err != nil {
		return fmt.Errorf("configure NSW adapter: %w", err)
	}
	client, err := nswadapter.NewClient(config)
	if err != nil {
		return fmt.Errorf("configure NSW client: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Telemetry: disabled without OTEL_EXPORTER_OTLP_ENDPOINT (boot
	// unaffected); enabled export is async/batched and collector-down is
	// drop-with-metric, never a delivery failure.
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

	runner, err := nswadapter.NewRunner(pool, signer, client, config)
	if err != nil {
		return err
	}
	log.Printf("nsw-adapter delivering to %s as kid %s (%s)", config.EndpointURL, signer.KID(), config.ContentType)
	for {
		delivered, err := runner.DrainOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Fail closed but stay alive: undelivered events remain PENDING
			// and are retried on the next cycle.
			log.Printf("nsw-adapter drain error (will retry): %v", err)
		}
		if delivered > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(config.PollInterval):
		}
	}
}

func durationEnv(name, fallback string) (time.Duration, error) {
	value, err := time.ParseDuration(defaultEnv(name, fallback))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return value, nil
}

func intEnv(name, fallback string) (int, error) {
	value, err := strconv.Atoi(defaultEnv(name, fallback))
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	return value, nil
}

func int64Env(name, fallback string) (int64, error) {
	value, err := strconv.ParseInt(defaultEnv(name, fallback), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	return value, nil
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
