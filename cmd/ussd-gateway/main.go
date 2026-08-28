// ussd-gateway exposes the Africa's Talking-style USSD callback for eCallUp
// booking status checks, slot booking and truck call-up queue entry/position.
// It fails closed without Redis (sessions) and PostgreSQL (bookings, queue).
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
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/queue"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/ussd"
	"github.com/redis/go-redis/v9"
)

func main() {
	if err := run(); err != nil {
		log.Printf("ussd-gateway: %v", err)
		os.Exit(1)
	}
}

func run() error {
	redisURL := requiredEnv("REDIS_URL")
	databaseURL := requiredEnv("DATABASE_URL")
	migrationPaths := requiredEnv("MIGRATION_PATH")
	port := requiredEnv("PORT")
	tenantID := requiredEnv("USSD_TENANT_ID")
	ttlSeconds, err := strconv.Atoi(defaultEnv("USSD_SESSION_TTL_SECONDS", "300"))
	if err != nil || ttlSeconds < 30 {
		return errors.New("USSD_SESSION_TTL_SECONDS must be an integer >= 30")
	}

	redisOptions, err := redis.ParseURL(redisURL)
	if err != nil {
		return fmt.Errorf("REDIS_URL is not valid: %w", err)
	}
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
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
	for index, migrationPath := range strings.Split(migrationPaths, ",") {
		migrationPath = strings.TrimSpace(migrationPath)
		if migrationPath == "" {
			return errors.New("MIGRATION_PATH contains an empty path")
		}
		migration, readErr := os.ReadFile(filepath.Clean(migrationPath))
		if readErr != nil {
			return fmt.Errorf("read migration %q: %w", migrationPath, readErr)
		}
		if _, err := pool.Exec(ctx, string(migration)); err != nil {
			return fmt.Errorf("apply migration %d: %w", index+1, err)
		}
	}

	sessions, err := ussd.NewSessionStore(redisClient, time.Duration(ttlSeconds)*time.Second)
	if err != nil {
		return err
	}
	// Envelope provenance signing is mandatory: bookings and queue entries
	// created over USSD emit JWS-signed lifecycle events.
	envelopeSigner, err := events.SignerFromEnv()
	if err != nil {
		return fmt.Errorf("configure envelope signer: %w", err)
	}
	bookingStore := booking.NewStore(pool, envelopeSigner)
	directory, err := booking.NewDirectory(bookingStore, booking.Principal{ID: "ussd-gateway", Role: "trucker"})
	if err != nil {
		return err
	}
	queueStore, err := queue.NewStore(pool, bookingStore, envelopeSigner, 0)
	if err != nil {
		return err
	}
	queueDirectory, err := queue.NewDirectory(queueStore, queue.Principal{ID: "ussd-gateway", Role: "trucker"})
	if err != nil {
		return err
	}
	handler, err := ussd.NewHandler(directory, queueDirectory, sessions)
	if err != nil {
		return err
	}

	// The USSD gateway is itself the trusted channel edge: it binds every
	// dialogue to the configured port tenant before storage runs.
	tenantBound := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		bound, err := tenantctx.WithClaims(request.Context(), tenantctx.Claims{
			Issuer:   "ussd-gateway",
			Audience: "s1-port-interoperability",
			TenantID: tenantID,
			Subject:  "ussd-gateway",
			Expires:  time.Now().Add(time.Minute).Unix(),
		})
		if err != nil {
			http.Error(response, "tenant binding failed", http.StatusInternalServerError)
			return
		}
		handler.Callback(response, request.WithContext(bound))
	})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("POST /ussd/callback", http.MaxBytesHandler(tenantBound, 1<<20))

	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
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
	log.Printf("ussd-gateway listening on :%s", port)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
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
