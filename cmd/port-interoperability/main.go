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
	"github.com/munisp/blueeconomy-port-interoperability/internal/declarations"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/ledger"
	"github.com/munisp/blueeconomy-port-interoperability/internal/nswsecurity"
	"github.com/munisp/blueeconomy-port-interoperability/internal/payments"
	"github.com/munisp/blueeconomy-port-interoperability/internal/portcall"
	"github.com/munisp/blueeconomy-port-interoperability/internal/queue"
	"github.com/munisp/blueeconomy-port-interoperability/internal/server"
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
	// Tenant token verification: RS256 Keycloak JWKS when TENANT_GATEWAY_JWKS_URL
	// is set (production profile); otherwise the HS256 shared-key loopback
	// profile. Both fail closed on missing configuration.
	tenantIssuer := requiredEnv("TENANT_GATEWAY_ISS")
	tenantAudience := requiredEnv("TENANT_GATEWAY_AUD")
	tenantGateway := tenantctx.Verifier{
		Key:      []byte(os.Getenv("TENANT_GATEWAY_KEY")),
		Issuer:   tenantIssuer,
		Audience: tenantAudience,
	}
	var tenantGatewayJWKS *tenantctx.JWKSVerifier
	if jwksURL := os.Getenv("TENANT_GATEWAY_JWKS_URL"); jwksURL != "" {
		jwksVerifier, jwksErr := tenantctx.NewJWKSVerifier(jwksURL, tenantIssuer, tenantAudience, os.Getenv("TENANT_GATEWAY_JWKS_CA_FILE"))
		if jwksErr != nil {
			return fmt.Errorf("configure tenant gateway JWKS verifier: %w", jwksErr)
		}
		tenantGatewayJWKS = jwksVerifier
	} else if !tenantGateway.Ready() {
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
	// The refund rail is mandatory wiring: cancelling a paid booking posts a
	// compensating TigerBeetle transfer, so the API service refuses to boot
	// without a ledger (fail closed — never strand trucker money).
	settlement, err := ledger.NewTigerBeetle(requiredEnv("TIGERBEETLE_CLUSTER_ID"), strings.Split(requiredEnv("TIGERBEETLE_ADDRESSES"), ","))
	if err != nil {
		return fmt.Errorf("configure TigerBeetle refund rail: %w", err)
	}
	defer settlement.Close()
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
	// Declaration risk scoring is mandatory wiring: without the fail-closed
	// scorer every submission would park in SCORING_UNAVAILABLE. Phase-7
	// (PRA-066..068): the scorer is called over gRPC
	// (blueeconomy.riskscore.v1.RiskScoreService) with bounded retries and a
	// circuit breaker. PRA-126: authentication is a Keycloak
	// client_credentials service-account token — the static
	// DECLARATIONS_SCORER_BEARER_TOKEN path is retired. The provisioned
	// client must carry an audience mapper for "declaration-scorer" (the
	// scorer's KEYCLOAK_EXPECTED_AUDIENCE contract value).
	scorerTimeout, err := time.ParseDuration(defaultEnv("DECLARATIONS_SCORER_TIMEOUT", "10s"))
	if err != nil || scorerTimeout <= 0 {
		return errors.New("DECLARATIONS_SCORER_TIMEOUT must be a positive duration")
	}
	scorerTokenSource, err := declarations.NewKeycloakTokenSource(declarations.KeycloakTokenSourceConfig{
		TokenURL:     requiredEnv("KEYCLOAK_TOKEN_URL"),
		ClientID:     requiredEnv("KEYCLOAK_CLIENT_ID"),
		ClientSecret: requiredEnv("KEYCLOAK_CLIENT_SECRET"),
	})
	if err != nil {
		return fmt.Errorf("configure declaration scorer authentication: %w", err)
	}
	declarationScorer, err := declarations.NewGRPCScorer(declarations.GRPCScorerConfig{
		Address:     requiredEnv("DECLARATIONS_SCORER_GRPC_ADDR"),
		CACertFile:  os.Getenv("DECLARATIONS_SCORER_CA_CERT_FILE"),
		Timeout:     scorerTimeout,
		MaxRetries:  -1, // platform default retry budget
		TokenSource: scorerTokenSource,
	})
	if err != nil {
		return fmt.Errorf("configure declaration risk scorer: %w", err)
	}
	defer declarationScorer.Close()
	highValueMinor, err := strconv.ParseInt(defaultEnv("DECLARATIONS_HIGH_VALUE_MINOR", "0"), 10, 64)
	if err != nil || highValueMinor < 0 {
		return errors.New("DECLARATIONS_HIGH_VALUE_MINOR must be a non-negative integer")
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
	// Envelope provenance signing is mandatory: every lifecycle event emitted
	// by the stores is a JWS (EdDSA over the JCS-canonical envelope).
	envelopeSigner, err := events.SignerFromEnv()
	if err != nil {
		return fmt.Errorf("configure envelope signer: %w", err)
	}
	bookingStore := booking.NewStore(pool, envelopeSigner)
	queueStore, err := queue.NewStore(pool, bookingStore, envelopeSigner, time.Duration(graceMinutes)*time.Minute)
	if err != nil {
		return fmt.Errorf("configure queue store: %w", err)
	}
	// Call-up engine hook: booking slot releases promote the head of the
	// terminal queue in the same transaction.
	bookingStore.SetCapacityListener(queueStore)
	// Refund rail: paid bookings cancelled through the API post a
	// compensating settlement transfer before reaching REFUNDED.
	if err := bookingStore.SetRefundPoster(settlement, fgnShareBPS); err != nil {
		return fmt.Errorf("wire refund rail: %w", err)
	}
	handler, err := server.New(server.Config{
		Store:                     portcall.NewStore(pool),
		Bookings:                  bookingStore,
		Queues:                    queueStore,
		Declarations:              declarations.NewStore(pool, envelopeSigner),
		DeclarationScorer:         declarationScorer,
		DeclarationHighValueMinor: highValueMinor,
		Payments:                  paymentsGateway,
		Orchestrator:              orchestrator,
		CallUps:                   callUps,
		AuthMode:                  authMode,
		TenantGateway:             tenantGateway,
		TenantGatewayJWKS:         tenantGatewayJWKS,
		NSWVerifier:               nswVerifier,
		Pool:                      pool,
		FGNShareBasisPoints:       fgnShareBPS,
		NSWReplayTTL:              time.Duration(replayTTLMinutes) * time.Minute,
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
