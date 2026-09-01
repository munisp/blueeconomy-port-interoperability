package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/cruise"
	"github.com/munisp/blueeconomy-port-interoperability/internal/declarations"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/manifests"
	"github.com/munisp/blueeconomy-port-interoperability/internal/nswsecurity"
	"github.com/munisp/blueeconomy-port-interoperability/internal/offshore"
	"github.com/munisp/blueeconomy-port-interoperability/internal/payments"
	"github.com/munisp/blueeconomy-port-interoperability/internal/portcall"
	"github.com/munisp/blueeconomy-port-interoperability/internal/pushtokens"
	"github.com/munisp/blueeconomy-port-interoperability/internal/queue"
	"github.com/munisp/blueeconomy-port-interoperability/internal/securechain"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tariff"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// mustManifestStore builds a manifest store without a database for
// fail-closed constructor tests.
func mustManifestStore() *manifests.Store {
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	store, err := manifests.NewStore(nil, mustSigner(), public, "manifest-authority-")
	if err != nil {
		panic(err)
	}
	return store
}

// mustSecureChainStore builds a secure-chain store without a database for
// fail-closed constructor tests; secure-chain routes are exercised against
// PostgreSQL in the securechain package tests.
func mustSecureChainStore() *securechain.Store {
	store, err := securechain.NewStore(nil, mustSigner(), securechain.Config{})
	if err != nil {
		panic(err)
	}
	return store
}

// mustSigner builds a throwaway envelope signer for constructor tests.
func mustSigner() *events.Signer {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	signer, err := events.NewSigner(key, "1")
	if err != nil {
		panic(err)
	}
	return signer
}

// mustQueueStore builds a queue store without a database for fail-closed
// constructor tests; routes backed by it are exercised against PostgreSQL.
func mustQueueStore() *queue.Store {
	store, err := queue.NewStore(nil, booking.NewStore(nil, mustSigner()), mustSigner(), time.Minute)
	if err != nil {
		panic(err)
	}
	return store
}

type fakePayments struct{}

func (fakePayments) RequestPayment(context.Context, payments.Intent) (payments.Receipt, error) {
	return payments.Receipt{}, nil
}
func (fakePayments) VerifyPayment(_ context.Context, txRef string, expectedAmountKobo int64) (payments.TransferStatus, error) {
	return payments.TransferStatus{TxRef: txRef, State: payments.TransferStateCommitted, AmountKobo: expectedAmountKobo, Currency: "NGN"}, nil
}

type fakeOrchestrator struct{}

func (fakeOrchestrator) StartBookingWorkflow(context.Context, booking.WorkflowInput) error {
	return nil
}
func (fakeOrchestrator) SignalPaymentConfirmed(context.Context, string, string) error { return nil }
func (fakeOrchestrator) SignalGateScan(context.Context, string, string) error         { return nil }
func (fakeOrchestrator) ObserverState(context.Context, string) (booking.ObserverState, error) {
	return booking.ObserverState{}, nil
}

type fakeCallUps struct{}

func (fakeCallUps) StartCallUpWorkflow(context.Context, queue.CallUpWorkflowInput) error {
	return nil
}
func (fakeCallUps) SignalArrivalConfirmed(context.Context, string, string) error { return nil }
func (fakeCallUps) CallUpObserverState(context.Context, string) (queue.CallUpObserverState, error) {
	return queue.CallUpObserverState{}, nil
}

type fakeScorer struct{}

func (fakeScorer) Score(context.Context, declarations.ScoreRequest) (declarations.ScoreResponse, error) {
	return declarations.ScoreResponse{Score: 10, ModelVersion: "test-scorer-1"}, nil
}

type fakePushTokens struct {
	revokeErr error
}

func (fake fakePushTokens) Register(_ context.Context, request pushtokens.RegisterRequest) (pushtokens.Token, error) {
	return pushtokens.Token{UserID: "integration-tester", DeviceID: request.DeviceID, Token: request.Token, Platform: request.Platform, Status: "ACTIVE"}, nil
}

func (fake fakePushTokens) Revoke(_ context.Context, deviceID string) (pushtokens.Token, error) {
	if fake.revokeErr != nil {
		return pushtokens.Token{}, fake.revokeErr
	}
	return pushtokens.Token{UserID: "integration-tester", DeviceID: deviceID, Status: "REVOKED"}, nil
}

func testConfig() Config {
	return Config{
		Store:             portcall.NewStore(nil),
		Bookings:          booking.NewStore(nil, mustSigner()),
		Queues:            mustQueueStore(),
		Declarations:      declarations.NewStore(nil, mustSigner()),
		Offshore:          offshore.NewStore(nil, mustSigner()),
		Cruise:            cruise.NewStore(nil, mustSigner()),
		Manifests:         mustManifestStore(),
		SecureChains:      mustSecureChainStore(),
		Tariffs:           tariff.NewStore(nil, mustSigner()),
		PushTokens:        fakePushTokens{},
		Registry:          fakeRegistry{},
		DeclarationScorer: fakeScorer{},
		Payments:          fakePayments{},
		Orchestrator:      fakeOrchestrator{},
		CallUps:           fakeCallUps{},
		AuthMode:          AuthModeLoopbackTrustedProxy,
		TenantGateway: tenantctx.Verifier{
			Key:      []byte("0123456789abcdef0123456789abcdef"),
			Issuer:   "gateway.blueeconomy.ng",
			Audience: "s1-port-interoperability",
		},
		NSWVerifier:         &nswsecurity.Verifier{},
		Pool:                nil,
		FGNShareBasisPoints: 250,
		NSWReplayTTL:        time.Hour,
	}
}

func TestNewFailsClosedWithoutDependencies(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("empty config must fail closed")
	}
	config := testConfig()
	config.TenantGateway = tenantctx.Verifier{}
	if _, err := New(config); err == nil {
		t.Fatal("missing tenant gateway configuration must fail closed")
	}
	config = testConfig()
	config.Payments = nil
	if _, err := New(config); err == nil {
		t.Fatal("missing payments gateway must fail closed")
	}
	config = testConfig()
	config.Orchestrator = nil
	if _, err := New(config); err == nil {
		t.Fatal("missing orchestrator must fail closed")
	}
	config = testConfig()
	config.Queues = nil
	if _, err := New(config); err == nil {
		t.Fatal("missing queue store must fail closed")
	}
	config = testConfig()
	config.CallUps = nil
	if _, err := New(config); err == nil {
		t.Fatal("missing call-up orchestrator must fail closed")
	}
	config = testConfig()
	config.FGNShareBasisPoints = 0
	if _, err := New(config); err == nil {
		t.Fatal("invalid FGN share must fail closed")
	}
	config = testConfig()
	config.NSWVerifier = nil
	if _, err := New(config); err == nil {
		t.Fatal("missing NSW verifier must fail closed")
	}
	config = testConfig()
	config.Declarations = nil
	if _, err := New(config); err == nil {
		t.Fatal("missing declaration store must fail closed")
	}
	config = testConfig()
	config.DeclarationScorer = nil
	if _, err := New(config); err == nil {
		t.Fatal("missing declaration scorer must fail closed")
	}
}

func newWiredHandler(t *testing.T) http.Handler {
	t.Helper()
	// pgxpool.New parses the config without connecting; the pool is only used
	// by code paths these wiring tests never reach.
	pool, err := pgxpool.New(context.Background(), "postgres://127.0.0.1:1/unreachable")
	if err != nil {
		t.Fatalf("build test pool: %v", err)
	}
	t.Cleanup(pool.Close)
	config := testConfig()
	config.Pool = pool
	handler, err := New(config)
	if err != nil {
		t.Fatalf("wire server: %v", err)
	}
	return handler
}

func TestHealthzIsPublic(t *testing.T) {
	handler := newWiredHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", response.Code)
	}
}

func loopbackRequest(method, path, body string) *http.Request {
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, path, reader)
	request.RemoteAddr = "127.0.0.1:41001"
	request.Header.Set("X-Trusted-Proxy", "loopback")
	request.Header.Set("X-Authenticated-Principal", "integration-tester")
	request.Header.Set("Content-Type", "application/json")
	return request
}

func TestTenantMiddlewareIsMountedAndRejectsMissingToken(t *testing.T) {
	handler := newWiredHandler(t)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, loopbackRequest(http.MethodGet, "/v1/port-calls/call-001", ""))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("request without tenant token = %d, want 401", response.Code)
	}
}

func TestTenantMiddlewareRejectsCallerSuppliedTenantHeader(t *testing.T) {
	handler := newWiredHandler(t)
	request := loopbackRequest(http.MethodGet, "/v1/port-calls/call-001", "")
	request.Header.Set("X-Tenant-ID", "tenant-forged")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("caller-supplied tenant header = %d, want 400", response.Code)
	}
}

func TestAuthModeRejectsNonLoopback(t *testing.T) {
	handler := newWiredHandler(t)
	request := loopbackRequest(http.MethodGet, "/v1/port-calls/call-001", "")
	request.RemoteAddr = "203.0.113.10:9999"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("non-loopback request = %d, want 403", response.Code)
	}
}

func TestNSWIngressRouteIsMountedAndRequiresSignature(t *testing.T) {
	handler := newWiredHandler(t)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, loopbackRequest(http.MethodPost, "/v1/nsw/port-calls", "{}"))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("NSW ingress without signature = %d, want 401", response.Code)
	}
}
