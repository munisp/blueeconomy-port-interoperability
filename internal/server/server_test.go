package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/nswsecurity"
	"github.com/munisp/blueeconomy-port-interoperability/internal/payments"
	"github.com/munisp/blueeconomy-port-interoperability/internal/portcall"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

type fakePayments struct{}

func (fakePayments) RequestPayment(context.Context, payments.Intent) (payments.Receipt, error) {
	return payments.Receipt{}, nil
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

func testConfig() Config {
	return Config{
		Store:        portcall.NewStore(nil),
		Bookings:     booking.NewStore(nil),
		Payments:     fakePayments{},
		Orchestrator: fakeOrchestrator{},
		AuthMode:     AuthModeLoopbackTrustedProxy,
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
	config.FGNShareBasisPoints = 0
	if _, err := New(config); err == nil {
		t.Fatal("invalid FGN share must fail closed")
	}
	config = testConfig()
	config.NSWVerifier = nil
	if _, err := New(config); err == nil {
		t.Fatal("missing NSW verifier must fail closed")
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
