package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/payments"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// These tests run the payment-confirmation boundary end to end against a real
// PostgreSQL (BOOKING_TEST_DATABASE_URL) and a real MojaloopGateway whose HTTP
// verification calls hit a controllable in-process TLS switch. Nothing about
// the database, the booking store, the tenant middleware or the gateway's
// VerifyPayment path is stubbed; only the Temporal orchestrator and the
// switch itself (an external system) are simulated.

var gatewayTestKey = []byte("0123456789abcdef0123456789abcdef")

// gatewayToken mints an HS256 gateway token the tenant middleware accepts.
func gatewayToken(t *testing.T, tenantID, subject string, roles ...string) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payloadJSON, err := json.Marshal(tenantctx.Claims{
		Issuer:   "gateway.blueeconomy.ng",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  subject,
		Expires:  time.Now().Add(time.Hour).Unix(),
		Roles:    roles,
	})
	if err != nil {
		t.Fatalf("encode token payload: %v", err)
	}
	payload := base64.RawURLEncoding.EncodeToString(payloadJSON)
	mac := hmac.New(sha256.New, gatewayTestKey)
	mac.Write([]byte(header + "." + payload))
	return header + "." + payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// switchStub is a controllable Mojaloop transfer resource: the tests flip
// what the "switch" reports for the settlement query.
type switchStub struct {
	mu     sync.Mutex
	status int
	body   string
	server *httptest.Server
}

func newSwitchStub(t *testing.T) *switchStub {
	t.Helper()
	stub := &switchStub{status: http.StatusOK, body: `{}`}
	stub.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/transfers/") {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		stub.mu.Lock()
		status, body := stub.status, stub.body
		stub.mu.Unlock()
		w.Header().Set("Content-Type", "application/vnd.interoperability.transfers+json;version=1.0")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(stub.server.Close)
	return stub
}

func (stub *switchStub) report(status int, body string) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.status = status
	stub.body = body
}

func committedTransfer(txRef, nairaAmount string) string {
	return fmt.Sprintf(`{
		"transferId": %q,
		"transferState": "COMMITTED",
		"fulfilment": "WLctttbu2HvTsa1XWvUoGRcQozHsqeu9Ahl2JW9Bsu8",
		"amount": {"amount": %q, "currency": "NGN"}
	}`, txRef, nairaAmount)
}

type paymentGateEnv struct {
	handler  http.Handler
	store    *booking.Store
	stub     *switchStub
	ctx      context.Context
	tenantID string
}

func newPaymentGateEnv(t *testing.T) paymentGateEnv {
	t.Helper()
	databaseURL := os.Getenv("BOOKING_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("BOOKING_TEST_DATABASE_URL is not set; skipping PostgreSQL-backed payment gate tests")
	}
	ctx := context.Background()
	signer := mustSigner()
	store, err := booking.Open(ctx, databaseURL, signer)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(store.Close)
	if _, err := store.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset test schema: %v", err)
	}
	migrationDir := filepath.Join("..", "..", "db", "migrations")
	entries, err := filepath.Glob(filepath.Join(migrationDir, "*.sql"))
	if err != nil || len(entries) == 0 {
		t.Fatalf("find migrations: %v", err)
	}
	sort.Strings(entries)
	for _, entry := range entries {
		migration, err := os.ReadFile(entry)
		if err != nil {
			t.Fatalf("read migration %s: %v", entry, err)
		}
		if _, err := store.Exec(ctx, string(migration)); err != nil {
			t.Fatalf("apply migration %s: %v", entry, err)
		}
	}
	tenantID := fmt.Sprintf("tenant-test-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := store.Pool().Exec(ctx, `INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ($1, $2)`, tenantID, "payment-gate-test-authority"); err != nil {
		t.Fatalf("insert test tenant: %v", err)
	}
	bound, err := tenantctx.WithClaims(ctx, tenantctx.Claims{
		Issuer:   "payment-gate-test",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  "payment-gate-tester",
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("bind test tenant claims: %v", err)
	}

	stub := newSwitchStub(t)
	// The real gateway: its VerifyPayment path (HTTPS, bearer auth, strict
	// COMMITTED + exact-amount checks) is what gates settlement here.
	gateway, err := payments.NewMojaloop(stub.server.URL, "test-token", stub.server.Client())
	if err != nil {
		t.Fatalf("build mojaloop gateway: %v", err)
	}

	config := testConfig()
	config.Bookings = store
	config.Payments = gateway
	config.Orchestrator = fakeOrchestrator{}
	config.Pool = store.Pool()
	config.TenantGateway = tenantctx.Verifier{
		Key:      gatewayTestKey,
		Issuer:   "gateway.blueeconomy.ng",
		Audience: "s1-port-interoperability",
	}
	handler, err := New(config)
	if err != nil {
		t.Fatalf("wire server: %v", err)
	}
	return paymentGateEnv{handler: handler, store: store, stub: stub, ctx: bound, tenantID: tenantID}
}

// makePayableBooking drives a booking to SLOT_RESERVED with a REQUESTED
// payment intent, straight through the real store.
func (env paymentGateEnv) makePayableBooking(t *testing.T, suffix string) booking.Booking {
	t.Helper()
	terminalID := fmt.Sprintf("TERM-%s", strings.ToUpper(suffix))
	if err := env.store.CreateTerminal(env.ctx, terminalID, "LAGOS", "Gate Test Terminal", 250000); err != nil {
		t.Fatalf("create terminal: %v", err)
	}
	slot, err := env.store.CreateSlot(env.ctx, terminalID, time.Now().UTC().Add(-time.Hour), time.Now().UTC().Add(2*time.Hour), 4)
	if err != nil {
		t.Fatalf("create slot: %v", err)
	}
	principal := booking.Principal{ID: "test-trucker", Role: "trucker"}
	created, err := env.store.Create(env.ctx, booking.CreateRequest{
		RequestID:     "req-p7-gate-" + suffix,
		TruckPlate:    "LAG-222-BB",
		TruckerMSISDN: "+2348012345678",
		TerminalID:    terminalID,
		Channel:       booking.ChannelWeb,
		AmountKobo:    250000,
		ExpiresAt:     time.Now().Add(2 * time.Hour),
	}, principal)
	if err != nil {
		t.Fatalf("create booking: %v", err)
	}
	reserved, err := env.store.ReserveSlot(env.ctx, created.BookingID, slot.SlotID, created.Version, principal)
	if err != nil {
		t.Fatalf("reserve slot: %v", err)
	}
	if _, err := env.store.CreatePaymentIntent(env.ctx, reserved.BookingID, "pay-p7-gate-"+suffix, "txref-p7-gate-"+suffix, reserved.Version); err != nil {
		t.Fatalf("create payment intent: %v", err)
	}
	return reserved
}

func (env paymentGateEnv) confirmViaHTTP(t *testing.T, bookingID, receiptRef string, version int64, token string) *httptest.ResponseRecorder {
	t.Helper()
	body := fmt.Sprintf(`{"receipt_ref": %q, "expected_version": %d}`, receiptRef, version)
	request := loopbackRequest(http.MethodPost, "/v1/bookings/"+bookingID+"/payment-confirmations", body)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	env.handler.ServeHTTP(response, request)
	return response
}

func (env paymentGateEnv) bookingStatus(t *testing.T, bookingID string) booking.Booking {
	t.Helper()
	found, err := env.store.Get(env.ctx, bookingID)
	if err != nil {
		t.Fatalf("get booking: %v", err)
	}
	return found
}

// GAP-PIO-01(c): settlement is gated on the payment switch at two levels —
// the caller must carry the verified payment-switch role (no self-settlement
// by a tenant user) and the switch itself must report the transfer COMMITTED
// for the exact intent amount. Every failure mode leaves the booking unpaid.
func TestIntegration_ConfirmPaymentRequiresSwitchRoleAndVerifiedSettlement(t *testing.T) {
	env := newPaymentGateEnv(t)

	// 1. Self-settlement refusal: a trucker token can never confirm, even
	// with the correct switch-issued receipt reference.
	reserved := env.makePayableBooking(t, "selfsettle")
	truckerToken := gatewayToken(t, env.tenantID, "test-trucker", RoleTrucker)
	response := env.confirmViaHTTP(t, reserved.BookingID, "txref-p7-gate-selfsettle", reserved.Version, truckerToken)
	if response.Code != http.StatusForbidden {
		t.Fatalf("self-settlement: status = %d, want 403", response.Code)
	}
	if found := env.bookingStatus(t, reserved.BookingID); found.Status != booking.StatusSlotReserved {
		t.Fatalf("self-settlement changed status to %s", found.Status)
	}

	// 2. Switch-role caller with a fabricated receipt reference: refused
	// before the switch is even queried.
	reserved = env.makePayableBooking(t, "fabricated")
	switchToken := gatewayToken(t, env.tenantID, "switch-service", RolePaymentSwitch)
	response = env.confirmViaHTTP(t, reserved.BookingID, "fabricated-by-caller", reserved.Version, switchToken)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("fabricated receipt: status = %d, want 422", response.Code)
	}
	if found := env.bookingStatus(t, reserved.BookingID); found.Status != booking.StatusSlotReserved {
		t.Fatalf("fabricated receipt changed status to %s", found.Status)
	}

	// 3. Switch reports the transfer not yet settled (RESERVED): refuse.
	reserved = env.makePayableBooking(t, "reserved")
	env.stub.report(http.StatusOK, `{
		"transferId": "txref-p7-gate-reserved",
		"transferState": "RESERVED",
		"amount": {"amount": "2500.00", "currency": "NGN"}
	}`)
	response = env.confirmViaHTTP(t, reserved.BookingID, "txref-p7-gate-reserved", reserved.Version, switchToken)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("uncommitted transfer: status = %d, want 422", response.Code)
	}
	if found := env.bookingStatus(t, reserved.BookingID); found.Status != booking.StatusSlotReserved {
		t.Fatalf("uncommitted transfer changed status to %s", found.Status)
	}

	// 4. Switch outage: fail closed as 503 UNVERIFIED, booking unpaid.
	reserved = env.makePayableBooking(t, "outage")
	env.stub.report(http.StatusBadGateway, `{}`)
	response = env.confirmViaHTTP(t, reserved.BookingID, "txref-p7-gate-outage", reserved.Version, switchToken)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("switch outage: status = %d, want 503", response.Code)
	}
	if found := env.bookingStatus(t, reserved.BookingID); found.Status != booking.StatusSlotReserved {
		t.Fatalf("switch outage changed status to %s", found.Status)
	}

	// 5. Switch reports COMMITTED but for the wrong amount: refuse.
	reserved = env.makePayableBooking(t, "wrongamount")
	env.stub.report(http.StatusOK, committedTransfer("txref-p7-gate-wrongamount", "2499.99"))
	response = env.confirmViaHTTP(t, reserved.BookingID, "txref-p7-gate-wrongamount", reserved.Version, switchToken)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("amount mismatch: status = %d, want 422", response.Code)
	}
	if found := env.bookingStatus(t, reserved.BookingID); found.Status != booking.StatusSlotReserved {
		t.Fatalf("amount mismatch changed status to %s", found.Status)
	}

	// 6. Happy path: COMMITTED for the exact intent amount settles; the
	// replayed confirmation is idempotent.
	reserved = env.makePayableBooking(t, "settled")
	env.stub.report(http.StatusOK, committedTransfer("txref-p7-gate-settled", "2500.00"))
	response = env.confirmViaHTTP(t, reserved.BookingID, "txref-p7-gate-settled", reserved.Version, switchToken)
	if response.Code != http.StatusOK {
		t.Fatalf("verified confirmation: status = %d, body = %s", response.Code, response.Body.String())
	}
	found := env.bookingStatus(t, reserved.BookingID)
	if found.Status != booking.StatusPaid || found.PaymentReceiptRef == nil || *found.PaymentReceiptRef != "txref-p7-gate-settled" {
		t.Fatalf("settled booking = %#v", found)
	}

	replay := env.confirmViaHTTP(t, reserved.BookingID, "txref-p7-gate-settled", reserved.Version, switchToken)
	if replay.Code != http.StatusOK {
		t.Fatalf("idempotent replay: status = %d, body = %s", replay.Code, replay.Body.String())
	}
	afterReplay := env.bookingStatus(t, reserved.BookingID)
	if afterReplay.Version != found.Version {
		t.Fatalf("replay bumped version %d -> %d", found.Version, afterReplay.Version)
	}
	var paidEvents int
	if err := env.store.Pool().QueryRow(env.ctx, `SELECT count(*) FROM platform_outbox WHERE event_type='booking.paid'`).Scan(&paidEvents); err != nil {
		t.Fatalf("count paid events: %v", err)
	}
	if paidEvents != 1 {
		t.Fatalf("booking.paid outbox events = %d, want exactly 1 across confirmation + replay", paidEvents)
	}
}
