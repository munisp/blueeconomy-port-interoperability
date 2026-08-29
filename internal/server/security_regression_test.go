package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"

	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/declarations"
	"github.com/munisp/blueeconomy-port-interoperability/internal/queue"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"testing"
	"time"
)

// gatewayKey mirrors testConfig's tenant gateway verifier key.
var gatewayKey = []byte("0123456789abcdef0123456789abcdef")

// mintToken builds an HS256 gateway token with verified role claims.
func mintToken(t *testing.T, subject string, roles ...string) string {
	t.Helper()
	head, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	payload, _ := json.Marshal(map[string]any{
		"iss": "gateway.blueeconomy.ng", "aud": "s1-port-interoperability",
		"tenant_id": "tenant-security-test", "sub": subject,
		"exp": time.Now().Add(time.Hour).Unix(), "roles": roles,
	})
	encoded := base64.RawURLEncoding.EncodeToString(head) + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, gatewayKey)
	mac.Write([]byte(encoded))
	return encoded + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// PI-1 regression: the payment-confirmation route is bound to the verified
// payment-switch role. A tenant user token — even a fully valid one — can
// never confirm a booking payment. The role gate runs before any storage, so
// no database is needed here.
func TestConfirmPaymentRequiresPaymentSwitchRole(t *testing.T) {
	handler := newWiredHandler(t)
	for name, token := range map[string]string{
		"role-less tenant user": mintToken(t, "trucker-1"),
		"trucker role":          mintToken(t, "trucker-1", RoleTrucker),
		"gate officer":          mintToken(t, "gate-1", RoleGateOfficer),
	} {
		request := loopbackRequest(http.MethodPost, "/v1/bookings/00000000-0000-0000-0000-000000000001/payment-confirmations",
			`{"receipt_ref":"anything","expected_version":1}`)
		request.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("%s: status = %d, want 403", name, response.Code)
		}
	}
}

// PI-4 regression: terminal and slot administration are bound to the verified
// port-operator-admin role. The role gate runs before any storage.
func TestTerminalAndSlotAdministrationRequireOperatorAdminRole(t *testing.T) {
	handler := newWiredHandler(t)
	for _, path := range []string{"/v1/terminals", "/v1/slots"} {
		for name, token := range map[string]string{
			"role-less tenant user": mintToken(t, "tenant-user"),
			"trucker":               mintToken(t, "trucker-1", RoleTrucker),
			"gate officer":          mintToken(t, "gate-1", RoleGateOfficer),
		} {
			request := loopbackRequest(http.MethodPost, path, `{}`)
			request.Header.Set("Authorization", "Bearer "+token)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusForbidden {
				t.Fatalf("%s %s: status = %d, want 403", name, path, response.Code)
			}
		}
	}
	// A token carrying the role passes the gate (fails later on validation).
	request := loopbackRequest(http.MethodPost, "/v1/terminals", `{"terminal_id":"BAD id"}`)
	request.Header.Set("Authorization", "Bearer "+mintToken(t, "ops-1", RolePortOperatorAdmin))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code == http.StatusForbidden || response.Code == http.StatusUnauthorized {
		t.Fatalf("operator admin must pass the role gate: status = %d", response.Code)
	}
}

// PI-4 regression: body-supplied actor identities are rejected outright.
func TestBodySuppliedActorFieldsAreRejected(t *testing.T) {
	handler := newWiredHandler(t)
	cases := map[string]string{
		"/v1/gate/scans":                       `{"booking_id":"b","gate_id":"GATE-A","scanned_by":"mallory"}`,
		"/v1/port-calls/call-1/clearance":      `{"expected_version":1,"decision":"APPROVED","reason":"ok","decided_by":"mallory"}`,
		"/v1/bookings/some-id/payment-intents": `{"request_id":"12345678","expected_version":1,"actor":"mallory"}`,
	}
	for path, body := range cases {
		request := loopbackRequest(http.MethodPost, path, body)
		request.Header.Set("Authorization", "Bearer "+mintToken(t, "officer-1", RoleGateOfficer, RoleNPAOfficer))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s with body actor field: status = %d, want 400", path, response.Code)
		}
	}
}

// PI-5 regression: booking reads are scoped to the creating subject; officer
// roles may read across subjects; cross-subject access is denied 403.
func TestBookingVisibilityScopesReadsToOwner(t *testing.T) {
	owner := "alice-trucker"
	found := booking.Booking{BookingID: "b-1", CreatedBy: &owner}
	legacy := booking.Booking{BookingID: "b-2"} // no recorded creator

	requestWithClaims := func(subject string, roles ...string) *http.Request {
		request := httptest.NewRequest(http.MethodGet, "/v1/bookings/b-1", nil)
		bound, err := tenantctx.WithClaims(request.Context(), tenantctx.Claims{
			Issuer: "gateway.blueeconomy.ng", Audience: "s1-port-interoperability",
			TenantID: "tenant-security-test", Subject: subject,
			Expires: time.Now().Add(time.Hour).Unix(), Roles: roles,
		})
		if err != nil {
			t.Fatalf("bind claims: %v", err)
		}
		return request.WithContext(bound)
	}
	visible := func(request *http.Request, b booking.Booking) (bool, int) {
		response := httptest.NewRecorder()
		ok := bookingVisible(response, request, b)
		return ok, response.Code
	}

	if ok, _ := visible(requestWithClaims("alice-trucker"), found); !ok {
		t.Fatal("owner must see their own booking")
	}
	if ok, code := visible(requestWithClaims("mallory"), found); ok || code != http.StatusForbidden {
		t.Fatalf("cross-subject read: ok = %v, code = %d, want false/403", ok, code)
	}
	for _, role := range []string{RoleGateOfficer, RoleNPAOfficer, RolePortOperatorAdmin, RolePaymentSwitch} {
		if ok, _ := visible(requestWithClaims("officer", role), found); !ok {
			t.Fatalf("officer role %s must read across subjects", role)
		}
	}
	// Legacy rows without a recorded creator are officer-readable only.
	if ok, code := visible(requestWithClaims("alice-trucker"), legacy); ok || code != http.StatusForbidden {
		t.Fatalf("legacy booking trader read: ok = %v, code = %d, want false/403", ok, code)
	}
	if ok, _ := visible(requestWithClaims("officer", RoleNPAOfficer), legacy); !ok {
		t.Fatal("legacy booking must be officer-readable")
	}
}

// PI-5 regression: declaration reads mirror list scoping — traders see only
// their own declarations; customs and NPA officers may read across traders.
func TestDeclarationVisibilityScopesReadsToTrader(t *testing.T) {
	declaration := declarations.Declaration{DeclarationID: "d-1", TraderID: "trader-ada"}
	requestWithClaims := func(subject string, roles ...string) *http.Request {
		request := httptest.NewRequest(http.MethodGet, "/v1/declarations/d-1", nil)
		bound, err := tenantctx.WithClaims(request.Context(), tenantctx.Claims{
			Issuer: "gateway.blueeconomy.ng", Audience: "s1-port-interoperability",
			TenantID: "tenant-security-test", Subject: subject,
			Expires: time.Now().Add(time.Hour).Unix(), Roles: roles,
		})
		if err != nil {
			t.Fatalf("bind claims: %v", err)
		}
		return request.WithContext(bound)
	}
	visible := func(request *http.Request) (bool, int) {
		response := httptest.NewRecorder()
		ok := declarationVisible(response, request, declaration)
		return ok, response.Code
	}
	if ok, _ := visible(requestWithClaims("trader-ada")); !ok {
		t.Fatal("owning trader must see their declaration")
	}
	if ok, code := visible(requestWithClaims("trader-mallory")); ok || code != http.StatusForbidden {
		t.Fatalf("cross-trader read: ok = %v, code = %d, want false/403", ok, code)
	}
	for _, role := range []string{RoleCustomsOfficer, RoleNPAOfficer} {
		if ok, _ := visible(requestWithClaims("officer", role)); !ok {
			t.Fatalf("officer role %s must read across traders", role)
		}
	}
}

// failingCallUps is a call-up orchestrator whose workflow starts always fail,
// modelling a Temporal outage.
type failingCallUps struct{ fakeCallUps }

func (failingCallUps) StartCallUpWorkflow(context.Context, queue.CallUpWorkflowInput) error {
	return errors.New("temporal unreachable")
}

// PI-10 regression: a failed grace-window workflow start for a promoted
// request is propagated, never swallowed — the helper returns the error so
// callers answer a retryable 502 and the sweeper re-ensures the workflow.
func TestStartPromotedCallUpsPropagatesStarterFailure(t *testing.T) {
	server := &Server{callUps: failingCallUps{}}
	deadline := time.Now().Add(time.Hour).UTC()
	promoted := &queue.Request{
		QueueRequestID: "QR-1", TenantID: "tenant-security-test",
		TerminalID: "APAPA-T1", Status: queue.StatusCalledUp, GraceDeadline: &deadline,
	}
	request := loopbackRequest(http.MethodPost, "/v1/queue-requests/QR-0/cancel",
		`{"expected_version":1,"reason":"test"}`)
	request.Header.Set("Authorization", "Bearer "+mintToken(t, "officer-1"))
	if err := server.startPromotedCallUps(request, promoted); err == nil {
		t.Fatal("starter failure must be propagated")
	}
	// No promotion or no grace deadline: no workflow needed, no error.
	serverOK := &Server{callUps: failingCallUps{}}
	if err := serverOK.startPromotedCallUps(request, nil); err != nil {
		t.Fatalf("nil promotion must be a no-op: %v", err)
	}
	noDeadline := &queue.Request{QueueRequestID: "QR-2", TenantID: "tenant-security-test", TerminalID: "APAPA-T1"}
	if err := serverOK.startPromotedCallUps(request, noDeadline); err != nil {
		t.Fatalf("promotion without grace deadline must be a no-op: %v", err)
	}
}
