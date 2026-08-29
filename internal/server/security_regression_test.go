package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
