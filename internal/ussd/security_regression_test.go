package ussd

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/queue"
)

// PI-6 regression: the carrier callback is authenticated with the shared
// secret; a missing or wrong secret is a 401 and the handler never runs.
func TestCallbackRequiresSharedSecret(t *testing.T) {
	handlerCalled := false
	inner := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { handlerCalled = true })
	authenticated, err := AuthenticateCallback("carrier-shared-secret", inner)
	if err != nil {
		t.Fatalf("build authenticated callback: %v", err)
	}
	for name, header := range map[string]string{
		"missing secret": "",
		"wrong secret":   "attacker-guess",
		"prefix secret":  "carrier-shared",
	} {
		handlerCalled = false
		request := httptest.NewRequest(http.MethodPost, "/ussd/callback", nil)
		if header != "" {
			request.Header.Set(CallbackSecretHeader, header)
		}
		response := httptest.NewRecorder()
		authenticated.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized || handlerCalled {
			t.Fatalf("%s: code = %d, handlerCalled = %v; want 401 and handler untouched", name, response.Code, handlerCalled)
		}
	}
	request := httptest.NewRequest(http.MethodPost, "/ussd/callback", nil)
	request.Header.Set(CallbackSecretHeader, "carrier-shared-secret")
	response := httptest.NewRecorder()
	authenticated.ServeHTTP(response, request)
	if !handlerCalled || response.Code == http.StatusUnauthorized {
		t.Fatalf("correct secret must reach the handler: code = %d, called = %v", response.Code, handlerCalled)
	}
	// Boot-fail when unconfigured.
	if _, err := AuthenticateCallback("", inner); err == nil {
		t.Fatal("callback authentication without a shared secret must fail closed")
	}
}

// PI-6 regression: booking status is bound to the session MSISDN — another
// phone's booking answers "not found", never its state.
func TestBookingStatusIsBoundToSessionMSISDN(t *testing.T) {
	directory := &fakeDirectory{bookings: map[string]booking.Booking{
		"BK-001": {BookingID: "BK-001", Status: booking.StatusPaid, TruckPlate: "LAG-123-XY", TruckerMSISDN: "08012345678"},
	}}
	queueDirectory := &fakeQueueDirectory{requests: map[string]queue.Request{}}
	handler, _ := newTestHandler(t, directory, queueDirectory)

	// Owner in local format, session in international format: matches.
	code, body := callback(t, handler, "sess-b1", "+2348012345678", "1*BK-001")
	if code != http.StatusOK || !strings.Contains(body, "Status: PAID") {
		t.Fatalf("owner status = %d %q", code, body)
	}
	// Another phone asking for the same booking id gets not-found.
	code, body = callback(t, handler, "sess-b2", "+2348099999999", "1*BK-001")
	if code != http.StatusOK || body != "END Booking not found" {
		t.Fatalf("cross-MSISDN status = %d %q, want not found", code, body)
	}
}

// PI-6 regression: session state is namespaced per MSISDN — two phones using
// the same carrier sessionId hold independent sessions, and a phone can
// never ride another phone's in-flight dialogue.
func TestSessionStateIsNamespacedPerMSISDN(t *testing.T) {
	directory := &fakeDirectory{slots: map[string]booking.Booking{
		"SLOT-1": {BookingID: "BK-777", Status: booking.StatusSlotReserved},
	}}
	queueDirectory := &fakeQueueDirectory{requests: map[string]queue.Request{}}
	handler, redisServer := newTestHandler(t, directory, queueDirectory)

	// Phone A parks SLOT-1 in its session under sessionId "shared".
	if code, body := callback(t, handler, "shared", "+2348012345678", "2*SLOT-1"); code != http.StatusOK || body != "CON Enter truck plate" {
		t.Fatalf("phone A slot prompt = %d %q", code, body)
	}
	// Phone B with the same sessionId must not see phone A's parked slot:
	// its dialogue is fresh, so a one-step plate entry is treated as a slot
	// id, not a plate.
	code, body := callback(t, handler, "shared", "+2348099999999", "2*LAG-999-ZZ")
	if code != http.StatusOK || body != "CON Enter truck plate" {
		t.Fatalf("phone B must have an independent session = %d %q", code, body)
	}
	if !redisServer.Exists("ussd:session:+2348012345678:shared") || !redisServer.Exists("ussd:session:+2348099999999:shared") {
		t.Fatal("both MSISDN-namespaced sessions must coexist")
	}
}

// PI-6 regression: malformed phone numbers never anchor a session or booking.
func TestMalformedPhoneNumbersAreRejected(t *testing.T) {
	directory := &fakeDirectory{}
	queueDirectory := &fakeQueueDirectory{requests: map[string]queue.Request{}}
	handler, _ := newTestHandler(t, directory, queueDirectory)
	for _, phone := range []string{"", "not-a-number", "+234", "+", "12345678901234567890", "+23480DROP TABLE"} {
		form := "sessionId=sess-x&phoneNumber=" + phone + "&text="
		request := httptest.NewRequest(http.MethodPost, "/ussd/callback", strings.NewReader(form))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response := httptest.NewRecorder()
		handler.Callback(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("phone %q: code = %d, want 400", phone, response.Code)
		}
	}
}

// Compile-time contract: queue directory fakes stay MSISDN-aware.
var _ QueueDirectory = (*fakeQueueDirectory)(nil)
var _ Directory = (*fakeDirectory)(nil)

// Reference so the booking/queue imports are exercised in this test file.
var (
	_ = booking.ErrNotFound
	_ = queue.ErrNotFound
)
