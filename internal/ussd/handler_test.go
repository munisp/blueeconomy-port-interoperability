package ussd

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/redis/go-redis/v9"
)

type fakeDirectory struct {
	bookings map[string]booking.Booking
	slots    map[string]booking.Booking
	fail     error
}

func (directory *fakeDirectory) BookingStatus(_ context.Context, bookingID string) (booking.Booking, error) {
	if directory.fail != nil {
		return booking.Booking{}, directory.fail
	}
	found, ok := directory.bookings[bookingID]
	if !ok {
		return booking.Booking{}, booking.ErrNotFound
	}
	return found, nil
}

func (directory *fakeDirectory) BookSlotByID(_ context.Context, slotID, truckPlate, msisdn, requestID string) (booking.Booking, error) {
	if directory.fail != nil {
		return booking.Booking{}, directory.fail
	}
	reserved, ok := directory.slots[slotID]
	if !ok {
		return booking.Booking{}, booking.ErrNotFound
	}
	reserved.TruckPlate = truckPlate
	reserved.TruckerMSISDN = msisdn
	reserved.RequestID = requestID
	return reserved, nil
}

func newTestHandler(t *testing.T, directory Directory) (*Handler, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { client.Close() })
	sessions, err := NewSessionStore(client, 5*time.Minute)
	if err != nil {
		t.Fatalf("build session store: %v", err)
	}
	handler, err := NewHandler(directory, sessions)
	if err != nil {
		t.Fatalf("build handler: %v", err)
	}
	return handler, server
}

func callback(t *testing.T, handler *Handler, sessionID, phone, text string) (int, string) {
	t.Helper()
	form := url.Values{"sessionId": {sessionID}, "phoneNumber": {phone}, "text": {text}, "serviceCode": {"*384*123#"}, "networkCode": {"62130"}}
	request := httptest.NewRequest(http.MethodPost, "/ussd/callback", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.Callback(response, request)
	return response.Code, response.Body.String()
}

func TestUssdMenuAndBookingStatusFlow(t *testing.T) {
	directory := &fakeDirectory{bookings: map[string]booking.Booking{
		"BK-001": {BookingID: "BK-001", Status: booking.StatusPaid, TruckPlate: "LAG-123-XY"},
	}}
	handler, _ := newTestHandler(t, directory)

	code, body := callback(t, handler, "sess-1", "+2348012345678", "")
	if code != http.StatusOK || !strings.HasPrefix(body, "CON ") || !strings.Contains(body, "1. Booking status") {
		t.Fatalf("menu response = %d %q", code, body)
	}
	code, body = callback(t, handler, "sess-1", "+2348012345678", "1")
	if code != http.StatusOK || body != "CON Enter booking ID" {
		t.Fatalf("status prompt = %d %q", code, body)
	}
	code, body = callback(t, handler, "sess-1", "+2348012345678", "1*BK-001")
	if code != http.StatusOK || !strings.Contains(body, "Status: PAID") {
		t.Fatalf("status response = %d %q", code, body)
	}
	code, body = callback(t, handler, "sess-1", "+2348012345678", "1*BK-UNKNOWN")
	if code != http.StatusOK || body != "END Booking not found" {
		t.Fatalf("unknown booking response = %d %q", code, body)
	}
}

func TestUssdSlotBookingFlowUsesSession(t *testing.T) {
	directory := &fakeDirectory{slots: map[string]booking.Booking{
		"SLOT-1": {BookingID: "BK-777", Status: booking.StatusSlotReserved},
	}}
	handler, redisServer := newTestHandler(t, directory)

	code, body := callback(t, handler, "sess-2", "+2348012345678", "2")
	if code != http.StatusOK || body != "CON Enter slot ID" {
		t.Fatalf("slot prompt = %d %q", code, body)
	}
	// Choosing the slot stores it in the Redis session.
	code, body = callback(t, handler, "sess-2", "+2348012345678", "2*SLOT-1")
	if code != http.StatusOK || body != "CON Enter truck plate" {
		t.Fatalf("plate prompt = %d %q", code, body)
	}
	if !redisServer.Exists("ussd:session:sess-2") {
		t.Fatal("session must be persisted in Redis")
	}
	// A mid-session switch to a different slot is rejected.
	code, body = callback(t, handler, "sess-2", "+2348012345678", "2*SLOT-2*LAG-123-XY")
	if code != http.StatusOK || !strings.Contains(body, "start again") {
		t.Fatalf("slot switch response = %d %q", code, body)
	}
	// Completing the dialogue books the slot.
	code, body = callback(t, handler, "sess-2", "+2348012345678", "2*SLOT-1*lag-123-xy")
	if code != http.StatusOK || !strings.Contains(body, "Booking confirmed") || !strings.Contains(body, "BK-777") {
		t.Fatalf("booking confirmation = %d %q", code, body)
	}
}

func TestUssdFailsClosedWhenSessionStoreIsDown(t *testing.T) {
	directory := &fakeDirectory{}
	handler, redisServer := newTestHandler(t, directory)
	redisServer.Close()
	code, _ := callback(t, handler, "sess-3", "+2348012345678", "2*SLOT-1")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("response with Redis down = %d, want 503 (fail-closed)", code)
	}
}

func TestUssdRejectsMalformedCallback(t *testing.T) {
	directory := &fakeDirectory{}
	handler, _ := newTestHandler(t, directory)
	form := url.Values{"text": {""}}
	request := httptest.NewRequest(http.MethodPost, "/ussd/callback", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.Callback(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("missing sessionId response = %d, want 400", response.Code)
	}
}

func TestSessionStoreFailsClosedWhenUnconfigured(t *testing.T) {
	if _, err := NewSessionStore(nil, time.Minute); err == nil {
		t.Fatal("session store without Redis client must fail closed")
	}
	if _, err := NewSessionStore(redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}), 0); err == nil {
		t.Fatal("session store without TTL must fail closed")
	}
	if _, err := NewHandler(nil, nil); err == nil {
		t.Fatal("handler without dependencies must fail closed")
	}
}

func TestDirectoryErrorSurfaces(t *testing.T) {
	directory := &fakeDirectory{fail: errors.New("database unreachable")}
	handler, _ := newTestHandler(t, directory)
	code, _ := callback(t, handler, "sess-4", "+2348012345678", "1*BK-001")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("directory error response code = %d, want 503 (fail-closed)", code)
	}
}
