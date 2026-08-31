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
	"github.com/munisp/blueeconomy-port-interoperability/internal/queue"
	"github.com/redis/go-redis/v9"
)

type fakeDirectory struct {
	bookings map[string]booking.Booking
	slots    map[string]booking.Booking
	fail     error
}

func (directory *fakeDirectory) BookingStatus(_ context.Context, bookingID, msisdn string) (booking.Booking, error) {
	if directory.fail != nil {
		return booking.Booking{}, directory.fail
	}
	found, ok := directory.bookings[bookingID]
	if !ok || !MSISDNMatches(found.TruckerMSISDN, msisdn) {
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

type fakeQueueDirectory struct {
	requests map[string]queue.Request
	fail     error
}

func (directory *fakeQueueDirectory) RequestQueueEntry(_ context.Context, terminalID, truckPlate, msisdn, requestID string) (queue.Request, error) {
	if directory.fail != nil {
		return queue.Request{}, directory.fail
	}
	if terminalID != "APAPA-T1" {
		return queue.Request{}, queue.ErrNotFound
	}
	// Idempotent per request id: replay returns the retained request.
	if retained, ok := directory.requests[requestID]; ok {
		return retained, nil
	}
	position := int64(7)
	created := queue.Request{
		QueueRequestID: "QR-" + requestID,
		IdempotencyKey: requestID,
		TruckPlate:     truckPlate,
		TruckerMSISDN:  msisdn,
		TerminalID:     terminalID,
		PriorityClass:  queue.ClassStandard,
		Status:         queue.StatusQueued,
		Position:       &position,
	}
	directory.requests[requestID] = created
	return created, nil
}

func (directory *fakeQueueDirectory) QueueStatus(_ context.Context, queueRequestID, msisdn string) (queue.Request, error) {
	if directory.fail != nil {
		return queue.Request{}, directory.fail
	}
	for _, request := range directory.requests {
		if request.QueueRequestID == queueRequestID && MSISDNMatches(request.TruckerMSISDN, msisdn) {
			return request, nil
		}
	}
	return queue.Request{}, queue.ErrNotFound
}

func newTestHandler(t *testing.T, directory Directory, queues QueueDirectory) (*Handler, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { client.Close() })
	sessions, err := NewSessionStore(client, 5*time.Minute)
	if err != nil {
		t.Fatalf("build session store: %v", err)
	}
	handler, err := NewHandler(directory, queues, sessions)
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
		"BK-001": {BookingID: "BK-001", Status: booking.StatusPaid, TruckPlate: "LAG-123-XY", TruckerMSISDN: "+2348012345678"},
	}}
	queueDirectory := &fakeQueueDirectory{requests: map[string]queue.Request{}}
	handler, _ := newTestHandler(t, directory, queueDirectory)

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
	queueDirectory := &fakeQueueDirectory{requests: map[string]queue.Request{}}
	handler, redisServer := newTestHandler(t, directory, queueDirectory)

	code, body := callback(t, handler, "sess-2", "+2348012345678", "2")
	if code != http.StatusOK || body != "CON Enter slot ID" {
		t.Fatalf("slot prompt = %d %q", code, body)
	}
	// Choosing the slot stores it in the Redis session.
	code, body = callback(t, handler, "sess-2", "+2348012345678", "2*SLOT-1")
	if code != http.StatusOK || body != "CON Enter truck plate" {
		t.Fatalf("plate prompt = %d %q", code, body)
	}
	if !redisServer.Exists("ussd:session:+2348012345678:sess-2") {
		t.Fatal("session must be persisted in Redis namespaced by the session MSISDN")
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
	queueDirectory := &fakeQueueDirectory{requests: map[string]queue.Request{}}
	handler, redisServer := newTestHandler(t, directory, queueDirectory)
	redisServer.Close()
	code, _ := callback(t, handler, "sess-3", "+2348012345678", "2*SLOT-1")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("response with Redis down = %d, want 503 (fail-closed)", code)
	}
}

func TestUssdRejectsMalformedCallback(t *testing.T) {
	directory := &fakeDirectory{}
	queueDirectory := &fakeQueueDirectory{requests: map[string]queue.Request{}}
	handler, _ := newTestHandler(t, directory, queueDirectory)
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
	if _, err := NewHandler(nil, nil, nil); err == nil {
		t.Fatal("handler without dependencies must fail closed")
	}
}

func TestUssdQueueEntryFlowIsIdempotentPerSession(t *testing.T) {
	directory := &fakeDirectory{}
	queueDirectory := &fakeQueueDirectory{requests: map[string]queue.Request{}}
	handler, redisServer := newTestHandler(t, directory, queueDirectory)

	code, body := callback(t, handler, "sess-q1", "+2348012345678", "")
	if code != http.StatusOK || !strings.Contains(body, "3. Request queue entry") || !strings.Contains(body, "4. Check queue position") {
		t.Fatalf("menu response = %d %q", code, body)
	}
	code, body = callback(t, handler, "sess-q1", "+2348012345678", "3")
	if code != http.StatusOK || body != "CON Enter terminal code" {
		t.Fatalf("terminal prompt = %d %q", code, body)
	}
	code, body = callback(t, handler, "sess-q1", "+2348012345678", "3*apapa-t1")
	if code != http.StatusOK || body != "CON Enter truck plate" {
		t.Fatalf("plate prompt = %d %q", code, body)
	}
	if !redisServer.Exists("ussd:session:+2348012345678:sess-q1") {
		t.Fatal("session must be persisted in Redis namespaced by the session MSISDN")
	}
	// A mid-session switch to a different terminal is rejected.
	code, body = callback(t, handler, "sess-q1", "+2348012345678", "3*OTHER-T1*LAG-123-XY")
	if code != http.StatusOK || !strings.Contains(body, "start again") {
		t.Fatalf("terminal switch response = %d %q", code, body)
	}
	// Completing the dialogue enters the queue with a position.
	code, body = callback(t, handler, "sess-q1", "+2348012345678", "3*APAPA-T1*lag-123-xy")
	if code != http.StatusOK || !strings.Contains(body, "Queue entry confirmed") || !strings.Contains(body, "Position: 7") {
		t.Fatalf("queue confirmation = %d %q", code, body)
	}
	// A replay of the same session returns the retained queue request.
	code, body = callback(t, handler, "sess-q1", "+2348012345678", "3*APAPA-T1*LAG-123-XY")
	if code != http.StatusOK || !strings.Contains(body, "QR-ussd-q-sess-q1") {
		t.Fatalf("idempotent replay = %d %q", code, body)
	}
	// Unknown terminal fails closed.
	code, body = callback(t, handler, "sess-q2", "+2348012345678", "3*NOWHERE-9*LAG-123-XY")
	if code != http.StatusOK || body != "END Unknown terminal" {
		t.Fatalf("unknown terminal response = %d %q", code, body)
	}
}

func TestUssdQueuePositionFlow(t *testing.T) {
	directory := &fakeDirectory{}
	queueDirectory := &fakeQueueDirectory{requests: map[string]queue.Request{}}
	handler, _ := newTestHandler(t, directory, queueDirectory)

	// Enter the queue, then check the position with the retained session id.
	if code, body := callback(t, handler, "sess-q3", "+2348012345678", "3*APAPA-T1*LAG-123-XY"); code != http.StatusOK || !strings.Contains(body, "Queue entry confirmed") {
		t.Fatalf("queue entry = %d %q", code, body)
	}
	code, body := callback(t, handler, "sess-q3", "+2348012345678", "4")
	if code != http.StatusOK || !strings.Contains(body, "Position: 7") || !strings.Contains(body, "Status: QUEUED") {
		t.Fatalf("queue position from session = %d %q", code, body)
	}
	// An explicit queue request id works from the same MSISDN's other
	// sessions — but never from another phone (PI-6 binding).
	code, body = callback(t, handler, "sess-q4", "+2348012345678", "4*QR-ussd-q-sess-q3")
	if code != http.StatusOK || !strings.Contains(body, "Status: QUEUED") {
		t.Fatalf("queue position by id = %d %q", code, body)
	}
	code, body = callback(t, handler, "sess-q4", "+2348099999999", "4*QR-ussd-q-sess-q3")
	if code != http.StatusOK || body != "END Queue request not found" {
		t.Fatalf("cross-MSISDN queue status must be not-found = %d %q", code, body)
	}
	code, body = callback(t, handler, "sess-q4", "+2348099999999", "4*QR-UNKNOWN")
	if code != http.StatusOK || body != "END Queue request not found" {
		t.Fatalf("unknown queue request response = %d %q", code, body)
	}
	// A fresh session without a stored id is prompted for one.
	code, body = callback(t, handler, "sess-q5", "+2348099999999", "4")
	if code != http.StatusOK || body != "CON Enter queue request ID" {
		t.Fatalf("queue id prompt = %d %q", code, body)
	}
}

func TestUssdQueueDirectoryErrorSurfaces(t *testing.T) {
	directory := &fakeDirectory{}
	queueDirectory := &fakeQueueDirectory{fail: errors.New("database unreachable")}
	handler, _ := newTestHandler(t, directory, queueDirectory)
	code, body := callback(t, handler, "sess-q6", "+2348012345678", "3*APAPA-T1*LAG-123-XY")
	if code != http.StatusOK || !strings.Contains(body, "could not be completed") {
		t.Fatalf("queue directory error response = %d %q", code, body)
	}
}

func TestDirectoryErrorSurfaces(t *testing.T) {
	directory := &fakeDirectory{fail: errors.New("database unreachable")}
	queueDirectory := &fakeQueueDirectory{requests: map[string]queue.Request{}}
	handler, _ := newTestHandler(t, directory, queueDirectory)
	code, _ := callback(t, handler, "sess-4", "+2348012345678", "1*BK-001")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("directory error response code = %d, want 503 (fail-closed)", code)
	}
}
