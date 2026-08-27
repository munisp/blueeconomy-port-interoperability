// Package ussd implements the Africa's Talking-style USSD callback handler for
// eCallUp booking status checks and slot booking. Sessions are held in Redis
// with a TTL; the handler fails closed when the session store is unavailable.
package ussd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/redis/go-redis/v9"
)

// Directory is the booking operations the USSD flow may perform.
type Directory interface {
	BookingStatus(ctx context.Context, bookingID string) (booking.Booking, error)
	BookSlotByID(ctx context.Context, slotID, truckPlate, msisdn, requestID string) (booking.Booking, error)
}

// Session is the Redis-held state of one USSD dialogue.
type Session struct {
	PendingSlotID string `json:"pending_slot_id,omitempty"`
	BookedID      string `json:"booked_id,omitempty"`
	UpdatedAt     int64  `json:"updated_at"`
}

type SessionStore struct {
	client *redis.Client
	ttl    time.Duration
}

// NewSessionStore fails closed without a configured Redis client and TTL.
func NewSessionStore(client *redis.Client, ttl time.Duration) (*SessionStore, error) {
	if client == nil {
		return nil, errors.New("USSD session store requires a configured Redis client")
	}
	if ttl <= 0 {
		return nil, errors.New("USSD session TTL must be positive")
	}
	return &SessionStore{client: client, ttl: ttl}, nil
}

func (store *SessionStore) key(sessionID string) string {
	return "ussd:session:" + sessionID
}

func (store *SessionStore) Get(ctx context.Context, sessionID string) (Session, error) {
	payload, err := store.client.Get(ctx, store.key(sessionID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Session{}, nil
	}
	if err != nil {
		return Session{}, fmt.Errorf("read USSD session: %w", err)
	}
	var session Session
	if err := json.Unmarshal(payload, &session); err != nil {
		return Session{}, fmt.Errorf("decode USSD session: %w", err)
	}
	return session, nil
}

func (store *SessionStore) Put(ctx context.Context, sessionID string, session Session) error {
	session.UpdatedAt = time.Now().Unix()
	payload, err := json.Marshal(session)
	if err != nil {
		return fmt.Errorf("encode USSD session: %w", err)
	}
	if err := store.client.Set(ctx, store.key(sessionID), payload, store.ttl).Err(); err != nil {
		return fmt.Errorf("write USSD session: %w", err)
	}
	return nil
}

type Handler struct {
	directory Directory
	sessions  *SessionStore
}

func NewHandler(directory Directory, sessions *SessionStore) (*Handler, error) {
	if directory == nil || sessions == nil {
		return nil, errors.New("USSD handler requires a booking directory and a session store")
	}
	return &Handler{directory: directory, sessions: sessions}, nil
}

const menu = "Welcome to eCallUp 2.0\n1. Booking status\n2. Book a slot"

// Callback handles POST /ussd/callback with Africa's Talking form fields
// (sessionId, phoneNumber, text). Responses use the CON/END convention.
func (handler *Handler) Callback(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := request.ParseForm(); err != nil {
		http.Error(response, "invalid form", http.StatusBadRequest)
		return
	}
	sessionID := strings.TrimSpace(request.Form.Get("sessionId"))
	phoneNumber := strings.TrimSpace(request.Form.Get("phoneNumber"))
	text := request.Form.Get("text")
	if sessionID == "" || len(sessionID) > 128 || phoneNumber == "" || len(phoneNumber) > 32 {
		http.Error(response, "sessionId and phoneNumber are required", http.StatusBadRequest)
		return
	}
	reply, err := handler.step(request.Context(), sessionID, phoneNumber, text)
	if err != nil {
		http.Error(response, "session store unavailable", http.StatusServiceUnavailable)
		return
	}
	response.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = response.Write([]byte(reply))
}

func (handler *Handler) step(ctx context.Context, sessionID, phoneNumber, text string) (string, error) {
	session, err := handler.sessions.Get(ctx, sessionID)
	if err != nil {
		return "", err
	}
	parts := strings.Split(text, "*")
	if text == "" {
		return "CON " + menu, nil
	}
	switch parts[0] {
	case "1":
		if len(parts) == 1 || strings.TrimSpace(parts[1]) == "" {
			return "CON Enter booking ID", nil
		}
		found, err := handler.directory.BookingStatus(ctx, strings.TrimSpace(parts[1]))
		if errors.Is(err, booking.ErrNotFound) {
			return "END Booking not found", nil
		}
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("END Booking %s\nStatus: %s\nPlate: %s", found.BookingID, found.Status, found.TruckPlate), nil
	case "2":
		switch {
		case len(parts) == 1 || strings.TrimSpace(parts[1]) == "":
			return "CON Enter slot ID", nil
		case len(parts) == 2 || strings.TrimSpace(parts[2]) == "":
			session.PendingSlotID = strings.TrimSpace(parts[1])
			if err := handler.sessions.Put(ctx, sessionID, session); err != nil {
				return "", err
			}
			return "CON Enter truck plate", nil
		default:
			slotID := strings.TrimSpace(parts[1])
			if session.PendingSlotID != "" && session.PendingSlotID != slotID {
				return "END Slot changed mid-session; start again", nil
			}
			plate := strings.ToUpper(strings.TrimSpace(parts[2]))
			reserved, err := handler.directory.BookSlotByID(ctx, slotID, plate, phoneNumber, "ussd-"+sessionID)
			if errors.Is(err, booking.ErrSlotUnavailable) {
				return "END Slot is full; choose another slot", nil
			}
			if errors.Is(err, booking.ErrNotFound) {
				return "END Unknown slot", nil
			}
			if err != nil {
				return "END Booking could not be completed: " + err.Error(), nil
			}
			session.PendingSlotID = ""
			session.BookedID = reserved.BookingID
			if err := handler.sessions.Put(ctx, sessionID, session); err != nil {
				return "", err
			}
			return fmt.Sprintf("END Booking confirmed\nID: %s\nStatus: %s", reserved.BookingID, reserved.Status), nil
		}
	default:
		return "END Invalid choice", nil
	}
}
