package booking

import (
	"errors"
	"testing"
)

// reserveForPayment creates a booking and moves it to SLOT_RESERVED so the
// payment paths are reachable.
func (env testEnv) reserveForPayment(t *testing.T, terminalID, requestID string, slotID string) Booking {
	t.Helper()
	principal := Principal{ID: "test-trucker", Role: "trucker"}
	created := env.makeBooking(t, terminalID, requestID, ChannelWeb)
	reserved, err := env.store.ReserveSlot(env.ctx, created.BookingID, slotID, created.Version, principal)
	if err != nil {
		t.Fatalf("reserve slot: %v", err)
	}
	return reserved
}

func (env testEnv) makeIntent(t *testing.T, bookingID, requestID, txRef string, version int64) PaymentIntent {
	t.Helper()
	intent, err := env.store.CreatePaymentIntent(env.ctx, bookingID, requestID, txRef, version)
	if err != nil {
		t.Fatalf("create payment intent: %v", err)
	}
	return intent
}

// PI-1 regression: a caller-invented receipt reference must never pay a
// booking; only the exact switch-issued mojaloop_tx_ref of the booking's
// REQUESTED intent is accepted.
func TestConfirmPaymentBindsReceiptToSwitchIssuedTxRef(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, slot := env.makeTerminalAndSlot(t, 2)
	reserved := env.reserveForPayment(t, terminalID, "req-bind-0001", slot.SlotID)
	env.makeIntent(t, reserved.BookingID, "pay-bind-0001", "switch-txref-0001", reserved.Version)
	switchPrincipal := Principal{ID: "switch-service", Role: "payment-switch"}

	if _, err := env.store.ConfirmPayment(env.ctx, reserved.BookingID, "fabricated-by-caller", reserved.Version, switchPrincipal); !errors.Is(err, ErrPaymentInvalid) {
		t.Fatalf("fabricated receipt: err = %v, want ErrPaymentInvalid", err)
	}
	found, err := env.store.Get(env.ctx, reserved.BookingID)
	if err != nil {
		t.Fatalf("get booking: %v", err)
	}
	if found.Status != StatusSlotReserved {
		t.Fatalf("fabricated receipt changed status to %s", found.Status)
	}

	paid, err := env.store.ConfirmPayment(env.ctx, reserved.BookingID, "switch-txref-0001", reserved.Version, switchPrincipal)
	if err != nil {
		t.Fatalf("confirm with switch-issued ref: %v", err)
	}
	if paid.Status != StatusPaid || paid.PaymentReceiptRef == nil || *paid.PaymentReceiptRef != "switch-txref-0001" {
		t.Fatalf("paid = %#v", paid)
	}

	// Idempotent replay of the same confirmation returns the paid booking.
	replay, err := env.store.ConfirmPayment(env.ctx, reserved.BookingID, "switch-txref-0001", reserved.Version, switchPrincipal)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if replay.Status != StatusPaid || replay.Version != paid.Version {
		t.Fatalf("replay = %#v", replay)
	}
}


// PI-5 regression: bookings record the verified creating subject as the
// ownership anchor for read access control.
func TestCreateRecordsCreatorSubject(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, _ := env.makeTerminalAndSlot(t, 1)
	created := env.makeBooking(t, terminalID, "req-owner-0001", ChannelWeb)
	if created.CreatedBy == nil || *created.CreatedBy != "test-trucker" {
		t.Fatalf("created_by = %v, want test-trucker", created.CreatedBy)
	}
}
