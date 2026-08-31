package booking

import (
	"errors"
	"testing"
)

// PI-2 regression: one settlement receipt may back exactly one booking. Even
// if the switch (or a compromised intent path) reissues the same tx_ref for a
// second booking, the unique index rejects the second confirmation as a
// reuse conflict, never a paid booking.
func TestPaymentReceiptRefIsUniqueAcrossBookings(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, slot := env.makeTerminalAndSlot(t, 2)
	switchPrincipal := Principal{ID: "switch-service", Role: "payment-switch"}

	first := env.reserveForPayment(t, terminalID, "req-uniq-0001", slot.SlotID)
	env.makeIntent(t, first.BookingID, "pay-uniq-0001", "shared-txref-0001", first.Version)
	if _, err := env.store.ConfirmPayment(env.ctx, first.BookingID, "shared-txref-0001", first.Version, switchPrincipal); err != nil {
		t.Fatalf("pay first booking: %v", err)
	}

	second := env.reserveForPayment(t, terminalID, "req-uniq-0002", slot.SlotID)
	env.makeIntent(t, second.BookingID, "pay-uniq-0002", "switch-txref-0002", second.Version)
	// Simulate the switch reissuing the settled tx_ref for the second
	// booking's intent; the binding check alone would then pass.
	if _, err := env.store.Pool().Exec(env.ctx,
		`UPDATE booking_payment_intents SET mojaloop_tx_ref='shared-txref-0001' WHERE booking_id=$1`, second.BookingID); err != nil {
		t.Fatalf("re-point second intent at the settled ref: %v", err)
	}
	if _, err := env.store.ConfirmPayment(env.ctx, second.BookingID, "shared-txref-0001", second.Version, switchPrincipal); !errors.Is(err, ErrPaymentReceiptReuse) {
		t.Fatalf("reused receipt on second booking: err = %v, want ErrPaymentReceiptReuse", err)
	}
	secondFound, err := env.store.Get(env.ctx, second.BookingID)
	if err != nil {
		t.Fatalf("get second booking: %v", err)
	}
	if secondFound.Status != StatusSlotReserved || secondFound.PaymentReceiptRef != nil {
		t.Fatalf("second booking must stay unpaid after receipt reuse: %#v", secondFound)
	}
}
