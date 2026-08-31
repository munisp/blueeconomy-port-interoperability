package booking

import (
	"errors"
	"sync"
	"testing"
)

// countOutboxEvents returns how many platform_outbox rows carry eventType.
// Each DB-gated test in this package runs against a freshly rebuilt schema
// (newTestEnv drops and recreates it) and tests do not run in parallel, so a
// whole-table count is scoped to the test's own bookings.
func (env testEnv) countOutboxEvents(t *testing.T, eventType string) int {
	t.Helper()
	var count int
	if err := env.store.Pool().QueryRow(env.ctx, `SELECT count(*) FROM platform_outbox WHERE event_type=$1`, eventType).Scan(&count); err != nil {
		t.Fatalf("count %s outbox events: %v", eventType, err)
	}
	return count
}

func (env testEnv) intentStatus(t *testing.T, bookingID string) string {
	t.Helper()
	var status string
	if err := env.store.Pool().QueryRow(env.ctx, `SELECT status FROM booking_payment_intents WHERE booking_id=$1`, bookingID).Scan(&status); err != nil {
		t.Fatalf("load intent status: %v", err)
	}
	return status
}

// GAP-PIO-01(a): the payment_receipt_ref replay path is the settlement
// boundary. A duplicate confirmation of the same switch-issued receipt (the
// switch re-delivered the callback, or the caller retried after the workflow
// signal failed) must return the already-paid booking unchanged — never a
// second transition, a second booking.paid event, or a re-completed intent.
func TestIntegration_PaymentReceiptReplayIsIdempotentAndSettlesOnce(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, slot := env.makeTerminalAndSlot(t, 1)
	reserved := env.reserveForPayment(t, terminalID, "req-p7-replay-0001", slot.SlotID)
	env.makeIntent(t, reserved.BookingID, "pay-p7-replay-0001", "txref-p7-replay-0001", reserved.Version)
	switchPrincipal := Principal{ID: "switch-service", Role: "payment-switch"}

	paid, err := env.store.ConfirmPayment(env.ctx, reserved.BookingID, "txref-p7-replay-0001", reserved.Version, switchPrincipal)
	if err != nil {
		t.Fatalf("confirm payment: %v", err)
	}
	if paid.Status != StatusPaid || paid.PaymentReceiptRef == nil || *paid.PaymentReceiptRef != "txref-p7-replay-0001" {
		t.Fatalf("paid = %#v", paid)
	}
	if got := env.intentStatus(t, paid.BookingID); got != "COMPLETED" {
		t.Fatalf("intent status = %s, want COMPLETED", got)
	}

	// Replay the identical confirmation three times: same booking row back,
	// no version bump, no new settlement work.
	for attempt := 1; attempt <= 3; attempt++ {
		replay, err := env.store.ConfirmPayment(env.ctx, reserved.BookingID, "txref-p7-replay-0001", reserved.Version, switchPrincipal)
		if err != nil {
			t.Fatalf("replay %d: %v", attempt, err)
		}
		if replay.Status != StatusPaid || replay.Version != paid.Version {
			t.Fatalf("replay %d mutated the booking: %#v", attempt, replay)
		}
		if replay.UpdatedAt != paid.UpdatedAt {
			t.Fatalf("replay %d touched updated_at: %v != %v", attempt, replay.UpdatedAt, paid.UpdatedAt)
		}
	}

	if got := env.countOutboxEvents(t, "booking.paid"); got != 1 {
		t.Fatalf("booking.paid outbox events = %d, want exactly 1 (no double-settlement)", got)
	}
	if got := env.intentStatus(t, paid.BookingID); got != "COMPLETED" {
		t.Fatalf("intent status after replays = %s, want COMPLETED", got)
	}
}

// GAP-PIO-01(d): two concurrent confirmations carrying the SAME switch
// receipt race on the booking row lock. Exactly one performs the
// SLOT_RESERVED -> PAID transition; the other observes the idempotent
// replay. The observable settlement footprint is one transition and one
// booking.paid event, however the race resolves.
func TestIntegration_ConcurrentDuplicatePaymentSettlesExactlyOnce(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, slot := env.makeTerminalAndSlot(t, 1)
	reserved := env.reserveForPayment(t, terminalID, "req-p7-race-0001", slot.SlotID)
	env.makeIntent(t, reserved.BookingID, "pay-p7-race-0001", "txref-p7-race-0001", reserved.Version)
	switchPrincipal := Principal{ID: "switch-service", Role: "payment-switch"}

	var wait sync.WaitGroup
	outcomes := make([]Booking, 2)
	failures := make([]error, 2)
	confirm := func(index int) {
		defer wait.Done()
		outcomes[index], failures[index] = env.store.ConfirmPayment(env.ctx, reserved.BookingID, "txref-p7-race-0001", reserved.Version, switchPrincipal)
	}
	wait.Add(2)
	go confirm(0)
	go confirm(1)
	wait.Wait()

	for index, err := range failures {
		if err != nil {
			t.Fatalf("concurrent confirmation %d failed: %v (same-receipt replay must be idempotent)", index, err)
		}
		if outcomes[index].Status != StatusPaid {
			t.Fatalf("concurrent confirmation %d left status %s", index, outcomes[index].Status)
		}
	}

	found, err := env.store.Get(env.ctx, reserved.BookingID)
	if err != nil {
		t.Fatalf("get booking: %v", err)
	}
	if found.Status != StatusPaid || found.PaymentReceiptRef == nil || *found.PaymentReceiptRef != "txref-p7-race-0001" {
		t.Fatalf("booking after race = %#v", found)
	}
	// Exactly one transition happened: the version moved once off the
	// reserved version, and exactly one settlement event exists.
	if found.Version != reserved.Version+1 {
		t.Fatalf("version = %d, want %d (exactly one PAID transition)", found.Version, reserved.Version+1)
	}
	if got := env.countOutboxEvents(t, "booking.paid"); got != 1 {
		t.Fatalf("booking.paid outbox events = %d, want exactly 1", got)
	}
}

// GAP-PIO-01(d): two concurrent confirmations carrying DIFFERENT receipts —
// one the switch-issued tx_ref of the booking's intent, one foreign. At most
// one may settle; the loser must surface a conflict/validation error and the
// booking must carry only the winning receipt. No partial or double payment
// is observable.
func TestIntegration_ConcurrentConflictingReceiptsSettleAtMostOnce(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, slot := env.makeTerminalAndSlot(t, 1)
	reserved := env.reserveForPayment(t, terminalID, "req-p7-race-0002", slot.SlotID)
	env.makeIntent(t, reserved.BookingID, "pay-p7-race-0002", "txref-p7-race-legit", reserved.Version)
	switchPrincipal := Principal{ID: "switch-service", Role: "payment-switch"}

	var wait sync.WaitGroup
	failures := make([]error, 2)
	confirm := func(index int, receiptRef string) {
		defer wait.Done()
		_, failures[index] = env.store.ConfirmPayment(env.ctx, reserved.BookingID, receiptRef, reserved.Version, switchPrincipal)
	}
	wait.Add(2)
	go confirm(0, "txref-p7-race-legit")
	go confirm(1, "txref-p7-race-foreign")
	wait.Wait()

	found, err := env.store.Get(env.ctx, reserved.BookingID)
	if err != nil {
		t.Fatalf("get booking: %v", err)
	}

	var paidCount int
	for index, err := range failures {
		if err == nil {
			paidCount++
			continue
		}
		// The only acceptable losing outcomes are the fabricated-receipt
		// refusal and the optimistic-lock conflict of the serialized loser.
		if !errors.Is(err, ErrPaymentInvalid) && !errors.Is(err, ErrOptimisticConflict) {
			t.Fatalf("loser %d: err = %v, want ErrPaymentInvalid or ErrOptimisticConflict", index, err)
		}
	}
	if paidCount > 1 {
		t.Fatalf("both racing confirmations settled a booking: %d successes", paidCount)
	}
	if found.Status == StatusPaid {
		// If the legitimate receipt won, it is the only receipt the booking
		// may carry, and exactly one settlement event exists.
		if found.PaymentReceiptRef == nil || *found.PaymentReceiptRef != "txref-p7-race-legit" {
			t.Fatalf("paid booking carries receipt %v, want the switch-issued ref only", found.PaymentReceiptRef)
		}
		if got := env.countOutboxEvents(t, "booking.paid"); got != 1 {
			t.Fatalf("booking.paid outbox events = %d, want exactly 1", got)
		}
	} else if found.Status != StatusSlotReserved {
		t.Fatalf("booking in unexpected state %s after the race", found.Status)
	}
}

// GAP-PIO-01(a): a replayed confirmation that carries the booking's settled
// receipt but a DIFFERENT booking must still hit the unique index — replay
// tolerance never extends across bookings.
func TestIntegration_ReceiptReplayAcrossBookingsStillConflicts(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, slot := env.makeTerminalAndSlot(t, 2)
	switchPrincipal := Principal{ID: "switch-service", Role: "payment-switch"}

	first := env.reserveForPayment(t, terminalID, "req-p7-xbook-0001", slot.SlotID)
	env.makeIntent(t, first.BookingID, "pay-p7-xbook-0001", "txref-p7-xbook-shared", first.Version)
	if _, err := env.store.ConfirmPayment(env.ctx, first.BookingID, "txref-p7-xbook-shared", first.Version, switchPrincipal); err != nil {
		t.Fatalf("pay first booking: %v", err)
	}

	second := env.reserveForPayment(t, terminalID, "req-p7-xbook-0002", slot.SlotID)
	env.makeIntent(t, second.BookingID, "pay-p7-xbook-0002", "txref-p7-xbook-shared", second.Version)
	if _, err := env.store.ConfirmPayment(env.ctx, second.BookingID, "txref-p7-xbook-shared", second.Version, switchPrincipal); !errors.Is(err, ErrPaymentReceiptReuse) {
		t.Fatalf("cross-booking replay: err = %v, want ErrPaymentReceiptReuse", err)
	}
	secondFound, err := env.store.Get(env.ctx, second.BookingID)
	if err != nil {
		t.Fatalf("get second booking: %v", err)
	}
	if secondFound.Status != StatusSlotReserved || secondFound.PaymentReceiptRef != nil {
		t.Fatalf("second booking must stay unpaid: %#v", secondFound)
	}
	if got := env.countOutboxEvents(t, "booking.paid"); got != 1 {
		t.Fatalf("booking.paid outbox events = %d, want exactly 1 (first booking only)", got)
	}
}
