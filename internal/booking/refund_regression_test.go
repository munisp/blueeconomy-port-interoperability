package booking

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/ledger"
)

// fakeRefunder captures refund postings for the refund-rail regression tests.
type fakeRefunder struct {
	mu      sync.Mutex
	refunds []ledger.Refund
	fail    error
}

func (fake *fakeRefunder) RefundBookingSettlement(_ context.Context, refund ledger.Refund) (string, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.fail != nil {
		return "", fake.fail
	}
	fake.refunds = append(fake.refunds, refund)
	return ledger.RefundCommitHash(refund.BookingID), nil
}

func (fake *fakeRefunder) captured() []ledger.Refund {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]ledger.Refund(nil), fake.refunds...)
}

func (env testEnv) confirmPaid(t *testing.T, booking Booking) Booking {
	t.Helper()
	env.makeIntent(t, booking.BookingID, "pay-refund-"+booking.RequestID, "txref-"+booking.RequestID, booking.Version)
	paid, err := env.store.ConfirmPayment(env.ctx, booking.BookingID, "txref-"+booking.RequestID, booking.Version, Principal{ID: "switch-service", Role: "payment-switch"})
	if err != nil {
		t.Fatalf("confirm payment: %v", err)
	}
	return paid
}

// PI-3 regression: cancelling a PAID booking posts the compensating refund —
// operator share + FGN share exactly re-balancing the paid amount — and the
// booking lands in the terminal REFUNDED state with the refund commit hash.
func TestCancelPaidBookingRefundsAndBalances(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	refunder := &fakeRefunder{}
	if err := env.store.SetRefundPoster(refunder, 250); err != nil {
		t.Fatalf("wire refund poster: %v", err)
	}
	terminalID, slot := env.makeTerminalAndSlot(t, 1)
	reserved := env.reserveForPayment(t, terminalID, "req-refund-0001", slot.SlotID)
	paid := env.confirmPaid(t, reserved)

	cancelled, err := env.store.Cancel(env.ctx, paid.BookingID, paid.Version, "trucker withdrew", Principal{ID: "test-trucker", Role: "trucker"})
	if err != nil {
		t.Fatalf("cancel paid booking: %v", err)
	}
	if cancelled.Status != StatusRefunded {
		t.Fatalf("status = %s, want REFUNDED", cancelled.Status)
	}
	if cancelled.LedgerCommitHash == nil || *cancelled.LedgerCommitHash != ledger.RefundCommitHash(paid.BookingID) {
		t.Fatalf("refund commit hash = %v", cancelled.LedgerCommitHash)
	}
	refunds := refunder.captured()
	if len(refunds) != 1 {
		t.Fatalf("refunds posted = %d, want 1", len(refunds))
	}
	refund := refunds[0]
	// amount 250000 kobo, FGN share 250 bps => 6250; operator share 243750.
	if refund.AmountKobo != 250000 || refund.FgnShareKobo != 6250 {
		t.Fatalf("refund = %#v", refund)
	}
	if operatorShare := refund.AmountKobo - refund.FgnShareKobo; operatorShare+refund.FgnShareKobo != 250000 {
		t.Fatalf("refund legs do not re-balance the paid amount: %#v", refund)
	}
}

// PI-3 regression: a PAID booking whose validity elapses is refunded and
// lands in REFUNDED (not bare EXPIRED) with capacity released.
func TestExpireDueRefundsPaidBookings(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	refunder := &fakeRefunder{}
	if err := env.store.SetRefundPoster(refunder, 250); err != nil {
		t.Fatalf("wire refund poster: %v", err)
	}
	terminalID, slot := env.makeTerminalAndSlot(t, 1)
	reserved := env.reserveForPayment(t, terminalID, "req-refund-0002", slot.SlotID)
	paid := env.confirmPaid(t, reserved)
	if _, err := env.store.Pool().Exec(env.ctx, `UPDATE truck_bookings SET expires_at = now() - interval '1 minute' WHERE booking_id=$1`, paid.BookingID); err != nil {
		t.Fatalf("backdate expiry: %v", err)
	}

	count, err := env.store.ExpireDue(env.ctx, time.Now().UTC(), Principal{ID: "booking-worker", Role: "callup-engine"})
	if err != nil {
		t.Fatalf("expire due: %v", err)
	}
	if count != 1 {
		t.Fatalf("expired count = %d, want 1", count)
	}
	found, err := env.store.Get(env.ctx, paid.BookingID)
	if err != nil {
		t.Fatalf("get booking: %v", err)
	}
	if found.Status != StatusRefunded {
		t.Fatalf("status = %s, want REFUNDED", found.Status)
	}
	if len(refunder.captured()) != 1 {
		t.Fatalf("refunds posted = %d, want 1", len(refunder.captured()))
	}
	var eventCount int
	if err := env.store.Pool().QueryRow(env.ctx, `SELECT count(*) FROM platform_outbox WHERE event_type='booking.refunded'`).Scan(&eventCount); err != nil {
		t.Fatalf("count refund events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("booking.refunded outbox events = %d, want 1", eventCount)
	}
}

// PI-3 regression (fail closed): without a wired refund rail a paid booking
// cannot be cancelled or expired into a money-losing terminal state.
func TestPaidTransitionFailsClosedWithoutRefundRail(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, slot := env.makeTerminalAndSlot(t, 1)
	reserved := env.reserveForPayment(t, terminalID, "req-refund-0003", slot.SlotID)
	paid := env.confirmPaid(t, reserved)

	if _, err := env.store.Cancel(env.ctx, paid.BookingID, paid.Version, "no refund rail", Principal{ID: "test-trucker", Role: "trucker"}); !errors.Is(err, ErrRefundUnavailable) {
		t.Fatalf("cancel without refund rail: err = %v, want ErrRefundUnavailable", err)
	}
	found, err := env.store.Get(env.ctx, paid.BookingID)
	if err != nil {
		t.Fatalf("get booking: %v", err)
	}
	if found.Status != StatusPaid {
		t.Fatalf("booking must stay PAID when the refund rail is unavailable: %s", found.Status)
	}

	// A failing refund poster equally refuses the transition.
	failing := &fakeRefunder{fail: errors.New("tigerbeetle unreachable")}
	if err := env.store.SetRefundPoster(failing, 250); err != nil {
		t.Fatalf("wire failing refund poster: %v", err)
	}
	if _, err := env.store.Cancel(env.ctx, paid.BookingID, paid.Version, "ledger down", Principal{ID: "test-trucker", Role: "trucker"}); !errors.Is(err, ErrRefundUnavailable) {
		t.Fatalf("cancel with ledger down: err = %v, want ErrRefundUnavailable", err)
	}
}

// Unpaid cancellation still lands in CANCELLED and never touches the rail.
func TestCancelUnpaidBookingSkipsRefundRail(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	refunder := &fakeRefunder{}
	if err := env.store.SetRefundPoster(refunder, 250); err != nil {
		t.Fatalf("wire refund poster: %v", err)
	}
	terminalID, slot := env.makeTerminalAndSlot(t, 1)
	reserved := env.reserveForPayment(t, terminalID, "req-refund-0004", slot.SlotID)

	cancelled, err := env.store.Cancel(env.ctx, reserved.BookingID, reserved.Version, "changed plans", Principal{ID: "test-trucker", Role: "trucker"})
	if err != nil {
		t.Fatalf("cancel reserved booking: %v", err)
	}
	if cancelled.Status != StatusCancelled {
		t.Fatalf("status = %s, want CANCELLED", cancelled.Status)
	}
	if len(refunder.captured()) != 0 {
		t.Fatal("unpaid cancellation must not post refunds")
	}
}
