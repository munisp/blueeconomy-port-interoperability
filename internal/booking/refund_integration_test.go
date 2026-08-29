package booking

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	tb "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/munisp/blueeconomy-port-interoperability/internal/ledger"
)

// GAP-PIO-01(b): the refund rail settles a paid booking back through the
// ledger exactly once. Cancelling a PAID booking posts one compensating
// refund (operator share + FGN share re-balancing the paid amount), lands in
// REFUNDED with the refund commit hash, and emits one booking.refunded
// event. Every retry path afterwards — a repeated cancel, the expiry sweeper
// — must refuse or skip, never post a second refund.
func TestIntegration_RefundRailPostsCompensatingTransferExactlyOnce(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	refunder := &fakeRefunder{}
	if err := env.store.SetRefundPoster(refunder, 250); err != nil {
		t.Fatalf("wire refund poster: %v", err)
	}
	terminalID, slot := env.makeTerminalAndSlot(t, 1)
	reserved := env.reserveForPayment(t, terminalID, "req-p7-refund-0001", slot.SlotID)
	paid := env.confirmPaid(t, reserved)

	cancelled, err := env.store.Cancel(env.ctx, paid.BookingID, paid.Version, "trucker withdrew", Principal{ID: "test-trucker", Role: "trucker"})
	if err != nil {
		t.Fatalf("cancel paid booking: %v", err)
	}
	if cancelled.Status != StatusRefunded {
		t.Fatalf("status = %s, want REFUNDED", cancelled.Status)
	}
	if cancelled.LedgerCommitHash == nil || *cancelled.LedgerCommitHash != ledger.RefundCommitHash(paid.BookingID) {
		t.Fatalf("ledger commit hash = %v, want the deterministic refund hash", cancelled.LedgerCommitHash)
	}
	refunds := refunder.captured()
	if len(refunds) != 1 {
		t.Fatalf("refund postings = %d, want exactly 1", len(refunds))
	}
	// 250000 kobo at 250 bps FGN share: 6250 FGN + 243750 operator = 250000.
	if refunds[0].AmountKobo != 250000 || refunds[0].FgnShareKobo != 6250 || refunds[0].BookingID != paid.BookingID {
		t.Fatalf("refund = %#v", refunds[0])
	}
	if got := env.countOutboxEvents(t, "booking.refunded"); got != 1 {
		t.Fatalf("booking.refunded outbox events = %d, want exactly 1", got)
	}

	// Retry path 1: a repeated cancel with the pre-cancel version conflicts
	// and must not post again.
	if _, err := env.store.Cancel(env.ctx, paid.BookingID, paid.Version, "duplicate cancel", Principal{ID: "test-trucker", Role: "trucker"}); !errors.Is(err, ErrOptimisticConflict) {
		t.Fatalf("duplicate cancel: err = %v, want ErrOptimisticConflict", err)
	}
	// Retry path 2: even with the current version the terminal REFUNDED
	// state refuses any further transition.
	found, err := env.store.Get(env.ctx, paid.BookingID)
	if err != nil {
		t.Fatalf("get booking: %v", err)
	}
	if _, err := env.store.Cancel(env.ctx, paid.BookingID, found.Version, "second cancel", Principal{ID: "test-trucker", Role: "trucker"}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("cancel of REFUNDED booking: err = %v, want ErrInvalidTransition", err)
	}
	// Retry path 3: the sweeper has nothing left to refund.
	swept, err := env.store.ExpireDue(env.ctx, paid.ExpiresAt.Add(time.Hour), Principal{ID: "booking-worker", Role: "callup-engine"})
	if err != nil {
		t.Fatalf("expire sweep: %v", err)
	}
	if swept != 0 {
		t.Fatalf("sweeper touched %d bookings after refund, want 0", swept)
	}
	if got := len(refunder.captured()); got != 1 {
		t.Fatalf("refund postings after all retry paths = %d, want exactly 1", got)
	}
	if got := env.countOutboxEvents(t, "booking.refunded"); got != 1 {
		t.Fatalf("booking.refunded outbox events after retries = %d, want exactly 1", got)
	}
}

// tbClient opens a raw TigerBeetle client against the replica under test.
func tbClient(t *testing.T, addresses []string) tb.Client {
	t.Helper()
	client, err := tb.NewClient(tb.ToUint128(0), addresses)
	if err != nil {
		t.Fatalf("open tigerbeetle client: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func tbBalance(t *testing.T, client tb.Client, role string) (uint64, uint64) {
	t.Helper()
	accounts, err := client.LookupAccounts([]tb.Uint128{ledger.AccountID(role)})
	if err != nil || len(accounts) != 1 {
		t.Fatalf("lookup %s account: %v (%d accounts)", role, err, len(accounts))
	}
	return accounts[0].DebitsPosted.BigInt().Uint64(), accounts[0].CreditsPosted.BigInt().Uint64()
}

// GAP-PIO-01(b) against the real ledger: with a live TigerBeetle replica
// (TIGERBEETLE_TEST_ADDRESS, e.g. 127.0.0.1:3000), the settlement commits,
// the cancel refund mirrors it back to the trucker clearing account, and
// every ledger-level or store-level retry is a no-op — balances move exactly
// once. Skipped when no replica is configured.
func TestIntegration_TigerBeetleRefundRailSettlesExactlyOnce(t *testing.T) {
	address := os.Getenv("TIGERBEETLE_TEST_ADDRESS")
	if address == "" {
		t.Skip("TIGERBEETLE_TEST_ADDRESS is not set; skipping live TigerBeetle refund-rail tests")
	}
	env := newTestEnv(t)
	defer env.cleanup()
	addresses := strings.Split(address, ",")
	realLedger, err := ledger.NewTigerBeetle("0", addresses)
	if err != nil {
		t.Fatalf("open tigerbeetle ledger: %v", err)
	}
	defer realLedger.Close()
	if err := env.store.SetRefundPoster(realLedger, 250); err != nil {
		t.Fatalf("wire real refund rail: %v", err)
	}
	client := tbClient(t, addresses)

	terminalID, slot := env.makeTerminalAndSlot(t, 1)
	reserved := env.reserveForPayment(t, terminalID, "req-p7-tbrefund-0001", slot.SlotID)
	paid := env.confirmPaid(t, reserved)

	// Settle at the ledger first, as the booking workflow's CommitLedger
	// activity does after payment.
	if _, err := realLedger.CommitBookingSettlement(context.Background(), ledger.Settlement{
		BookingID:    paid.BookingID,
		AmountKobo:   250000,
		FgnShareKobo: 6250,
	}); err != nil {
		t.Fatalf("commit settlement: %v", err)
	}
	// Settlement commit retries are idempotent at the ledger.
	if _, err := realLedger.CommitBookingSettlement(context.Background(), ledger.Settlement{
		BookingID:    paid.BookingID,
		AmountKobo:   250000,
		FgnShareKobo: 6250,
	}); err != nil {
		t.Fatalf("settlement commit retry must be idempotent: %v", err)
	}
	_, operatorCredit := tbBalance(t, client, "terminal-operator")
	_, fgnCredit := tbBalance(t, client, "fgn-share")
	if operatorCredit != 243750 || fgnCredit != 6250 {
		t.Fatalf("settlement balances: operator credited %d, fgn credited %d; want 243750/6250 exactly once", operatorCredit, fgnCredit)
	}

	cancelled, err := env.store.Cancel(env.ctx, paid.BookingID, paid.Version, "trucker withdrew", Principal{ID: "test-trucker", Role: "trucker"})
	if err != nil {
		t.Fatalf("cancel paid booking: %v", err)
	}
	if cancelled.Status != StatusRefunded {
		t.Fatalf("status = %s, want REFUNDED", cancelled.Status)
	}
	if cancelled.LedgerCommitHash == nil || *cancelled.LedgerCommitHash != ledger.RefundCommitHash(paid.BookingID) {
		t.Fatalf("ledger commit hash = %v, want the deterministic refund hash", cancelled.LedgerCommitHash)
	}

	// The refund mirrors the settlement back through the ledger: operator
	// and FGN shares debited, trucker clearing credited with the full amount.
	operatorDebit, _ := tbBalance(t, client, "terminal-operator")
	fgnDebit, _ := tbBalance(t, client, "fgn-share")
	_, clearingCredit := tbBalance(t, client, "trucker-clearing")
	if operatorDebit != 243750 || fgnDebit != 6250 || clearingCredit != 250000 {
		t.Fatalf("refund balances: operator debited %d, fgn debited %d, clearing credited %d; want 243750/6250/250000", operatorDebit, fgnDebit, clearingCredit)
	}

	// A direct refund retry reuses the deterministic transfer ids: accepted
	// as idempotent at the ledger, balances unchanged.
	if _, err := realLedger.RefundBookingSettlement(context.Background(), ledger.Refund{
		BookingID:    paid.BookingID,
		AmountKobo:   250000,
		FgnShareKobo: 6250,
	}); err != nil {
		t.Fatalf("refund retry must be idempotent at the ledger: %v", err)
	}
	// A store-level retry conflicts and posts nothing.
	if _, err := env.store.Cancel(env.ctx, paid.BookingID, paid.Version, "duplicate cancel", Principal{ID: "test-trucker", Role: "trucker"}); err == nil {
		t.Fatal("duplicate cancel of a REFUNDED booking must fail")
	}
	operatorDebit, _ = tbBalance(t, client, "terminal-operator")
	fgnDebit, _ = tbBalance(t, client, "fgn-share")
	_, clearingCredit = tbBalance(t, client, "trucker-clearing")
	if operatorDebit != 243750 || fgnDebit != 6250 || clearingCredit != 250000 {
		t.Fatalf("balances after retries: operator %d, fgn %d, clearing %d; refunds must post exactly once", operatorDebit, fgnDebit, clearingCredit)
	}
	if got := env.countOutboxEvents(t, "booking.refunded"); got != 1 {
		t.Fatalf("booking.refunded outbox events = %d, want exactly 1", got)
	}
}
