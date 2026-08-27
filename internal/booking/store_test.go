package booking

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// These tests run against a real PostgreSQL when BOOKING_TEST_DATABASE_URL is
// set (see scripts/verify-local.sh and docker-compose.integration.yml). They
// are skipped otherwise; there is no in-memory substitute for the capacity
// trigger and row-lock semantics under test.

type testEnv struct {
	store   *Store
	ctx     context.Context
	cleanup func()
}

func newTestEnv(t *testing.T) testEnv {
	t.Helper()
	databaseURL := os.Getenv("BOOKING_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("BOOKING_TEST_DATABASE_URL is not set; skipping PostgreSQL-backed booking tests")
	}
	ctx := context.Background()
	store, err := Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if _, err := store.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset test schema: %v", err)
	}
	migrationDir := filepath.Join("..", "..", "db", "migrations")
	entries, err := filepath.Glob(filepath.Join(migrationDir, "*.sql"))
	if err != nil || len(entries) == 0 {
		t.Fatalf("find migrations: %v", err)
	}
	sort.Strings(entries)
	for _, entry := range entries {
		migration, err := os.ReadFile(entry)
		if err != nil {
			t.Fatalf("read migration %s: %v", entry, err)
		}
		if _, err := store.Exec(ctx, string(migration)); err != nil {
			t.Fatalf("apply migration %s: %v", entry, err)
		}
	}
	tenantID := fmt.Sprintf("tenant-test-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := store.Pool().Exec(ctx, `INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ($1, $2)`, tenantID, "booking-test-authority"); err != nil {
		t.Fatalf("insert test tenant: %v", err)
	}
	bound, err := tenantctx.WithClaims(ctx, tenantctx.Claims{
		Issuer:   "booking-test",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  "booking-test-agent",
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("bind test tenant claims: %v", err)
	}
	return testEnv{store: store, ctx: bound, cleanup: store.Close}
}

func (env testEnv) makeTerminalAndSlot(t *testing.T, capacity int) (string, Slot) {
	t.Helper()
	terminalID := fmt.Sprintf("TERM-%d", time.Now().UnixNano()%1_000_000)
	if err := env.store.CreateTerminal(env.ctx, terminalID, "LAGOS", "Test Terminal", 250000); err != nil {
		t.Fatalf("create terminal: %v", err)
	}
	slot, err := env.store.CreateSlot(env.ctx, terminalID, time.Now().UTC().Add(-time.Hour), time.Now().UTC().Add(2*time.Hour), capacity)
	if err != nil {
		t.Fatalf("create slot: %v", err)
	}
	return terminalID, slot
}

func (env testEnv) makeBooking(t *testing.T, terminalID, requestID string, channel Channel) Booking {
	t.Helper()
	principal := Principal{ID: "test-trucker", Role: "trucker"}
	created, err := env.store.Create(env.ctx, CreateRequest{
		RequestID:     requestID,
		TruckPlate:    "LAG-111-AA",
		TruckerMSISDN: "+2348012345678",
		TerminalID:    terminalID,
		Channel:       channel,
		AmountKobo:    250000,
		ExpiresAt:     time.Now().Add(2 * time.Hour),
	}, principal)
	if err != nil {
		t.Fatalf("create booking: %v", err)
	}
	return created
}

func TestSlotCapacityRaceEnforcesNoOverbooking(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, slot := env.makeTerminalAndSlot(t, 1)
	principal := Principal{ID: "test-trucker", Role: "trucker"}

	// Two trucks race for the last remaining slot capacity.
	first := env.makeBooking(t, terminalID, "req-race-0001", ChannelWeb)
	second := env.makeBooking(t, terminalID, "req-race-0002", ChannelWeb)

	var wait sync.WaitGroup
	outcomes := make([]error, 2)
	reserve := func(index int, bookingID string) {
		defer wait.Done()
		found, err := env.store.Get(env.ctx, bookingID)
		if err != nil {
			outcomes[index] = err
			return
		}
		_, outcomes[index] = env.store.ReserveSlot(env.ctx, bookingID, slot.SlotID, found.Version, principal)
	}
	wait.Add(2)
	go reserve(0, first.BookingID)
	go reserve(1, second.BookingID)
	wait.Wait()

	var wins, conflicts int
	loser := ""
	for index, err := range outcomes {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrSlotUnavailable):
			conflicts++
			loser = []Booking{first, second}[index].BookingID
		default:
			t.Fatalf("unexpected race outcome: %v", err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("capacity race: wins=%d conflicts=%d, want exactly one winner and one capacity rejection", wins, conflicts)
	}

	// The loser must fail closed on retry as well.
	found, err := env.store.Get(env.ctx, loser)
	if err != nil {
		t.Fatalf("reload loser: %v", err)
	}
	if _, err := env.store.ReserveSlot(env.ctx, loser, slot.SlotID, found.Version, principal); !errors.Is(err, ErrSlotUnavailable) {
		t.Fatalf("retry after capacity exhaustion: got %v, want ErrSlotUnavailable", err)
	}
}

func TestOfflineBookingReconciliationFailsClosedOnConflict(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, slot := env.makeTerminalAndSlot(t, 1)
	principal := Principal{ID: "sync-agent", Role: "sync-agent"}

	// An offline booking is accepted into PENDING_SYNC.
	offline := env.makeBooking(t, terminalID, "req-offline-001", ChannelOffline)
	if offline.Status != StatusPendingSync {
		t.Fatalf("offline booking status = %s, want PENDING_SYNC", offline.Status)
	}
	// Meanwhile an online booking takes the only slot.
	online := env.makeBooking(t, terminalID, "req-online-0001", ChannelWeb)
	if _, err := env.store.ReserveSlot(env.ctx, online.BookingID, slot.SlotID, online.Version, principal); err != nil {
		t.Fatalf("online reservation: %v", err)
	}
	// Reconnect: the offline booking conflicts and must surface for
	// reconciliation — never silently dropped or silently booked.
	reconciled, err := env.store.Reconcile(env.ctx, offline.BookingID, slot.SlotID, offline.Version, principal)
	if err != nil {
		t.Fatalf("reconcile conflicted booking: %v", err)
	}
	if reconciled.Status != StatusReconciliationRequired {
		t.Fatalf("reconciled status = %s, want RECONCILIATION_REQUIRED", reconciled.Status)
	}
	if reconciled.ReconciliationReason == nil || *reconciled.ReconciliationReason == "" {
		t.Fatal("reconciliation conflict must carry a recorded reason")
	}
	// PENDING_SYNC may never jump straight to PAID or GATE_APPROVED.
	other := env.makeBooking(t, terminalID, "req-offline-002", ChannelOffline)
	if _, err := env.store.ConfirmPayment(env.ctx, other.BookingID, "rcpt-x", other.Version, principal); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("paying a PENDING_SYNC booking: got %v, want ErrInvalidTransition", err)
	}
}

func TestGateScanControllerValidatesBookingSlotAndPayment(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, slot := env.makeTerminalAndSlot(t, 2)
	principal := Principal{ID: "gate-officer-1", Role: "gate-officer"}
	trucker := Principal{ID: "test-trucker", Role: "trucker"}

	booking := env.makeBooking(t, terminalID, "req-gate-00001", ChannelWeb)
	// Unpaid booking is denied at the gate.
	if _, _, err := env.store.RecordGateScan(env.ctx, booking.BookingID, "GATE-A", "officer-1", time.Now().UTC(), principal); !errors.Is(err, ErrGateDenied) {
		t.Fatalf("gate scan before payment: got %v, want ErrGateDenied", err)
	}
	reserved, err := env.store.ReserveSlot(env.ctx, booking.BookingID, slot.SlotID, booking.Version, trucker)
	if err != nil {
		t.Fatalf("reserve slot: %v", err)
	}
	// Still unpaid after reservation: denied.
	if _, _, err := env.store.RecordGateScan(env.ctx, booking.BookingID, "GATE-A", "officer-1", time.Now().UTC(), principal); !errors.Is(err, ErrGateDenied) {
		t.Fatalf("gate scan of unpaid reserved booking: got %v, want ErrGateDenied", err)
	}
	// Payment intent (idempotent) then confirmation.
	intent, err := env.store.CreatePaymentIntent(env.ctx, booking.BookingID, "pay-req-000001", "tx-ref-0001", reserved.Version)
	if err != nil {
		t.Fatalf("create payment intent: %v", err)
	}
	if _, err := env.store.CreatePaymentIntent(env.ctx, booking.BookingID, "pay-req-000001", "tx-ref-0001", reserved.Version); err != nil {
		t.Fatalf("idempotent payment intent replay: %v", err)
	}
	_ = intent
	paid, err := env.store.ConfirmPayment(env.ctx, booking.BookingID, "rcpt-0001", reserved.Version, trucker)
	if err != nil {
		t.Fatalf("confirm payment: %v", err)
	}
	if paid.Status != StatusPaid {
		t.Fatalf("paid status = %s, want PAID", paid.Status)
	}
	// Scan outside the slot window is denied.
	if _, _, err := env.store.RecordGateScan(env.ctx, booking.BookingID, "GATE-A", "officer-1", slot.EndsAt.Add(2*time.Hour), principal); !errors.Is(err, ErrGateDenied) {
		t.Fatalf("gate scan outside slot window: got %v, want ErrGateDenied", err)
	}
	// In-window scan of a paid booking with receipt is approved.
	scan, approved, err := env.store.RecordGateScan(env.ctx, booking.BookingID, "GATE-A", "officer-1", time.Now().UTC(), principal)
	if err != nil {
		t.Fatalf("gate scan of paid booking: %v", err)
	}
	if scan.Decision != "APPROVED" || approved.Status != StatusGateApproved {
		t.Fatalf("scan decision=%s booking status=%s, want APPROVED/GATE_APPROVED", scan.Decision, approved.Status)
	}
	// Audit commit completes the booking with the ledger hash.
	completed, err := env.store.Complete(env.ctx, booking.BookingID, approved.Version, "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", trucker)
	if err != nil {
		t.Fatalf("complete booking: %v", err)
	}
	if completed.Status != StatusCompleted {
		t.Fatalf("completed status = %s, want COMPLETED", completed.Status)
	}
}

func TestBookingCreateIsIdempotentAndRejectsConflictingReplay(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, _ := env.makeTerminalAndSlot(t, 4)
	principal := Principal{ID: "test-trucker", Role: "trucker"}
	request := CreateRequest{
		RequestID:     "req-idem-000001",
		TruckPlate:    "LAG-999-ZZ",
		TruckerMSISDN: "+2348099999999",
		TerminalID:    terminalID,
		Channel:       ChannelWeb,
		AmountKobo:    250000,
		ExpiresAt:     time.Now().Add(2 * time.Hour),
	}
	first, err := env.store.Create(env.ctx, request, principal)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	second, err := env.store.Create(env.ctx, request, principal)
	if err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	if first.BookingID != second.BookingID {
		t.Fatal("idempotent replay returned a different booking id")
	}
	conflicting := request
	conflicting.AmountKobo = 300000
	if _, err := env.store.Create(env.ctx, conflicting, principal); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting replay: got %v, want ErrIdempotencyConflict", err)
	}
}
