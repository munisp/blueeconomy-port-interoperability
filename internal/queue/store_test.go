package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// These tests run against a real PostgreSQL when QUEUE_TEST_DATABASE_URL (or
// BOOKING_TEST_DATABASE_URL) is set — see scripts/verify-local.sh and
// docker-compose.integration.yml. They are skipped otherwise; there is no
// in-memory substitute for the capacity trigger and row-lock semantics under
// test. A dedicated database keeps this package race-safe against the booking
// package's schema reset when `go test ./...` runs packages in parallel.

type testEnv struct {
	store    *Store
	bookings *booking.Store
	ctx      context.Context
	cleanup  func()
}

func newTestEnv(t *testing.T, graceWindow time.Duration) testEnv {
	t.Helper()
	databaseURL := os.Getenv("QUEUE_TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("BOOKING_TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("QUEUE_TEST_DATABASE_URL is not set; skipping PostgreSQL-backed queue tests")
	}
	ctx := context.Background()
	bookingStore, err := booking.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if _, err := bookingStore.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
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
		if _, err := bookingStore.Exec(ctx, string(migration)); err != nil {
			t.Fatalf("apply migration %s: %v", entry, err)
		}
	}
	tenantID := fmt.Sprintf("tenant-test-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := bookingStore.Pool().Exec(ctx, `INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ($1, $2)`, tenantID, "queue-test-authority"); err != nil {
		t.Fatalf("insert test tenant: %v", err)
	}
	store, err := NewStore(bookingStore.Pool(), bookingStore, graceWindow)
	if err != nil {
		t.Fatalf("build queue store: %v", err)
	}
	bound, err := tenantctx.WithClaims(ctx, tenantctx.Claims{
		Issuer:   "queue-test",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  "queue-test-agent",
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("bind test tenant claims: %v", err)
	}
	return testEnv{store: store, bookings: bookingStore, ctx: bound, cleanup: bookingStore.Close}
}

func (env testEnv) makeTerminal(t *testing.T, queueCapacity int) string {
	t.Helper()
	terminalID := fmt.Sprintf("TERM-%d", time.Now().UnixNano()%1_000_000)
	if err := env.bookings.CreateTerminal(env.ctx, terminalID, "LAGOS", "Test Terminal", 250000); err != nil {
		t.Fatalf("create terminal: %v", err)
	}
	if err := env.store.ConfigureTerminal(env.ctx, terminalID, queueCapacity); err != nil {
		t.Fatalf("set queue capacity: %v", err)
	}
	return terminalID
}

func (env testEnv) makeRequest(t *testing.T, terminalID, key string, class PriorityClass) Request {
	t.Helper()
	created, err := env.store.Create(env.ctx, CreateRequest{
		IdempotencyKey: key,
		TruckPlate:     "LAG-111-AA",
		TruckerMSISDN:  "+2348012345678",
		TerminalID:     terminalID,
		PriorityClass:  class,
	}, booking.ChannelWeb, Principal{ID: "test-trucker", Role: "trucker"})
	if err != nil {
		t.Fatalf("create queue request: %v", err)
	}
	return created
}

func TestQueueCreateAssignsPositionsAndIsIdempotent(t *testing.T) {
	env := newTestEnv(t, DefaultGraceWindow)
	defer env.cleanup()
	terminalID := env.makeTerminal(t, 2)
	request := CreateRequest{
		IdempotencyKey: "queue-idem-0001",
		TruckPlate:     "LAG-999-ZZ",
		TruckerMSISDN:  "+2348099999999",
		TerminalID:     terminalID,
		PriorityClass:  ClassStandard,
	}
	principal := Principal{ID: "test-trucker", Role: "trucker"}
	first, err := env.store.Create(env.ctx, request, booking.ChannelWeb, principal)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if first.Status != StatusQueued || first.Position == nil || *first.Position != 1 {
		t.Fatalf("first request status=%s position=%v, want QUEUED at position 1", first.Status, first.Position)
	}
	// A pending booking was created and linked atomically.
	if first.BookingID == nil {
		t.Fatal("queue request without booking_id must create a pending booking")
	}
	linked, err := env.bookings.Get(env.ctx, *first.BookingID)
	if err != nil {
		t.Fatalf("load pending booking: %v", err)
	}
	if linked.Status != booking.StatusDrafted || linked.AmountKobo != 250000 {
		t.Fatalf("pending booking status=%s amount=%d, want DRAFTED at terminal fee", linked.Status, linked.AmountKobo)
	}
	// Exact replay returns the retained request and reuses the booking.
	second, err := env.store.Create(env.ctx, request, booking.ChannelWeb, principal)
	if err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	if first.QueueRequestID != second.QueueRequestID || *second.BookingID != *first.BookingID {
		t.Fatal("idempotent replay returned a different queue request or booking")
	}
	// A conflicting replay fails closed.
	conflicting := request
	conflicting.PriorityClass = ClassPerishable
	if _, err := env.store.Create(env.ctx, conflicting, booking.ChannelWeb, principal); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting replay: got %v, want ErrIdempotencyConflict", err)
	}
}

func TestQueueCreateReferencesExistingBooking(t *testing.T) {
	env := newTestEnv(t, DefaultGraceWindow)
	defer env.cleanup()
	terminalID := env.makeTerminal(t, 1)
	trucker := Principal{ID: "test-trucker", Role: "trucker"}

	existing, err := env.bookings.Create(env.ctx, booking.CreateRequest{
		RequestID:     "queue-link-00001",
		TruckPlate:    "LAG-555-CC",
		TruckerMSISDN: "+2348012345678",
		TerminalID:    terminalID,
		Channel:       booking.ChannelWeb,
		AmountKobo:    250000,
		ExpiresAt:     time.Now().Add(2 * time.Hour),
	}, trucker)
	if err != nil {
		t.Fatalf("create booking: %v", err)
	}
	linked, err := env.store.Create(env.ctx, CreateRequest{
		IdempotencyKey: "queue-link-00002",
		TruckPlate:     "LAG-555-CC",
		TruckerMSISDN:  "+2348012345678",
		TerminalID:     terminalID,
		PriorityClass:  ClassPriority,
		BookingID:      existing.BookingID,
	}, booking.ChannelWeb, trucker)
	if err != nil {
		t.Fatalf("create linked queue request: %v", err)
	}
	if linked.BookingID == nil || *linked.BookingID != existing.BookingID {
		t.Fatalf("linked booking = %v, want %s", linked.BookingID, existing.BookingID)
	}
	// Referencing an unknown booking fails closed.
	_, err = env.store.Create(env.ctx, CreateRequest{
		IdempotencyKey: "queue-link-00003",
		TruckPlate:     "LAG-555-CC",
		TruckerMSISDN:  "+2348012345678",
		TerminalID:     terminalID,
		PriorityClass:  ClassStandard,
		BookingID:      "00000000-0000-0000-0000-000000000000",
	}, booking.ChannelWeb, trucker)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown booking reference: got %v, want ErrNotFound", err)
	}
}

func TestQueuePositionRaceHasExactlyOneWinnerPerPosition(t *testing.T) {
	env := newTestEnv(t, DefaultGraceWindow)
	defer env.cleanup()
	terminalID := env.makeTerminal(t, 2)
	principal := Principal{ID: "test-trucker", Role: "trucker"}

	const racers = 2
	var wait sync.WaitGroup
	positions := make([]int64, racers)
	outcomes := make([]error, racers)
	race := func(index int, key string) {
		defer wait.Done()
		created, err := env.store.Create(env.ctx, CreateRequest{
			IdempotencyKey: key,
			TruckPlate:     fmt.Sprintf("LAG-%03d-RA", index),
			TruckerMSISDN:  "+2348012345678",
			TerminalID:     terminalID,
			PriorityClass:  ClassStandard,
		}, booking.ChannelWeb, principal)
		if err != nil {
			outcomes[index] = err
			return
		}
		if created.Position == nil {
			outcomes[index] = errors.New("queued request has no position")
			return
		}
		positions[index] = *created.Position
	}
	wait.Add(racers)
	go race(0, "queue-race-0001")
	go race(1, "queue-race-0002")
	wait.Wait()
	for _, err := range outcomes {
		if err != nil {
			t.Fatalf("race outcome: %v", err)
		}
	}
	seen := map[int64]bool{}
	for _, position := range positions {
		if seen[position] {
			t.Fatalf("position %d assigned twice; want exactly one winner per position", position)
		}
		seen[position] = true
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("positions = %v, want {1, 2} each exactly once", positions)
	}
}

func TestCallUpChainRespectsPriorityClassesAndCapacity(t *testing.T) {
	env := newTestEnv(t, DefaultGraceWindow)
	defer env.cleanup()
	terminalID := env.makeTerminal(t, 1)
	engine := Principal{ID: "callup-engine", Role: "callup-engine"}

	standard := env.makeRequest(t, terminalID, "queue-fifo-0001", ClassStandard)
	perishable := env.makeRequest(t, terminalID, "queue-fifo-0002", ClassPerishable)
	priority := env.makeRequest(t, terminalID, "queue-fifo-0003", ClassPriority)

	// The operator view orders by class, then FIFO position within the class.
	entries, err := env.store.ListTerminal(env.ctx, terminalID)
	if err != nil {
		t.Fatalf("list terminal queue: %v", err)
	}
	if len(entries) != 3 || entries[0].QueueRequestID != perishable.QueueRequestID ||
		entries[1].QueueRequestID != priority.QueueRequestID || entries[2].QueueRequestID != standard.QueueRequestID {
		ids := make([]string, 0, len(entries))
		for _, entry := range entries {
			ids = append(ids, entry.QueueRequestID+":"+string(entry.PriorityClass))
		}
		t.Fatalf("queue order = %v, want perishable, priority, standard", ids)
	}

	// Capacity 1: the perishable head is called up first despite joining later.
	promoted, err := env.store.PromoteNext(env.ctx, terminalID, engine)
	if err != nil {
		t.Fatalf("promote head: %v", err)
	}
	if promoted == nil || promoted.QueueRequestID != perishable.QueueRequestID {
		t.Fatalf("promoted = %+v, want the perishable head of queue", promoted)
	}
	if promoted.Status != StatusCalledUp || promoted.GraceDeadline == nil {
		t.Fatalf("promoted status=%s, want CALLED_UP with grace deadline", promoted.Status)
	}
	// No capacity left: nobody else is promoted.
	again, err := env.store.PromoteNext(env.ctx, terminalID, engine)
	if err != nil {
		t.Fatalf("promote beyond capacity: %v", err)
	}
	if again != nil {
		t.Fatalf("promoted beyond capacity: %+v, want nil", again)
	}

	// The arrival frees capacity and chains the next promotion (priority class).
	gate := Principal{ID: "gate-officer-1", Role: "gate-officer"}
	arrived, chained, err := env.store.Arrive(env.ctx, perishable.QueueRequestID, "GATE-A", promoted.Version, gate)
	if err != nil {
		t.Fatalf("arrive: %v", err)
	}
	if arrived.Status != StatusArrived || arrived.ArrivedAt == nil {
		t.Fatalf("arrived status=%s, want ARRIVED", arrived.Status)
	}
	if chained == nil || chained.QueueRequestID != priority.QueueRequestID {
		t.Fatalf("chained promotion = %+v, want the priority request", chained)
	}
}

func TestGraceExpiryForfeitsAndChainsPromotion(t *testing.T) {
	env := newTestEnv(t, time.Minute)
	defer env.cleanup()
	terminalID := env.makeTerminal(t, 1)
	engine := Principal{ID: "callup-engine", Role: "callup-engine"}

	first := env.makeRequest(t, terminalID, "queue-grace-0001", ClassStandard)
	second := env.makeRequest(t, terminalID, "queue-grace-0002", ClassStandard)
	promoted, err := env.store.PromoteNext(env.ctx, terminalID, engine)
	if err != nil || promoted == nil || promoted.QueueRequestID != first.QueueRequestID {
		t.Fatalf("promote head: promoted=%+v err=%v", promoted, err)
	}

	// Grace window elapses: the head is forfeited, audited and the chain
	// promotes the next in queue.
	count, chained, err := env.store.ForfeitExpired(env.ctx, time.Now().UTC().Add(2*time.Minute), engine)
	if err != nil {
		t.Fatalf("forfeit expired: %v", err)
	}
	if count != 1 || len(chained) != 1 || chained[0].QueueRequestID != second.QueueRequestID {
		t.Fatalf("forfeit count=%d chained=%+v, want 1 forfeit chaining the second truck", count, chained)
	}
	forfeited, err := env.store.Get(env.ctx, first.QueueRequestID)
	if err != nil {
		t.Fatalf("reload forfeited: %v", err)
	}
	if forfeited.Status != StatusForfeited || forfeited.ForfeitReason == nil {
		t.Fatalf("status=%s, want FORFEITED with reason", forfeited.Status)
	}

	// A late arrival after forfeiture fails closed.
	_, _, err = env.store.Arrive(env.ctx, first.QueueRequestID, "GATE-A", forfeited.Version, Principal{ID: "gate-officer-1", Role: "gate-officer"})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("arrive after forfeit: got %v, want ErrInvalidTransition", err)
	}
}

func TestArrivalAfterGraceDeadlineFailsClosedAsForfeiture(t *testing.T) {
	env := newTestEnv(t, time.Minute)
	defer env.cleanup()
	terminalID := env.makeTerminal(t, 1)
	engine := Principal{ID: "callup-engine", Role: "callup-engine"}

	request := env.makeRequest(t, terminalID, "queue-late-0001", ClassStandard)
	promoted, err := env.store.PromoteNext(env.ctx, terminalID, engine)
	if err != nil || promoted == nil {
		t.Fatalf("promote: promoted=%+v err=%v", promoted, err)
	}
	// Push the grace deadline into the past, then arrive late.
	err = env.store.withTx(env.ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		_, err := tx.Exec(env.ctx, `UPDATE truck_queue_requests SET grace_deadline=$1 WHERE queue_request_id=$2`, time.Now().UTC().Add(-time.Minute), request.QueueRequestID)
		return err
	})
	if err != nil {
		t.Fatalf("backdate grace deadline: %v", err)
	}
	_, _, err = env.store.Arrive(env.ctx, request.QueueRequestID, "GATE-A", promoted.Version, Principal{ID: "gate-officer-1", Role: "gate-officer"})
	if !errors.Is(err, ErrGraceWindow) {
		t.Fatalf("late arrival: got %v, want ErrGraceWindow", err)
	}
	reloaded, err := env.store.Get(env.ctx, request.QueueRequestID)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.Status != StatusForfeited {
		t.Fatalf("late arrival status=%s, want FORFEITED (fail-closed)", reloaded.Status)
	}
}

func TestQueueEventsCarrySignedEnvelopes(t *testing.T) {
	env := newTestEnv(t, DefaultGraceWindow)
	defer env.cleanup()
	terminalID := env.makeTerminal(t, 1)
	engine := Principal{ID: "callup-engine", Role: "callup-engine"}

	request := env.makeRequest(t, terminalID, "queue-event-0001", ClassStandard)
	if _, err := env.store.PromoteNext(env.ctx, terminalID, engine); err != nil {
		t.Fatalf("promote: %v", err)
	}
	rows, err := env.store.Pool().Query(env.ctx, `SELECT payload FROM platform_outbox WHERE topic=$1 ORDER BY created_at`, events.TopicQueue)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer rows.Close()
	var eventTypes []string
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			t.Fatalf("scan outbox payload: %v", err)
		}
		var envelope events.Envelope
		if err := json.Unmarshal(payload, &envelope); err != nil {
			t.Fatalf("decode envelope: %v", err)
		}
		// The outbox column is JSONB (canonicalized bytes), so the byte-exact
		// bundle signature is asserted on the direct envelope in
		// internal/events; here the persisted metadata must be intact.
		if envelope.Classification != events.ClassificationIntern {
			t.Fatalf("envelope classification = %s, want INTERNAL", envelope.Classification)
		}
		if envelope.Provenance.SignatureSHA256 == "" {
			t.Fatal("envelope provenance must carry the sha256 signature")
		}
		if envelope.Provenance.PrincipalID == "" || envelope.CorrelationID != request.IdempotencyKey {
			t.Fatalf("envelope provenance/correlation = %+v / %s", envelope.Provenance, envelope.CorrelationID)
		}
		eventTypes = append(eventTypes, envelope.EventType)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate outbox: %v", err)
	}
	want := []string{"queue.requested", "queue.position_assigned", "queue.called_up"}
	if len(eventTypes) != len(want) {
		t.Fatalf("outbox events = %v, want %v", eventTypes, want)
	}
	for index := range want {
		if eventTypes[index] != want[index] {
			t.Fatalf("outbox events = %v, want %v", eventTypes, want)
		}
	}
}

func TestCallUpPromotionRaceHasExactlyOneWinner(t *testing.T) {
	env := newTestEnv(t, DefaultGraceWindow)
	defer env.cleanup()
	terminalID := env.makeTerminal(t, 1)
	engine := Principal{ID: "callup-engine", Role: "callup-engine"}

	first := env.makeRequest(t, terminalID, "queue-cup-0001", ClassStandard)
	second := env.makeRequest(t, terminalID, "queue-cup-0002", ClassStandard)

	// Two promoters race to fill the single call-up slot.
	var wait sync.WaitGroup
	promotedIDs := make([]string, 2)
	outcomes := make([]error, 2)
	promote := func(index int) {
		defer wait.Done()
		promoted, err := env.store.PromoteNext(env.ctx, terminalID, engine)
		outcomes[index] = err
		if promoted != nil {
			promotedIDs[index] = promoted.QueueRequestID
		}
	}
	wait.Add(2)
	go promote(0)
	go promote(1)
	wait.Wait()
	for _, err := range outcomes {
		if err != nil {
			t.Fatalf("promotion race outcome: %v", err)
		}
	}
	var winners int
	for _, id := range promotedIDs {
		if id != "" {
			winners++
			if id != first.QueueRequestID {
				t.Fatalf("promoted %s, want the FIFO head %s", id, first.QueueRequestID)
			}
		}
	}
	if winners != 1 {
		t.Fatalf("promotion race winners=%d, want exactly one (capacity 1)", winners)
	}
	// The DB must hold exactly one active call-up and one still-queued truck.
	firstAfter, err := env.store.Get(env.ctx, first.QueueRequestID)
	if err != nil {
		t.Fatalf("reload first: %v", err)
	}
	secondAfter, err := env.store.Get(env.ctx, second.QueueRequestID)
	if err != nil {
		t.Fatalf("reload second: %v", err)
	}
	if firstAfter.Status != StatusCalledUp || secondAfter.Status != StatusQueued {
		t.Fatalf("after race: first=%s second=%s, want CALLED_UP and QUEUED", firstAfter.Status, secondAfter.Status)
	}
}

func TestBookingSlotReleasePromotesQueueHeadInSameTransaction(t *testing.T) {
	env := newTestEnv(t, DefaultGraceWindow)
	defer env.cleanup()
	// The call-up engine hooks into the booking slot release paths.
	env.bookings.SetCapacityListener(env.store)
	engine := Principal{ID: "callup-engine", Role: "callup-engine"}
	trucker := Principal{ID: "test-trucker", Role: "trucker"}
	terminalID := env.makeTerminal(t, 1)

	slot, err := env.bookings.CreateSlot(env.ctx, terminalID, time.Now().UTC().Add(-time.Hour), time.Now().UTC().Add(2*time.Hour), 1)
	if err != nil {
		t.Fatalf("create slot: %v", err)
	}
	// A booking occupies the only slot while a truck waits in the queue.
	occupant, err := env.bookings.Create(env.ctx, booking.CreateRequest{
		RequestID:     "hook-req-000001",
		TruckPlate:    "LAG-222-BB",
		TruckerMSISDN: "+2348012345678",
		TerminalID:    terminalID,
		Channel:       booking.ChannelWeb,
		AmountKobo:    250000,
		ExpiresAt:     time.Now().Add(2 * time.Hour),
	}, trucker)
	if err != nil {
		t.Fatalf("create occupant booking: %v", err)
	}
	if _, err := env.bookings.ReserveSlot(env.ctx, occupant.BookingID, slot.SlotID, occupant.Version, trucker); err != nil {
		t.Fatalf("reserve slot: %v", err)
	}
	queued := env.makeRequest(t, terminalID, "hook-queue-0001", ClassStandard)
	if promoted, err := env.store.PromoteNext(env.ctx, terminalID, engine); err != nil || promoted == nil {
		// Queue capacity is free even while the slot is occupied; the hook is
		// exercised through the release path below. Park the call-up first.
		t.Fatalf("initial promote: promoted=%+v err=%v", promoted, err)
	}
	calledUp, err := env.store.Get(env.ctx, queued.QueueRequestID)
	if err != nil {
		t.Fatalf("reload queued: %v", err)
	}
	if calledUp.Status != StatusCalledUp {
		t.Fatalf("status=%s, want CALLED_UP after first promotion", calledUp.Status)
	}
	// Cancelling the call-up frees capacity; the booking cancellation then
	// releases the slot and — through the in-transaction hook — promotes the
	// next queued truck atomically.
	waiting := env.makeRequest(t, terminalID, "hook-queue-0002", ClassStandard)
	if _, _, err := env.store.Cancel(env.ctx, calledUp.QueueRequestID, calledUp.Version, "truck diverted", engine); err != nil {
		t.Fatalf("cancel call-up: %v", err)
	}
	if _, err := env.bookings.Cancel(env.ctx, occupant.BookingID, occupant.Version+1, "truck diverted", trucker); err != nil {
		t.Fatalf("cancel booking: %v", err)
	}
	promoted, err := env.store.Get(env.ctx, waiting.QueueRequestID)
	if err != nil {
		t.Fatalf("reload waiting: %v", err)
	}
	if promoted.Status != StatusCalledUp || promoted.GraceDeadline == nil {
		t.Fatalf("hook-promoted status=%s, want CALLED_UP with grace deadline", promoted.Status)
	}
}

func TestExpireStaleQueuedRequests(t *testing.T) {
	env := newTestEnv(t, DefaultGraceWindow)
	defer env.cleanup()
	terminalID := env.makeTerminal(t, 1)
	engine := Principal{ID: "callup-engine", Role: "callup-engine"}

	stale := env.makeRequest(t, terminalID, "queue-stale-001", ClassStandard)
	count, err := env.store.ExpireStale(env.ctx, time.Now().UTC().Add(time.Minute), engine)
	if err != nil {
		t.Fatalf("expire stale: %v", err)
	}
	if count != 1 {
		t.Fatalf("expired count=%d, want 1", count)
	}
	reloaded, err := env.store.Get(env.ctx, stale.QueueRequestID)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.Status != StatusExpired {
		t.Fatalf("status=%s, want EXPIRED", reloaded.Status)
	}
	// Terminal state: an expired request can never be called up.
	if _, err := env.store.PromoteNext(env.ctx, terminalID, engine); err != nil {
		t.Fatalf("promote after expiry: %v", err)
	}
}

func TestRequeueFromReconciliationAssignsTailPosition(t *testing.T) {
	env := newTestEnv(t, DefaultGraceWindow)
	defer env.cleanup()
	terminalID := env.makeTerminal(t, 2)
	operator := Principal{ID: "npa-officer-1", Role: "npa-officer"}

	first := env.makeRequest(t, terminalID, "queue-rec-0001", ClassStandard)
	second := env.makeRequest(t, terminalID, "queue-rec-0002", ClassStandard)
	flagged, err := env.store.FlagReconciliation(env.ctx, first.QueueRequestID, first.Version, "booking cancelled elsewhere", operator)
	if err != nil {
		t.Fatalf("flag reconciliation: %v", err)
	}
	if flagged.Status != StatusReconciliationRequired || flagged.ReconciliationReason == nil {
		t.Fatalf("flagged status=%s, want RECONCILIATION_REQUIRED with reason", flagged.Status)
	}
	requeued, err := env.store.Requeue(env.ctx, first.QueueRequestID, flagged.Version, operator)
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if requeued.Status != StatusQueued || requeued.Position == nil || *requeued.Position != 3 {
		t.Fatalf("requeued status=%s position=%v, want QUEUED at tail position 3 (after %v)", requeued.Status, requeued.Position, second.Position)
	}
}
