package queue

import (
	"context"
	"expvar"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// These tests run against a real PostgreSQL when QUEUE_TEST_DATABASE_URL (or
// BOOKING_TEST_DATABASE_URL) is set — the same gating as store_test.go. They
// are skipped otherwise; the multi-tenant iteration under test depends on RLS
// and the platform_tenants registry, which have no in-memory substitute.

type sweeperEnv struct {
	pool     *pgxpool.Pool
	bookings *booking.Store
	store    *Store
	ctx      context.Context
}

func newSweeperEnv(t *testing.T) sweeperEnv {
	t.Helper()
	databaseURL := os.Getenv("QUEUE_TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("BOOKING_TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("QUEUE_TEST_DATABASE_URL is not set; skipping PostgreSQL-backed sweeper tests")
	}
	ctx := context.Background()
	bookingStore, err := booking.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(bookingStore.Close)
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
	store, err := NewStore(bookingStore.Pool(), bookingStore, DefaultGraceWindow)
	if err != nil {
		t.Fatalf("build queue store: %v", err)
	}
	return sweeperEnv{pool: bookingStore.Pool(), bookings: bookingStore, store: store, ctx: ctx}
}

// addTenant registers a tenant and returns a context bound to its claims.
func (env sweeperEnv) addTenant(t *testing.T, tenantID string, active bool) context.Context {
	t.Helper()
	if _, err := env.pool.Exec(env.ctx,
		`INSERT INTO platform_tenants (tenant_id, authority_reference, active) VALUES ($1, $2, $3)`,
		tenantID, "sweeper-test-authority", active); err != nil {
		t.Fatalf("insert tenant %s: %v", tenantID, err)
	}
	bound, err := tenantctx.WithClaims(env.ctx, tenantctx.Claims{
		Issuer:   "sweeper-test",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  "sweeper-test-agent",
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("bind tenant %s: %v", tenantID, err)
	}
	return bound
}

// seedQueuedRequest creates a terminal with one call-up slot and one QUEUED
// request for the tenant bound to ctx.
func (env sweeperEnv) seedQueuedRequest(t *testing.T, bound context.Context, key string) Request {
	t.Helper()
	terminalID := fmt.Sprintf("TERM-%d", time.Now().UnixNano()%1_000_000_000)
	if err := env.bookings.CreateTerminal(bound, terminalID, "LAGOS", "Sweeper Test Terminal", 250000); err != nil {
		t.Fatalf("create terminal: %v", err)
	}
	if err := env.store.ConfigureTerminal(bound, terminalID, 1); err != nil {
		t.Fatalf("set queue capacity: %v", err)
	}
	request, err := env.store.Create(bound, CreateRequest{
		IdempotencyKey: key,
		TruckPlate:     "LAG-222-BB",
		TruckerMSISDN:  "+2348012345678",
		TerminalID:     terminalID,
		PriorityClass:  ClassStandard,
	}, booking.ChannelWeb, Principal{ID: "test-trucker", Role: "trucker"})
	if err != nil {
		t.Fatalf("create queue request: %v", err)
	}
	return request
}

func (env sweeperEnv) requestStatus(t *testing.T, tenantID, queueRequestID string) string {
	t.Helper()
	// truck_queue_requests is RLS-enforced, so the read must run inside a
	// tenant-bound transaction exactly like the production store paths; a raw
	// pool read only works for roles that bypass RLS (e.g. superuser).
	tx, err := env.pool.Begin(env.ctx)
	if err != nil {
		t.Fatalf("begin tenant-bound read: %v", err)
	}
	defer tx.Rollback(env.ctx)
	if _, err := tx.Exec(env.ctx, "SELECT set_config('app.tenant_id', $1, true)", tenantID); err != nil {
		t.Fatalf("bind tenant for read: %v", err)
	}
	var status string
	if err := tx.QueryRow(env.ctx,
		`SELECT status FROM truck_queue_requests WHERE tenant_id=$1 AND queue_request_id=$2`,
		tenantID, queueRequestID).Scan(&status); err != nil {
		t.Fatalf("read request status for %s/%s: %v", tenantID, queueRequestID, err)
	}
	return status
}

// recordingCallUps is a test double capturing workflow starts; it never
// touches Temporal.
type recordingCallUps struct {
	mu      sync.Mutex
	started []CallUpWorkflowInput
}

func (recording *recordingCallUps) StartCallUpWorkflow(_ context.Context, input CallUpWorkflowInput) error {
	recording.mu.Lock()
	defer recording.mu.Unlock()
	recording.started = append(recording.started, input)
	return nil
}

func (recording *recordingCallUps) SignalArrivalConfirmed(context.Context, string, string) error {
	return nil
}

func (recording *recordingCallUps) CallUpObserverState(context.Context, string) (CallUpObserverState, error) {
	return CallUpObserverState{}, nil
}

func (recording *recordingCallUps) startedFor(tenantID string) int {
	recording.mu.Lock()
	defer recording.mu.Unlock()
	count := 0
	for _, input := range recording.started {
		if input.TenantID == tenantID {
			count++
		}
	}
	return count
}

func TestNewSweeperFailsClosedOnMissingDependencies(t *testing.T) {
	if _, err := NewSweeper(nil, &Store{}, &recordingCallUps{}, ""); err == nil {
		t.Fatal("sweeper without a database pool must fail closed")
	}
	if _, err := NewSweeper(&pgxpool.Pool{}, nil, &recordingCallUps{}, ""); err == nil {
		t.Fatal("sweeper without a queue store must fail closed")
	}
	if _, err := NewSweeper(&pgxpool.Pool{}, &Store{}, nil, ""); err == nil {
		t.Fatal("sweeper without a call-up orchestrator must fail closed")
	}
}

func TestSweeperSweepsEveryActiveTenant(t *testing.T) {
	env := newSweeperEnv(t)
	tenantA := fmt.Sprintf("tenant-sweep-a-%d", time.Now().UnixNano()%1_000_000)
	tenantB := fmt.Sprintf("tenant-sweep-b-%d", time.Now().UnixNano()%1_000_000)
	tenantOff := fmt.Sprintf("tenant-sweep-off-%d", time.Now().UnixNano()%1_000_000)
	boundA := env.addTenant(t, tenantA, true)
	boundB := env.addTenant(t, tenantB, true)
	boundOff := env.addTenant(t, tenantOff, false)
	requestA := env.seedQueuedRequest(t, boundA, "sweeper-idem-a")
	requestB := env.seedQueuedRequest(t, boundB, "sweeper-idem-b")
	requestOff := env.seedQueuedRequest(t, boundOff, "sweeper-idem-off")

	callUps := &recordingCallUps{}
	sweeper, err := NewSweeper(env.pool, env.store, callUps, "")
	if err != nil {
		t.Fatalf("build sweeper: %v", err)
	}
	if err := sweeper.SweepOnce(env.ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	// Both active tenants were reconciled: the queue head was promoted into
	// the free call-up slot and a grace-window workflow was started.
	if status := env.requestStatus(t, tenantA, requestA.QueueRequestID); status != string(StatusCalledUp) {
		t.Fatalf("tenant A request status = %s, want CALLED_UP", status)
	}
	if status := env.requestStatus(t, tenantB, requestB.QueueRequestID); status != string(StatusCalledUp) {
		t.Fatalf("tenant B request status = %s, want CALLED_UP", status)
	}
	if callUps.startedFor(tenantA) != 1 || callUps.startedFor(tenantB) != 1 {
		t.Fatalf("workflow starts = %+v, want one per active tenant", callUps.started)
	}
	// The inactive tenant is outside the sweep.
	if status := env.requestStatus(t, tenantOff, requestOff.QueueRequestID); status != string(StatusQueued) {
		t.Fatalf("inactive tenant request status = %s, want QUEUED (not swept)", status)
	}
}

func TestSweeperHonoursSingleTenantRestriction(t *testing.T) {
	env := newSweeperEnv(t)
	tenantA := fmt.Sprintf("tenant-only-a-%d", time.Now().UnixNano()%1_000_000)
	tenantB := fmt.Sprintf("tenant-only-b-%d", time.Now().UnixNano()%1_000_000)
	boundA := env.addTenant(t, tenantA, true)
	boundB := env.addTenant(t, tenantB, true)
	requestA := env.seedQueuedRequest(t, boundA, "sweeper-only-a")
	requestB := env.seedQueuedRequest(t, boundB, "sweeper-only-b")

	callUps := &recordingCallUps{}
	sweeper, err := NewSweeper(env.pool, env.store, callUps, tenantB)
	if err != nil {
		t.Fatalf("build sweeper: %v", err)
	}
	if err := sweeper.SweepOnce(env.ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if status := env.requestStatus(t, tenantB, requestB.QueueRequestID); status != string(StatusCalledUp) {
		t.Fatalf("restricted tenant request status = %s, want CALLED_UP", status)
	}
	if status := env.requestStatus(t, tenantA, requestA.QueueRequestID); status != string(StatusQueued) {
		t.Fatalf("other tenant request status = %s, want QUEUED (restriction honoured)", status)
	}
	if callUps.startedFor(tenantB) != 1 || callUps.startedFor(tenantA) != 0 {
		t.Fatalf("workflow starts = %+v, want exactly one for the restricted tenant", callUps.started)
	}
}

func TestSweeperIsolatesTenantFailures(t *testing.T) {
	env := newSweeperEnv(t)
	// "tenant-bad" satisfies the platform_tenants CHECK constraint but is
	// rejected by tenantctx claim validation, so its sweep fails while the
	// healthy tenant must still be reconciled.
	broken := "tenant-bad"
	healthy := fmt.Sprintf("tenant-sweep-ok-%d", time.Now().UnixNano()%1_000_000)
	boundHealthy := env.addTenant(t, healthy, true)
	if _, err := env.pool.Exec(env.ctx,
		`INSERT INTO platform_tenants (tenant_id, authority_reference, active) VALUES ($1, $2, true)`,
		broken, "sweeper-test-authority"); err != nil {
		t.Fatalf("insert broken tenant: %v", err)
	}
	request := env.seedQueuedRequest(t, boundHealthy, "sweeper-isolation-ok")

	before := failureCount(broken)
	callUps := &recordingCallUps{}
	sweeper, err := NewSweeper(env.pool, env.store, callUps, "")
	if err != nil {
		t.Fatalf("build sweeper: %v", err)
	}
	err = sweeper.SweepOnce(env.ctx)
	if err == nil || !strings.Contains(err.Error(), broken) {
		t.Fatalf("sweep error = %v, want an error naming the failing tenant %s", err, broken)
	}
	// Failure isolation: the healthy tenant was still swept.
	if status := env.requestStatus(t, healthy, request.QueueRequestID); status != string(StatusCalledUp) {
		t.Fatalf("healthy tenant request status = %s, want CALLED_UP despite the failing tenant", status)
	}
	if callUps.startedFor(healthy) != 1 {
		t.Fatalf("workflow starts = %+v, want one for the healthy tenant", callUps.started)
	}
	if got := failureCount(broken); got != before+1 {
		t.Fatalf("failure metric for %s = %d, want %d", broken, got, before+1)
	}
}

func failureCount(tenantID string) int64 {
	value, ok := sweepTenantFailures.Get(tenantID).(*expvar.Int)
	if !ok || value == nil {
		return 0
	}
	return value.Value()
}
