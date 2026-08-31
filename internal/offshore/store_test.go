package offshore

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tariff"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// These tests run against a real PostgreSQL when OFFSHORE_TEST_DATABASE_URL
// is set; they are skipped otherwise. There is no in-memory substitute for
// the row-lock, RLS and outbox atomicity semantics under test.

type testEnv struct {
	store   *Store
	tariffs *tariff.Store
	ctx     context.Context
	cleanup func()
}

func testSigner(t *testing.T) *events.Signer {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}
	signer, err := events.NewSigner(key, "1")
	if err != nil {
		t.Fatalf("build test signer: %v", err)
	}
	return signer
}

func newTestEnv(t *testing.T) testEnv {
	t.Helper()
	databaseURL := os.Getenv("OFFSHORE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("OFFSHORE_TEST_DATABASE_URL is not set; skipping PostgreSQL-backed offshore tests")
	}
	ctx := context.Background()
	signer := testSigner(t)
	store, err := Open(ctx, databaseURL, signer)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if _, err := store.Pool().Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
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
		if _, err := store.Pool().Exec(ctx, string(migration)); err != nil {
			t.Fatalf("apply migration %s: %v", entry, err)
		}
	}
	tenantID := fmt.Sprintf("tenant-offshore-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := store.Pool().Exec(ctx, `INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ($1, $2)`, tenantID, "offshore-test-authority"); err != nil {
		t.Fatalf("insert test tenant: %v", err)
	}
	bound, err := tenantctx.WithClaims(ctx, tenantctx.Claims{
		Issuer:   "offshore-test",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  "offshore-test-agent",
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("bind test tenant claims: %v", err)
	}
	// Dedicated non-superuser role so FORCE ROW LEVEL SECURITY actually binds
	// (the integration superuser bypasses RLS by design; cross-tenant
	// isolation must be observed under a role it constrains).
	if _, err := store.Pool().Exec(ctx, `DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='offshore_rls_tester') THEN CREATE ROLE offshore_rls_tester LOGIN PASSWORD 'offshore-rls-test-only'; END IF; END $$`); err != nil {
		t.Fatalf("create RLS tester role: %v", err)
	}
	for _, grant := range []string{
		`GRANT USAGE ON SCHEMA public TO offshore_rls_tester`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO offshore_rls_tester`,
		`GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO offshore_rls_tester`,
	} {
		if _, err := store.Pool().Exec(ctx, grant); err != nil {
			t.Fatalf("grant RLS tester: %v", err)
		}
	}
	tariffs := tariff.NewStore(store.Pool(), signer)
	return testEnv{store: store, tariffs: tariffs, ctx: bound, cleanup: store.Close}
}

// openRLSStore opens a second pool authenticated as the non-superuser RLS
// tester role against the same database.
func (env testEnv) openRLSStore(t *testing.T, signer *events.Signer) *Store {
	t.Helper()
	databaseURL := os.Getenv("OFFSHORE_TEST_DATABASE_URL")
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse database url: %v", err)
	}
	config.ConnConfig.User = "offshore_rls_tester"
	config.ConnConfig.Password = "offshore-rls-test-only"
	pool, err := pgxpool.NewWithConfig(env.ctx, config)
	if err != nil {
		t.Fatalf("open RLS pool: %v", err)
	}
	if err := pool.Ping(env.ctx); err != nil {
		t.Fatalf("ping RLS pool: %v", err)
	}
	return NewStore(pool, signer)
}

func (env testEnv) registerSchedule(t *testing.T, suffix string, from time.Time) tariff.Schedule {
	t.Helper()
	bandMax := int64(100000)
	schedule := tariff.Schedule{
		ScheduleID:    "npa-offshore-2026." + suffix,
		Domain:        tariff.DomainOffshoreTerminal,
		Name:          "NPA offshore terminal tariff 2026." + suffix,
		Currency:      "USD",
		EffectiveFrom: from,
		LegalAnchor:   "NPA tariff — harbour dues private jetty liquid bulk/SBM",
		RegisteredBy:  "npa-tariff-office",
		Active:        true,
		Rules: []tariff.Rule{
			{ComponentCode: "NPA_HARBOUR_DUES_SBM", Unit: tariff.UnitPerTon, AmountMinor: 56, LegalAnchor: "NPA tariff SBM US$0.56/ton"},
			{ComponentCode: "ENV_PROTECTION_LEVY", Unit: tariff.UnitPerTon, AmountMinor: 10, LegalAnchor: "NPA tariff EPL US$0.10/ton"},
			{ComponentCode: "PILOTAGE_ROYALTY_CALL", Unit: tariff.UnitPerCall, AmountMinor: 150000, LegalAnchor: "NPA compulsory pilotage royalty"},
			{ComponentCode: "SEA_PROTECTION_LEVY", Unit: tariff.UnitPerGTBand, AmountMinor: 125, BandMin: 0, BandMax: &bandMax, LegalAnchor: "Sea Protection Levy Regulations 2012 US$1.25/GT"},
			{ComponentCode: "SEA_PROTECTION_LEVY", Unit: tariff.UnitPerGTBand, AmountMinor: 75, BandMin: 100000, LegalAnchor: "Sea Protection Levy Regulations 2012 US$0.75/GT"},
		},
	}
	if err := env.tariffs.RegisterSchedule(env.ctx, schedule); err != nil {
		t.Fatalf("register schedule: %v", err)
	}
	return schedule
}

func (env testEnv) makeCall(t *testing.T, callID, idemKey string) Call {
	t.Helper()
	// Fixed mooring window: idempotent replays must be byte-identical.
	windowStart := time.Date(2026, 6, 1, 8, 0, 0, 0, time.UTC)
	call, err := env.store.Create(env.ctx, idemKey, CreateRequest{
		CallID:             callID,
		VesselIMO:          "9074729",
		VesselName:         "MT TEST VOYAGER",
		TerminalCode:       "BONNY-SBM-1",
		TerminalKind:       TerminalSBM,
		BuoyID:             "BUOY-07",
		AgencyCode:         "NPA",
		GrossTonnage:       160000,
		CargoTonnes:        300000,
		MooringWindowStart: windowStart,
		MooringWindowEnd:   windowStart.Add(24 * time.Hour),
		NominatedBy:        "terminal-ops-1",
	}, Principal{ID: "terminal-ops-1", Role: "terminal-operator"})
	if err != nil {
		t.Fatalf("create call: %v", err)
	}
	return call
}

func (env testEnv) outboxCount(t *testing.T, topic, eventType string) int {
	t.Helper()
	var count int
	if err := env.store.Pool().QueryRow(env.ctx,
		`SELECT count(*) FROM platform_outbox WHERE topic = $1 AND event_type = $2`, topic, eventType).Scan(&count); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return count
}

func TestOffshoreCallLifecycleAndFeeAssessmentExactlyOnce(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	env.registerSchedule(t, "1", time.Now().UTC().Add(-24*time.Hour))
	principal := Principal{ID: "mooring-ops", Role: "mooring-master-supervisor"}

	call := env.makeCall(t, "OFF-2026-0001", "off-idem-0001")
	if call.Status != StatusNominated || call.Version != 1 {
		t.Fatalf("new call status=%s version=%d, want NOMINATED/1", call.Status, call.Version)
	}

	// Idempotent replay returns the same call; a conflicting replay fails.
	replay := env.makeCall(t, "OFF-2026-0001", "off-idem-0001")
	if replay.CallID != call.CallID {
		t.Fatal("idempotent replay returned a different call")
	}
	conflicting := call.CreateRequest
	conflicting.CargoTonnes = 1
	if _, err := env.store.Create(env.ctx, "off-idem-0001", conflicting, principal); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting replay: got %v, want ErrIdempotencyConflict", err)
	}

	// The mooring-master workflow runs in order; MOORED names the master.
	steps := []struct {
		next   Status
		master string
	}{
		{StatusApproachCleared, ""},
		{StatusMoored, "capt-adebayo"},
		{StatusHoseConnected, ""},
		{StatusLoading, ""},
		{StatusCustodyTransferred, ""},
		{StatusDisconnected, ""},
		{StatusDeparted, ""},
	}
	current := call
	for _, step := range steps {
		updated, err := env.store.Transition(env.ctx, current.CallID, current.Version, step.next, step.master, principal)
		if err != nil {
			t.Fatalf("transition to %s: %v", step.next, err)
		}
		if updated.Status != step.next || updated.Version != current.Version+1 {
			t.Fatalf("transition to %s: status=%s version=%d", step.next, updated.Status, updated.Version)
		}
		current = updated
	}
	if current.MooringMaster == nil || *current.MooringMaster != "capt-adebayo" {
		t.Fatal("mooring master not retained on the call")
	}
	// Terminal states accept no further transitions.
	if _, err := env.store.Transition(env.ctx, current.CallID, current.Version, StatusNominated, "", principal); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("transition out of DEPARTED: got %v, want ErrInvalidTransition", err)
	}
	// Status-change events were emitted once per transition plus nomination.
	if got := env.outboxCount(t, events.TopicOffshore, "offshore.call_status_changed"); got != len(steps) {
		t.Fatalf("status-change outbox events = %d, want %d", got, len(steps))
	}
	if got := env.outboxCount(t, events.TopicOffshore, "offshore.call_nominated"); got != 1 {
		t.Fatalf("nominated outbox events = %d, want 1", got)
	}

	// Fee assessment: 300000t × (56+10) + 150000 flat + 160000 GT × 75.
	asOf := time.Now().UTC()
	assessment, err := env.store.Assess(env.ctx, call.CallID, "off-assess-0001", asOf, principal)
	if err != nil {
		t.Fatalf("assess: %v", err)
	}
	wantTotal := int64(300000*66 + 150000 + 160000*75)
	if assessment.TotalMinor != wantTotal {
		t.Fatalf("assessment total = %d, want %d", assessment.TotalMinor, wantTotal)
	}
	if assessment.Currency != "USD" || assessment.Domain != tariff.DomainOffshoreTerminal {
		t.Fatalf("assessment currency/domain = %s/%s", assessment.Currency, assessment.Domain)
	}
	if got := env.outboxCount(t, events.TopicRevenueAssessments, "revenue.assessment_issued"); got != 1 {
		t.Fatalf("revenue outbox events = %d, want exactly 1", got)
	}

	// Exactly-once: an identical replay returns the retained assessment and
	// emits nothing new.
	replayed, err := env.store.Assess(env.ctx, call.CallID, "off-assess-0001", asOf, principal)
	if err != nil {
		t.Fatalf("assessment replay: %v", err)
	}
	if replayed.AssessmentID != assessment.AssessmentID {
		t.Fatal("assessment replay returned a different assessment id")
	}
	if got := env.outboxCount(t, events.TopicRevenueAssessments, "revenue.assessment_issued"); got != 1 {
		t.Fatalf("revenue outbox events after replay = %d, want exactly 1", got)
	}
	// A conflicting reuse of the key (different assessing principal, hence a
	// different retained record) fails closed.
	other := Principal{ID: "mooring-ops-deputy", Role: "mooring-master-supervisor"}
	if _, err := env.store.Assess(env.ctx, call.CallID, "off-assess-0001", asOf, other); !errors.Is(err, tariff.ErrAssessmentReplay) {
		t.Fatalf("conflicting assessment replay: got %v, want ErrAssessmentReplay", err)
	}
}

func TestOffshoreWorkflowGuards(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	env.registerSchedule(t, "2", time.Now().UTC().Add(-24*time.Hour))
	principal := Principal{ID: "mooring-ops", Role: "mooring-master-supervisor"}
	call := env.makeCall(t, "OFF-2026-0002", "off-idem-0002")

	// Out-of-order transitions fail closed.
	if _, err := env.store.Transition(env.ctx, call.CallID, call.Version, StatusMoored, "capt-x", principal); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("skip to MOORED: got %v, want ErrInvalidTransition", err)
	}
	if _, err := env.store.Transition(env.ctx, call.CallID, call.Version+9, StatusApproachCleared, "", principal); !errors.Is(err, ErrOptimisticConflict) {
		t.Fatalf("stale version: got %v, want ErrOptimisticConflict", err)
	}
	// MOORED without a named mooring master fails closed.
	cleared, err := env.store.Transition(env.ctx, call.CallID, call.Version, StatusApproachCleared, "", principal)
	if err != nil {
		t.Fatalf("clear approach: %v", err)
	}
	if _, err := env.store.Transition(env.ctx, call.CallID, cleared.Version, StatusMoored, "", principal); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("MOORED without master: got %v, want ErrInvalidTransition", err)
	}
	moored, err := env.store.Transition(env.ctx, call.CallID, cleared.Version, StatusMoored, "capt-eze", principal)
	if err != nil {
		t.Fatalf("moor: %v", err)
	}

	// Operational events follow the workflow state.
	opening, closing := "12000.000", "45871.250"
	meterID := "METER-SBM-07-A"
	if _, err := env.store.RecordEvent(env.ctx, call.CallID, EventRequest{
		EventType: EventCustodyMeterReading, RecordedBy: "cargo-surveyor-1",
		MeterID: &meterID, MeterOpeningM3: &opening, MeterClosingM3: &closing,
	}, principal); !errors.Is(err, ErrEventRejected) {
		t.Fatalf("meter reading before LOADING: got %v, want ErrEventRejected", err)
	}
	if _, err := env.store.RecordEvent(env.ctx, call.CallID, EventRequest{
		EventType: EventHoseConnection, RecordedBy: "hose-team-1", Remarks: "16in crude hose connected",
	}, principal); err != nil {
		t.Fatalf("hose connection while MOORED: %v", err)
	}
	// Metering fields on a non-metering event are rejected.
	if _, err := env.store.RecordEvent(env.ctx, call.CallID, EventRequest{
		EventType: EventHoseConnection, RecordedBy: "hose-team-1", MeterID: &meterID,
	}, principal); !errors.Is(err, ErrEventRejected) {
		t.Fatalf("metering fields on hose event: got %v, want ErrEventRejected", err)
	}
	connected, err := env.store.Transition(env.ctx, call.CallID, moored.Version, StatusHoseConnected, "", principal)
	if err != nil {
		t.Fatalf("hose-connected transition: %v", err)
	}
	loading, err := env.store.Transition(env.ctx, call.CallID, connected.Version, StatusLoading, "", principal)
	if err != nil {
		t.Fatalf("loading transition: %v", err)
	}
	if loading.Status != StatusLoading {
		t.Fatalf("status = %s, want LOADING", loading.Status)
	}
	reading, err := env.store.RecordEvent(env.ctx, call.CallID, EventRequest{
		EventType: EventCustodyMeterReading, RecordedBy: "cargo-surveyor-1", Remarks: "closing custody reading",
		MeterID: &meterID, MeterOpeningM3: &opening, MeterClosingM3: &closing,
	}, principal)
	if err != nil {
		t.Fatalf("custody meter reading while LOADING: %v", err)
	}
	if reading.MeterOpeningM3 == nil || *reading.MeterOpeningM3 != "12000.000" {
		t.Fatalf("meter opening = %v", reading.MeterOpeningM3)
	}
	// A closing reading below the opening reading is rejected.
	low := "11999.999"
	if _, err := env.store.RecordEvent(env.ctx, call.CallID, EventRequest{
		EventType: EventCustodyMeterReading, RecordedBy: "cargo-surveyor-1",
		MeterID: &meterID, MeterOpeningM3: &opening, MeterClosingM3: &low,
	}, principal); !errors.Is(err, ErrEventRejected) {
		t.Fatalf("closing below opening: got %v, want ErrEventRejected", err)
	}
	trail, err := env.store.ListEvents(env.ctx, call.CallID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(trail) != 2 {
		t.Fatalf("event trail length = %d, want 2 (nothing silently dropped or duplicated)", len(trail))
	}

	// Assessment before any schedule window fails closed.
	if _, err := env.store.Assess(env.ctx, call.CallID, "off-assess-gap", time.Now().UTC().Add(-72*time.Hour), principal); !errors.Is(err, tariff.ErrNotEffective) {
		t.Fatalf("assessment without effective schedule: got %v, want ErrNotEffective", err)
	}
}

func TestOffshoreTenantIsolation(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	call := env.makeCall(t, "OFF-2026-0003", "off-idem-0003")
	otherTenant := fmt.Sprintf("tenant-offshore-other-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := env.store.Pool().Exec(env.ctx, `INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ($1, $2)`, otherTenant, "offshore-other-authority"); err != nil {
		t.Fatalf("insert other tenant: %v", err)
	}
	bound, err := tenantctx.WithClaims(context.Background(), tenantctx.Claims{
		Issuer: "offshore-test", Audience: "s1-port-interoperability", TenantID: otherTenant,
		Subject: "other-agent", Expires: time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("bind other tenant: %v", err)
	}
	rlsStore := env.openRLSStore(t, testSigner(t))
	defer rlsStore.Close()
	if _, err := rlsStore.Get(bound, call.CallID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant get: got %v, want ErrNotFound (RLS)", err)
	}
}
