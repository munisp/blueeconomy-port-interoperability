package cruise

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

	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/portcall"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tariff"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// These tests run against a real PostgreSQL when CRUISE_TEST_DATABASE_URL
// is set; they are skipped otherwise. The cruise call extends the real
// port-call model, so the fixtures register an agency profile and a port
// call through the production portcall store.

type testEnv struct {
	store    *Store
	portcall *portcall.Store
	tariffs  *tariff.Store
	ctx      context.Context
	cleanup  func()
}

func newTestEnv(t *testing.T) testEnv {
	t.Helper()
	databaseURL := os.Getenv("CRUISE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("CRUISE_TEST_DATABASE_URL is not set; skipping PostgreSQL-backed cruise tests")
	}
	ctx := context.Background()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}
	signer, err := events.NewSigner(key, "1")
	if err != nil {
		t.Fatalf("build test signer: %v", err)
	}
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
	tenantID := fmt.Sprintf("tenant-cruise-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := store.Pool().Exec(ctx, `INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ($1, $2)`, tenantID, "cruise-test-authority"); err != nil {
		t.Fatalf("insert test tenant: %v", err)
	}
	bound, err := tenantctx.WithClaims(ctx, tenantctx.Claims{
		Issuer:   "cruise-test",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  "cruise-test-agent",
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("bind test tenant claims: %v", err)
	}
	return testEnv{
		store:    store,
		portcall: portcall.NewStore(store.Pool()),
		tariffs:  tariff.NewStore(store.Pool(), signer),
		ctx:      bound,
		cleanup:  store.Close,
	}
}

func (env testEnv) principal() Principal {
	return Principal{ID: "cruise-ops-1", Role: "cruise-terminal-operator"}
}

func (env testEnv) makePortCall(t *testing.T, callID string) {
	t.Helper()
	if err := env.portcall.RegisterAgencyProfile(env.ctx, portcall.AgencyProfileRegistration{
		ProfileID: "npa-cruise-profile", Version: "1", AgencyCode: "NPA",
		ProfileSHA256: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RegisteredBy:  "cruise-test-agent", Active: true,
	}); err != nil {
		t.Fatalf("register agency profile: %v", err)
	}
	if _, err := env.portcall.Create(env.ctx, "pc-idem-"+callID, portcall.CreateRequest{
		CallID: callID, VesselIMO: "9234567", PortCode: "LAGOS",
		DeclarationRef: "DECL-2026-0001", SubmittedBy: "cruise-test-agent",
		AgencyProfileID: "npa-cruise-profile", AgencyProfileVersion: "1",
	}); err != nil {
		t.Fatalf("create port call: %v", err)
	}
}

func (env testEnv) registerDuesSchedule(t *testing.T) {
	t.Helper()
	schedule := tariff.Schedule{
		ScheduleID:    "npa-cruise-dues-2026.1",
		Domain:        tariff.DomainCruiseDues,
		Name:          "NPA cruise passenger dues 2026.1",
		Currency:      "USD",
		EffectiveFrom: time.Now().UTC().Add(-24 * time.Hour),
		LegalAnchor:   "NPA tariff — Passenger Charge",
		RegisteredBy:  "npa-tariff-office",
		Active:        true,
		Rules: []tariff.Rule{
			{ComponentCode: "NPA_PASSENGER_CHARGE", Unit: tariff.UnitPerPax, AmountMinor: 1000, LegalAnchor: "NPA tariff Passenger Charge US$10.00/head"},
			{ComponentCode: "CRUISE_TERMINAL_FACILITY", Unit: tariff.UnitPerCall, AmountMinor: 250000, LegalAnchor: "NPA tariff cruise terminal facility charge"},
		},
	}
	if err := env.tariffs.RegisterSchedule(env.ctx, schedule); err != nil {
		t.Fatalf("register dues schedule: %v", err)
	}
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

func TestPaxBandsAreDeterministic(t *testing.T) {
	cases := []struct {
		count int
		band  PaxBand
	}{
		{1, BandSmall}, {499, BandSmall}, {500, BandMedium}, {1499, BandMedium},
		{1500, BandLarge}, {3999, BandLarge}, {4000, BandMega}, {6500, BandMega},
	}
	for _, test := range cases {
		if got := BandFor(test.count); got != test.band {
			t.Fatalf("BandFor(%d) = %s, want %s", test.count, got, test.band)
		}
	}
}

func TestCruiseCallLifecycleAndDuesHook(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	env.makePortCall(t, "PC-2026-0001")
	env.registerDuesSchedule(t)

	call, err := env.store.Create(env.ctx, "crz-idem-0001", CreateRequest{
		CallID: "CRZ-2026-0001", PortCallID: "PC-2026-0001",
		CruiseLine: "West Africa Coastal Cruises", VesselName: "MV CALABAR STAR",
		PaxCount: 2400, CreatedBy: "cruise-ops-1",
	}, env.principal())
	if err != nil {
		t.Fatalf("create cruise call: %v", err)
	}
	if call.Status != StatusPlanned || call.PaxBand != BandLarge {
		t.Fatalf("new call status=%s band=%s, want PLANNED/LARGE", call.Status, call.PaxBand)
	}

	// Idempotent replay and conflicting reuse.
	if _, err := env.store.Create(env.ctx, "crz-idem-0001", call.CreateRequest, env.principal()); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	conflicting := call.CreateRequest
	conflicting.PaxCount = 2401
	if _, err := env.store.Create(env.ctx, "crz-idem-0001", conflicting, env.principal()); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting replay: got %v, want ErrIdempotencyConflict", err)
	}

	// A cruise call cannot extend a missing (or cross-tenant) port call.
	if _, err := env.store.Create(env.ctx, "crz-idem-orphan", CreateRequest{
		CallID: "CRZ-2026-ORPHAN", PortCallID: "PC-DOES-NOT-EXIST",
		CruiseLine: "West Africa Coastal Cruises", VesselName: "MV GHOST",
		PaxCount: 100, CreatedBy: "cruise-ops-1",
	}, env.principal()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("orphan port call: got %v, want ErrNotFound", err)
	}

	// Workflow guards and progression.
	if _, err := env.store.Transition(env.ctx, call.CallID, call.Version, StatusArrived, env.principal()); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("skip to ARRIVED: got %v, want ErrInvalidTransition", err)
	}
	confirmed, err := env.store.Transition(env.ctx, call.CallID, call.Version, StatusConfirmed, env.principal())
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}

	// Excursion manifest, idempotent.
	excursion, err := env.store.AddExcursion(env.ctx, call.CallID, "exc-idem-0001", "Lekki Conservation Tour", "Lekki Tours Ltd", 180, env.principal())
	if err != nil {
		t.Fatalf("add excursion: %v", err)
	}
	if _, err := env.store.AddExcursion(env.ctx, call.CallID, "exc-idem-0001", "Lekki Conservation Tour", "Lekki Tours Ltd", 180, env.principal()); err != nil {
		t.Fatalf("excursion replay: %v", err)
	}
	if _, err := env.store.AddExcursion(env.ctx, call.CallID, "exc-idem-0001", "Different Tour", "Lekki Tours Ltd", 180, env.principal()); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("excursion conflict: got %v, want ErrIdempotencyConflict", err)
	}
	_ = excursion

	// Tender allocation: overlapping windows on the same berth are rejected.
	windowStart := time.Date(2026, 7, 10, 8, 0, 0, 0, time.UTC)
	if _, err := env.store.AllocateTender(env.ctx, call.CallID, "tnd-idem-0001", "LAGOS-CRZ", "TENDER-1", windowStart, windowStart.Add(6*time.Hour), env.principal()); err != nil {
		t.Fatalf("allocate tender: %v", err)
	}
	if _, err := env.store.AllocateTender(env.ctx, call.CallID, "tnd-idem-0002", "LAGOS-CRZ", "TENDER-1", windowStart.Add(3*time.Hour), windowStart.Add(9*time.Hour), env.principal()); err == nil {
		t.Fatal("overlapping tender window on the same berth must be rejected")
	}
	if _, err := env.store.AllocateTender(env.ctx, call.CallID, "tnd-idem-0003", "LAGOS-CRZ", "TENDER-2", windowStart.Add(3*time.Hour), windowStart.Add(9*time.Hour), env.principal()); err != nil {
		t.Fatalf("different berth same window: %v", err)
	}

	// Pre-arrival dues assessment: 2400 pax × US$10.00 + US$2500 flat.
	asOf := time.Now().UTC()
	first, err := env.store.AssessDues(env.ctx, call.CallID, "crz-assess-0001", asOf, env.principal())
	if err != nil {
		t.Fatalf("assess dues: %v", err)
	}
	if want := int64(2400*1000 + 250000); first.TotalMinor != want {
		t.Fatalf("dues total = %d, want %d", first.TotalMinor, want)
	}
	if got := env.outboxCount(t, events.TopicRevenueAssessments, "revenue.assessment_issued"); got != 1 {
		t.Fatalf("revenue outbox = %d, want 1", got)
	}
	// Exactly-once replay.
	replayed, err := env.store.AssessDues(env.ctx, call.CallID, "crz-assess-0001", asOf, env.principal())
	if err != nil || replayed.AssessmentID != first.AssessmentID {
		t.Fatalf("assessment replay: %v %v", replayed.AssessmentID, err)
	}
	if got := env.outboxCount(t, events.TopicRevenueAssessments, "revenue.assessment_issued"); got != 1 {
		t.Fatalf("revenue outbox after replay = %d, want exactly 1", got)
	}

	// Final manifest: pax count updated, dues recomputed as a NEW assessment;
	// the pre-arrival assessment is never mutated.
	arrived, err := env.store.Transition(env.ctx, call.CallID, confirmed.Version, StatusArrived, env.principal())
	if err != nil {
		t.Fatalf("arrive: %v", err)
	}
	updated, err := env.store.UpdatePaxCount(env.ctx, call.CallID, arrived.Version, 2450, env.principal())
	if err != nil {
		t.Fatalf("update pax count: %v", err)
	}
	if updated.PaxBand != BandLarge || updated.PaxCount != 2450 {
		t.Fatalf("updated call pax=%d band=%s", updated.PaxCount, updated.PaxBand)
	}
	second, err := env.store.AssessDues(env.ctx, call.CallID, "crz-assess-0002", asOf, env.principal())
	if err != nil {
		t.Fatalf("re-assess dues: %v", err)
	}
	if want := int64(2450*1000 + 250000); second.TotalMinor != want {
		t.Fatalf("recomputed dues total = %d, want %d", second.TotalMinor, want)
	}
	if got := env.outboxCount(t, events.TopicRevenueAssessments, "revenue.assessment_issued"); got != 2 {
		t.Fatalf("revenue outbox = %d, want 2 immutable assessments", got)
	}
	var assessments int
	if err := env.store.Pool().QueryRow(env.ctx,
		`SELECT count(*) FROM revenue_assessments WHERE domain = 'CRUISE_DUES' AND call_reference = $1`, call.CallID).Scan(&assessments); err != nil {
		t.Fatalf("count assessments: %v", err)
	}
	if assessments != 2 {
		t.Fatalf("retained assessments = %d, want 2", assessments)
	}

	// Completion closes the workflow.
	completed, err := env.store.Transition(env.ctx, call.CallID, updated.Version, StatusCompleted, env.principal())
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if _, err := env.store.UpdatePaxCount(env.ctx, call.CallID, completed.Version, 2400, env.principal()); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("pax update after completion: got %v, want ErrInvalidTransition", err)
	}
	if got := env.outboxCount(t, events.TopicCruise, "cruise.call_status_changed"); got != 3 {
		t.Fatalf("status-change events = %d, want 3", got)
	}
}
