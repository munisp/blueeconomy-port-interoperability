package declarations

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// testSigner builds a throwaway envelope signer for store tests.
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

// These tests run against a real PostgreSQL when DECLARATIONS_TEST_DATABASE_URL
// is set (see scripts/verify-local.sh and docker-compose.integration.yml).
// They are skipped otherwise; there is no in-memory substitute for the RLS,
// unique-index and row-lock semantics under test.

type testEnv struct {
	store   *Store
	signer  *events.Signer
	ctx     context.Context
	cleanup func()
}

func newTestEnv(t *testing.T) testEnv {
	t.Helper()
	databaseURL := os.Getenv("DECLARATIONS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DECLARATIONS_TEST_DATABASE_URL is not set; skipping PostgreSQL-backed declaration tests")
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
	tenantID := fmt.Sprintf("tenant-test-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := store.Pool().Exec(ctx, `INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ($1, $2)`, tenantID, "declarations-test-authority"); err != nil {
		t.Fatalf("insert test tenant: %v", err)
	}
	bound, err := tenantctx.WithClaims(ctx, tenantctx.Claims{
		Issuer:   "declarations-test",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  "declarations-test-trader",
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("bind test tenant claims: %v", err)
	}
	return testEnv{store: store, signer: signer, ctx: bound, cleanup: store.Close}
}

func createRequest(requestID string) CreateRequest {
	return CreateRequest{
		RequestID:            requestID,
		DeclarationRef:       "NCS-2026-ABC123",
		DeclarationType:      TypeImport,
		HSCode:               "8703.24",
		GoodsDescription:     "Used motor vehicles for transport",
		CountryOfOrigin:      "DE",
		PortOfEntry:          "APAPA",
		GrossWeightKg:        12000,
		NetWeightKg:          11500,
		NumberOfPackages:     4,
		ConsigneeID:          "consignee-dangote-01",
		OperatorID:           "operator-apapa-01",
		InvoiceAmountMinor:   500000000,
		FreightAmountMinor:   2500000,
		InsuranceAmountMinor: 500000,
		InvoiceCurrency:      "NGN",
		TariffBPS:            2000,
		VatBPS:               750,
		LevyBPS:              100,
	}
}

// stubScorer is a wired test scorer for lifecycle tests; the fail-closed
// scorer boundary itself is covered in scorer_test.go against real HTTP.
type stubScorer struct {
	response ScoreResponse
	err      error
}

func (scorer stubScorer) Score(context.Context, ScoreRequest) (ScoreResponse, error) {
	return scorer.response, scorer.err
}

func principal() Principal {
	return Principal{ID: "declarations-test-trader", Role: "trader"}
}

func (env testEnv) outboxEvents(t *testing.T) []map[string]any {
	t.Helper()
	rows, err := env.store.Pool().Query(context.Background(), `
		SELECT topic, event_type, payload::text FROM platform_outbox ORDER BY created_at, event_type`)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	defer rows.Close()
	var events []map[string]any
	for rows.Next() {
		var topic, eventType, payload string
		if err := rows.Scan(&topic, &eventType, &payload); err != nil {
			t.Fatalf("scan outbox: %v", err)
		}
		var envelope map[string]any
		if err := json.Unmarshal([]byte(payload), &envelope); err != nil {
			t.Fatalf("decode envelope: %v", err)
		}
		envelope["_topic"] = topic
		envelope["_event_type"] = eventType
		events = append(events, envelope)
	}
	return events
}

func assertEnvelopeV1(t *testing.T, signer *events.Signer, envelope map[string]any, wantEventType string) {
	t.Helper()
	if envelope["_topic"] != "trade.declarations.v1" {
		t.Fatalf("topic = %v, want trade.declarations.v1", envelope["_topic"])
	}
	if envelope["_event_type"] != wantEventType || envelope["eventType"] != wantEventType {
		t.Fatalf("eventType = %v/%v, want %s", envelope["_event_type"], envelope["eventType"], wantEventType)
	}
	if envelope["envelopeVersion"] != "1.0" {
		t.Fatalf("envelopeVersion = %v, want 1.0", envelope["envelopeVersion"])
	}
	if envelope["classification"] != "INTERNAL" {
		t.Fatalf("classification = %v, want INTERNAL", envelope["classification"])
	}
	if _, ok := envelope["fhir"].(map[string]any); !ok {
		t.Fatal("envelope must carry the FHIR bundle under the canonical fhir key")
	}
	provenance, ok := envelope["provenance"].(map[string]any)
	if !ok {
		t.Fatal("envelope must carry provenance")
	}
	signature, ok := provenance["signature"].(string)
	if !ok || len(strings.Split(signature, ".")) != 3 {
		t.Fatalf("provenance.signature must be a JWS compact serialization, got %v", provenance["signature"])
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("re-encode envelope for verification: %v", err)
	}
	var typed events.Envelope
	if err := json.Unmarshal(encoded, &typed); err != nil {
		t.Fatalf("decode typed envelope for verification: %v", err)
	}
	if err := events.Verify(typed, signer.PublicKey()); err != nil {
		t.Fatalf("provenance JWS must verify against the signer public key: %v", err)
	}
	if _, leaked := provenance["signatureSha256"]; leaked {
		t.Fatal("provenance must not emit the legacy signatureSha256 key")
	}
	if provenance["principalId"] == "" || provenance["principalRole"] == "" {
		t.Fatal("provenance principal id and role are required")
	}
}

func TestDeclarationLifecycleClearsGreenLaneWithCertificateAndEvents(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	created, err := env.store.Create(env.ctx, createRequest("req-decl-0001"), principal())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.Status != StatusDraft || created.Revision != 1 || created.Version != 1 {
		t.Fatalf("created = %+v", created)
	}
	if created.HSCode != "870324" {
		t.Fatalf("hs code must be normalized, got %q", created.HSCode)
	}
	// Idempotent replay returns the retained declaration.
	replay, err := env.store.Create(env.ctx, createRequest("req-decl-0001"), principal())
	if err != nil || replay.DeclarationID != created.DeclarationID {
		t.Fatalf("idempotent replay = %+v, %v", replay, err)
	}

	submitted, err := env.store.Submit(env.ctx, created.DeclarationID, created.Version, principal())
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if submitted.Status != StatusSubmitted || submitted.TotalDutyMinor == nil || *submitted.TotalDutyMinor <= 0 {
		t.Fatalf("submitted = %+v", submitted)
	}
	// CIF = 503000000; duty 20% = 100600000; levy 1% = 5030000; VAT base =
	// 608630000, VAT 7.5% = 45647250; total = 151277250.
	if *submitted.TotalDutyMinor != 151277250 {
		t.Fatalf("total duty = %d, want 151277250", *submitted.TotalDutyMinor)
	}

	assessed, err := env.store.AssessRisk(env.ctx, created.DeclarationID, submitted.Version,
		stubScorer{response: ScoreResponse{Score: 12, ModelVersion: "scorer-v1"}}, 0, principal())
	if err != nil {
		t.Fatalf("assess risk: %v", err)
	}
	if assessed.Status != StatusCleared || assessed.RiskLane == nil || *assessed.RiskLane != LaneGreen {
		t.Fatalf("green lane must auto-clear, got %+v", assessed)
	}
	if assessed.RiskScore == nil || *assessed.RiskScore != 12 {
		t.Fatalf("risk score = %v", assessed.RiskScore)
	}

	certificate, declaration, err := env.store.ClearanceCertificate(env.ctx, created.DeclarationID)
	if err != nil {
		t.Fatalf("clearance certificate: %v", err)
	}
	if certificate.CertificateNumber != "CC-NCS-2026-ABC123-R1" || certificate.PayloadSHA256 == "" {
		t.Fatalf("certificate = %+v", certificate)
	}
	if declaration.Status != StatusCleared {
		t.Fatalf("declaration = %+v", declaration)
	}

	events := env.outboxEvents(t)
	if len(events) != 3 {
		t.Fatalf("outbox events = %d, want 3 (submitted, risk-assessed, cleared)", len(events))
	}
	assertEnvelopeV1(t, env.signer, events[0], EventSubmitted)
	assertEnvelopeV1(t, env.signer, events[1], EventCleared)
	assertEnvelopeV1(t, env.signer, events[2], EventRiskAssessed)
}

func TestScorerOutageParksDeclarationScoringUnavailable(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	created, err := env.store.Create(env.ctx, createRequest("req-decl-0002"), principal())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	submitted, err := env.store.Submit(env.ctx, created.DeclarationID, created.Version, principal())
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	assessed, err := env.store.AssessRisk(env.ctx, created.DeclarationID, submitted.Version,
		stubScorer{err: errors.New("scorer unreachable")}, 0, principal())
	if err != nil {
		t.Fatalf("assess risk: %v", err)
	}
	if assessed.Status != StatusScoringUnavailable || assessed.RiskLane != nil {
		t.Fatalf("scorer outage must park the declaration, got %+v", assessed)
	}
	if assessed.ScoringError == nil || *assessed.ScoringError == "" {
		t.Fatal("the scoring error must be recorded for audit")
	}
	if _, _, err := env.store.ClearanceCertificate(env.ctx, created.DeclarationID); !errors.Is(err, ErrNotCleared) {
		t.Fatalf("certificate before clearance = %v, want ErrNotCleared", err)
	}
	// The only escape from SCORING_UNAVAILABLE is an amendment.
	amended, err := env.store.Amend(env.ctx, created.DeclarationID, func() CreateRequest {
		request := createRequest("req-decl-0002-amend")
		request.GrossWeightKg = 12500
		request.NetWeightKg = 12000
		return request
	}(), assessed.Version, principal())
	if err != nil {
		t.Fatalf("amend: %v", err)
	}
	if amended.Status != StatusDraft || amended.Revision != 2 || amended.GrossWeightKg != 12500 {
		t.Fatalf("amended = %+v", amended)
	}
	head, err := env.store.HeadByRef(env.ctx, "NCS-2026-ABC123")
	if err != nil || head.DeclarationID != amended.DeclarationID {
		t.Fatalf("head = %+v, %v", head, err)
	}
	superseded, err := env.store.Get(env.ctx, created.DeclarationID)
	if err != nil || superseded.Status != StatusSuperseded {
		t.Fatalf("superseded = %+v, %v", superseded, err)
	}
	// Idempotent amendment replay returns the retained revision.
	replay, err := env.store.Amend(env.ctx, created.DeclarationID, func() CreateRequest {
		request := createRequest("req-decl-0002-amend")
		request.GrossWeightKg = 12500
		request.NetWeightKg = 12000
		return request
	}(), assessed.Version, principal())
	if err != nil || replay.DeclarationID != amended.DeclarationID {
		t.Fatalf("amendment replay = %+v, %v", replay, err)
	}

	events := env.outboxEvents(t)
	if len(events) != 3 {
		t.Fatalf("outbox events = %d, want 3 (submitted, scoring-unavailable, amended)", len(events))
	}
	assertEnvelopeV1(t, env.signer, events[0], EventSubmitted)
	assertEnvelopeV1(t, env.signer, events[1], EventScoringUnavailable)
	assertEnvelopeV1(t, env.signer, events[2], EventAmended)
}

func TestDeclarationRejectsIllegalTransitions(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	created, err := env.store.Create(env.ctx, createRequest("req-decl-0003"), principal())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Risk assessment requires SUBMITTED.
	if _, err := env.store.AssessRisk(env.ctx, created.DeclarationID, created.Version,
		stubScorer{response: ScoreResponse{Score: 10, ModelVersion: "scorer-v1"}}, 0, principal()); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("assess draft = %v, want ErrInvalidTransition", err)
	}
	// Stale version fails closed.
	if _, err := env.store.Submit(env.ctx, created.DeclarationID, created.Version+1, principal()); !errors.Is(err, ErrOptimisticConflict) {
		t.Fatalf("stale submit = %v, want ErrOptimisticConflict", err)
	}
	submitted, err := env.store.Submit(env.ctx, created.DeclarationID, created.Version, principal())
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	// Double submit is an illegal transition.
	if _, err := env.store.Submit(env.ctx, created.DeclarationID, submitted.Version, principal()); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("double submit = %v, want ErrInvalidTransition", err)
	}
	// Yellow lane does not auto-clear and has no certificate.
	assessed, err := env.store.AssessRisk(env.ctx, created.DeclarationID, submitted.Version,
		stubScorer{response: ScoreResponse{Score: 55, ModelVersion: "scorer-v1"}}, 0, principal())
	if err != nil {
		t.Fatalf("assess risk: %v", err)
	}
	if assessed.Status != StatusYellowLane {
		t.Fatalf("score 55 must lane yellow, got %+v", assessed)
	}
	if _, _, err := env.store.ClearanceCertificate(env.ctx, created.DeclarationID); !errors.Is(err, ErrNotCleared) {
		t.Fatalf("certificate on yellow lane = %v, want ErrNotCleared", err)
	}
	// Lane-assessed declarations are amendable: the amendment supersedes the
	// laned revision and writes a fresh DRAFT that must be re-scored.
	amended, err := env.store.Amend(env.ctx, created.DeclarationID, createRequest("req-decl-0003-amend"), assessed.Version, principal())
	if err != nil {
		t.Fatalf("amend yellow lane: %v", err)
	}
	if amended.Status != StatusDraft || amended.Revision != 2 || amended.SupersedesID == nil || *amended.SupersedesID != created.DeclarationID {
		t.Fatalf("lane amendment must open a superseding DRAFT revision, got %+v", amended)
	}
	head, err := env.store.HeadByRef(env.ctx, amended.DeclarationRef)
	if err != nil || head.DeclarationID != amended.DeclarationID {
		t.Fatalf("amendment revision must head the ref, got %+v, %v", head, err)
	}
	superseded, err := env.store.Get(env.ctx, created.DeclarationID)
	if err != nil || superseded.Status != StatusSuperseded {
		t.Fatalf("laned revision must be SUPERSEDED, got %+v, %v", superseded, err)
	}
	// The cleared terminal remains immutable.
	if _, err := env.store.Amend(env.ctx, created.DeclarationID, createRequest("req-decl-0003-amend-2"), superseded.Version, principal()); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("amend superseded = %v, want ErrInvalidTransition", err)
	}
	// List is scoped to the trader and returns both revisions.
	list, err := env.store.List(env.ctx, "declarations-test-trader", "", 50, 0)
	if err != nil || len(list) != 2 {
		t.Fatalf("list = %+v, %v", list, err)
	}
	drafts, err := env.store.List(env.ctx, "declarations-test-trader", StatusDraft, 50, 0)
	if err != nil || len(drafts) != 1 || drafts[0].DeclarationID != amended.DeclarationID {
		t.Fatalf("status-filtered list = %+v, %v", drafts, err)
	}
	other, err := env.store.List(env.ctx, "another-trader", "", 50, 0)
	if err != nil || len(other) != 0 {
		t.Fatalf("other trader list = %+v, %v", other, err)
	}
}

func TestPermitBlocksSubmitUntilApprovedAndUnexpired(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	created, err := env.store.Create(env.ctx, createRequest("req-decl-0004"), principal())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := env.store.RegisterPermit(env.ctx, created.DeclarationID, "NAFDAC", "National Agency Food", nil, nil); err != nil {
		t.Fatalf("register permit: %v", err)
	}
	if _, err := env.store.Submit(env.ctx, created.DeclarationID, created.Version, principal()); !errors.Is(err, ErrPermitInvalid) {
		t.Fatalf("submit with pending permit = %v, want ErrPermitInvalid", err)
	}
}
