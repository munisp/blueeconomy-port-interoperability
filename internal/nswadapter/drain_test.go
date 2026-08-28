package nswadapter

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantdb"
)

// These tests run against a real PostgreSQL when NSW_TEST_DATABASE_URL (or
// BOOKING_TEST_DATABASE_URL) is set; they are skipped otherwise.

type drainEnv struct {
	pool     *pgxpool.Pool
	ctx      context.Context
	tenantID string
}

func newDrainEnv(t *testing.T) drainEnv {
	t.Helper()
	databaseURL := os.Getenv("NSW_TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("BOOKING_TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("NSW_TEST_DATABASE_URL is not set; skipping PostgreSQL-backed adapter tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
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
		if _, err := pool.Exec(ctx, string(migration)); err != nil {
			t.Fatalf("apply migration %s: %v", entry, err)
		}
	}
	tenantID := fmt.Sprintf("tenant-nsw-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := pool.Exec(ctx, `INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ($1, $2)`, tenantID, "nsw-adapter-test-authority"); err != nil {
		t.Fatalf("insert test tenant: %v", err)
	}
	return drainEnv{pool: pool, ctx: ctx, tenantID: tenantID}
}

func (env drainEnv) withTenant(t *testing.T, work func(pgx.Tx)) {
	t.Helper()
	bound, err := tenantctx.WithClaims(env.ctx, tenantctx.Claims{
		Issuer:   "nsw-adapter-test",
		Audience: "s1-port-interoperability",
		TenantID: env.tenantID,
		Subject:  "nsw-adapter-test",
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("bind tenant: %v", err)
	}
	if err := tenantdb.WithTx(bound, env.pool, func(tx pgx.Tx, _ tenantctx.Claims) error {
		work(tx)
		return nil
	}); err != nil {
		t.Fatalf("seed tenant data: %v", err)
	}
}

func bookingEnvelope(t *testing.T, eventType, requestID, bookingID string) []byte {
	t.Helper()
	payload := json.RawMessage(`{"booking_id":"` + bookingID + `"}`)
	envelope, err := events.Message(eventType, events.TopicBooking, requestID, bookingID, payload, nil,
		events.Provenance{PrincipalID: "test", PrincipalRole: "test"}, time.Now().UTC())
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	envelopeJSON, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return envelopeJSON
}

// seedNSWEvents inserts one NSW-relevant booking event and one port-call
// clearance decision into the outboxes.
func (env drainEnv) seedNSWEvents(t *testing.T) (bookingEventID, clearanceEventID string) {
	t.Helper()
	bookingEventID = uuid.NewString()
	clearanceEventID = uuid.NewString()
	env.withTenant(t, func(tx pgx.Tx) {
		if _, err := tx.Exec(env.ctx, `
			INSERT INTO platform_outbox (event_id, tenant_id, topic, event_type, idempotency_key, payload, created_at)
			VALUES ($1,$2,'ports.booking.v1','booking.paid',$3,$4,$5)`,
			bookingEventID, env.tenantID, bookingEventID, bookingEnvelope(t, "booking.paid", "req-00001", "booking-0001"), time.Now().UTC()); err != nil {
			t.Fatalf("seed platform outbox: %v", err)
		}
		if _, err := tx.Exec(env.ctx, `
			INSERT INTO port_calls (call_id, vessel_imo, port_code, declaration_reference, submitted_by, status, idempotency_key, created_at, updated_at, version, tenant_id)
			VALUES ('CALL-TEST-0001','1234567','LAGOS','DECL-1','tester','DRAFT','idem-call-0001',$1,$1,1,$2)`,
			time.Now().UTC(), env.tenantID); err != nil {
			t.Fatalf("seed port call: %v", err)
		}
		if _, err := tx.Exec(env.ctx, `
			INSERT INTO port_call_outbox (event_id, call_id, event_type, payload, created_at, tenant_id)
			VALUES ($1,'CALL-TEST-0001','port_call.clearance_decided',$2,$3,$4)`,
			clearanceEventID, `{"decision":"APPROVED","reason":"docs verified"}`, time.Now().UTC(), env.tenantID); err != nil {
			t.Fatalf("seed port-call outbox: %v", err)
		}
	})
	return bookingEventID, clearanceEventID
}

func (env drainEnv) runnerConfig(t *testing.T, fixture nswFixture, contentType string, maxAttempts int) Config {
	t.Helper()
	config := Config{
		EndpointURL:  fixture.server.URL,
		CACertFile:   fixtureCAFile(t, fixture),
		ContentType:  contentType,
		Timeout:      2 * time.Second,
		MaxAttempts:  maxAttempts,
		BackoffBase:  time.Second,
		BackoffMax:   time.Minute,
		PollInterval: time.Second,
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("validate runner config: %v", err)
	}
	return config
}

func fixtureCAFile(t *testing.T, fixture nswFixture) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	block := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: fixture.server.Certificate().Raw})
	if err := os.WriteFile(path, block, 0o600); err != nil {
		t.Fatalf("write fixture CA: %v", err)
	}
	return path
}

func TestDrainDeliversOutboxEventsSignedAndTracksState(t *testing.T) {
	env := newDrainEnv(t)
	bookingEventID, clearanceEventID := env.seedNSWEvents(t)
	fixture := newNSWFixture(t, http.StatusOK)
	config := env.runnerConfig(t, fixture, ContentTypeJSON, 3)
	runner, err := NewRunner(env.pool, fixture.signer, fixture.client, config)
	if err != nil {
		t.Fatalf("build runner: %v", err)
	}
	delivered, err := runner.DrainOnce(context.Background())
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if delivered != 2 {
		t.Fatalf("delivered = %d, want 2 (booking.paid + clearance decision)", delivered)
	}
	seen := map[string]capturedRequest{}
	for i := 0; i < 2; i++ {
		request := <-fixture.capture
		fixture.verifySignature(t, request)
		var probe struct {
			EventType string `json:"eventType"`
		}
		if err := json.Unmarshal(request.body, &probe); err != nil {
			t.Fatalf("decode delivered body: %v", err)
		}
		seen[probe.EventType] = request
	}
	if _, ok := seen["booking.paid"]; !ok {
		t.Fatalf("booking.paid event was not delivered; got %v", seen)
	}
	var statuses map[string]string = map[string]string{}
	rows, err := env.pool.Query(env.ctx, `SELECT event_id::text, status, attempts FROM nsw_delivery`)
	if err != nil {
		t.Fatalf("query deliveries: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventID, status string
		var attempts int
		if err := rows.Scan(&eventID, &status, &attempts); err != nil {
			t.Fatalf("scan delivery: %v", err)
		}
		if status != StatusDelivered || attempts != 1 {
			t.Fatalf("delivery %s status=%s attempts=%d, want DELIVERED/1", eventID, status, attempts)
		}
		statuses[eventID] = status
	}
	if len(statuses) != 2 || statuses[bookingEventID] != StatusDelivered || statuses[clearanceEventID] != StatusDelivered {
		t.Fatalf("delivery ledger = %v, want both events DELIVERED", statuses)
	}
	// Re-drain: delivered events are not re-registered or re-sent.
	delivered, err = runner.DrainOnce(context.Background())
	if err != nil {
		t.Fatalf("re-drain: %v", err)
	}
	if delivered != 0 {
		t.Fatalf("re-drain delivered = %d, want 0 (at-least-once with dedup)", delivered)
	}
}

func TestDrainFailsPermanentlyAfterAttemptBudget(t *testing.T) {
	env := newDrainEnv(t)
	env.seedNSWEvents(t)
	fixture := newNSWFixture(t, http.StatusInternalServerError)
	config := env.runnerConfig(t, fixture, ContentTypeJSON, 1)
	runner, err := NewRunner(env.pool, fixture.signer, fixture.client, config)
	if err != nil {
		t.Fatalf("build runner: %v", err)
	}
	delivered, err := runner.DrainOnce(context.Background())
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if delivered != 0 {
		t.Fatalf("delivered = %d, want 0 against a failing endpoint", delivered)
	}
	var failed int
	if err := env.pool.QueryRow(env.ctx, `SELECT count(*) FROM nsw_delivery WHERE status='FAILED_PERMANENT' AND last_error <> ''`).Scan(&failed); err != nil {
		t.Fatalf("count permanent failures: %v", err)
	}
	if failed != 2 {
		t.Fatalf("FAILED_PERMANENT rows = %d, want 2 — nothing silently dropped", failed)
	}
}

func TestDrainSerializesXMLHandoffWhenNegotiated(t *testing.T) {
	env := newDrainEnv(t)
	_, clearanceEventID := env.seedNSWEvents(t)
	fixture := newNSWFixture(t, http.StatusOK)
	config := env.runnerConfig(t, fixture, ContentTypeXML, 3)
	runner, err := NewRunner(env.pool, fixture.signer, fixture.client, config)
	if err != nil {
		t.Fatalf("build runner: %v", err)
	}
	if _, err := runner.DrainOnce(context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}
	var sawXML bool
	for i := 0; i < 2; i++ {
		request := <-fixture.capture
		fixture.verifySignature(t, request)
		if request.contentType != ContentTypeXML {
			t.Fatalf("content type = %q, want application/xml", request.contentType)
		}
		var document PortCallEvent
		if err := xml.Unmarshal(request.body, &document); err != nil {
			t.Fatalf("delivered body is not a well-formed handoff document: %v", err)
		}
		if document.TenantID != env.tenantID || document.PayloadSHA256 == "" {
			t.Fatalf("document = %#v", document)
		}
		if document.EventID == clearanceEventID {
			sawXML = true
			if document.CallReference != "CALL-TEST-0001" || document.EventType != "port_call.clearance_decided" {
				t.Fatalf("clearance document = %#v", document)
			}
		}
	}
	if !sawXML {
		t.Fatal("clearance decision was not delivered as an XML handoff")
	}
}
