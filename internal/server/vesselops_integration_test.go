package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/cruise"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/manifests"
	"github.com/munisp/blueeconomy-port-interoperability/internal/offshore"
	"github.com/munisp/blueeconomy-port-interoperability/internal/portcall"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tariff"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// End-to-end HTTP coverage of the W-FEAT-6 vessel-operations surface against
// real stores on PostgreSQL. Gated on OFFSHORE_TEST_DATABASE_URL; skipped
// otherwise.

type vesselOpsEnv struct {
	handler        http.Handler
	pool           *pgxpool.Pool
	ctx            context.Context
	tenantID       string
	authoritySign  *events.Signer
	envelopeSigner *events.Signer
}

func newVesselOpsEnv(t *testing.T) vesselOpsEnv {
	t.Helper()
	databaseURL := os.Getenv("OFFSHORE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("OFFSHORE_TEST_DATABASE_URL is not set; skipping PostgreSQL-backed vessel-ops server tests")
	}
	ctx := context.Background()
	signer := mustSigner()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	entries, err := filepath.Glob(filepath.Join("..", "..", "db", "migrations", "*.sql"))
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
	tenantID := fmt.Sprintf("tenant-vesselops-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := pool.Exec(ctx, `INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ($1, $2)`, tenantID, "vesselops-test-authority"); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	bound, err := tenantctx.WithClaims(ctx, tenantctx.Claims{
		Issuer: "vesselops-test", Audience: "s1-port-interoperability", TenantID: tenantID,
		Subject: "vesselops-tester", Expires: time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("bind claims: %v", err)
	}
	authorityPublic, authorityPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate authority key: %v", err)
	}
	authoritySigner, err := events.NewSignerWithKeyID(authorityPrivate, "manifest-authority-1")
	if err != nil {
		t.Fatalf("build authority signer: %v", err)
	}
	manifestStore, err := manifests.NewStore(pool, signer, authorityPublic, "manifest-authority-")
	if err != nil {
		t.Fatalf("build manifest store: %v", err)
	}
	config := testConfig()
	config.Store = portcall.NewStore(pool)
	config.Offshore = offshore.NewStore(pool, signer)
	config.Cruise = cruise.NewStore(pool, signer)
	config.Manifests = manifestStore
	config.Tariffs = tariff.NewStore(pool, signer)
	config.Pool = pool
	config.TenantGateway = tenantctx.Verifier{Key: gatewayTestKey, Issuer: "gateway.blueeconomy.ng", Audience: "s1-port-interoperability"}
	handler, err := New(config)
	if err != nil {
		t.Fatalf("wire server: %v", err)
	}
	return vesselOpsEnv{handler: handler, pool: pool, ctx: bound, tenantID: tenantID, authoritySign: authoritySigner, envelopeSigner: signer}
}

func (env vesselOpsEnv) call(t *testing.T, method, path, token, body string, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	request := loopbackRequest(method, path, body)
	request.Header.Set("Authorization", "Bearer "+token)
	for index := 0; index+1 < len(headers); index += 2 {
		request.Header.Set(headers[index], headers[index+1])
	}
	response := httptest.NewRecorder()
	env.handler.ServeHTTP(response, request)
	return response
}

func TestVesselOpsHTTPSurface(t *testing.T) {
	env := newVesselOpsEnv(t)
	npaToken := gatewayToken(t, env.tenantID, "npa-officer-1", RoleNPAOfficer)
	adminToken := gatewayToken(t, env.tenantID, "ops-admin-1", RolePortOperatorAdmin)
	authorityToken := gatewayToken(t, env.tenantID, "nsw-adapter-1", RoleManifestAuthority)
	plainToken := gatewayToken(t, env.tenantID, "plain-user")

	// Role gates fail closed for unprivileged tokens.
	if response := env.call(t, http.MethodPost, "/v1/offshore-calls", plainToken, `{}`); response.Code != http.StatusForbidden {
		t.Fatalf("offshore create without role = %d, want 403", response.Code)
	}
	if response := env.call(t, http.MethodPost, "/v1/tariff-schedules", npaToken, `{}`); response.Code != http.StatusForbidden {
		t.Fatalf("tariff registration by npa-officer = %d, want 403", response.Code)
	}

	// Tariff schedule registration (offshore + cruise domains).
	offshoreSchedule := `{
		"schedule_id":"npa-offshore-2026.http","domain":"OFFSHORE_TERMINAL","name":"NPA offshore 2026 http",
		"currency":"USD","effective_from":"2026-01-01T00:00:00Z",
		"legal_anchor":"NPA tariff SBM","registered_by":"placeholder","active":true,
		"rules":[
			{"component_code":"NPA_HARBOUR_DUES_SBM","unit":"PER_TON","amount_minor":56,"legal_anchor":"NPA tariff SBM US$0.56/ton"},
			{"component_code":"PILOTAGE_ROYALTY_CALL","unit":"PER_CALL","amount_minor":150000,"legal_anchor":"NPA pilotage royalty"}
		]}`
	if response := env.call(t, http.MethodPost, "/v1/tariff-schedules", adminToken, offshoreSchedule); response.Code != http.StatusCreated {
		t.Fatalf("register offshore schedule = %d: %s", response.Code, response.Body.String())
	}
	cruiseSchedule := `{
		"schedule_id":"npa-cruise-2026.http","domain":"CRUISE_DUES","name":"NPA cruise 2026 http",
		"currency":"USD","effective_from":"2026-01-01T00:00:00Z",
		"legal_anchor":"NPA tariff Passenger Charge","registered_by":"placeholder","active":true,
		"rules":[{"component_code":"NPA_PASSENGER_CHARGE","unit":"PER_PAX","amount_minor":1000,"legal_anchor":"NPA US$10.00/head"}]}`
	if response := env.call(t, http.MethodPost, "/v1/tariff-schedules", adminToken, cruiseSchedule); response.Code != http.StatusCreated {
		t.Fatalf("register cruise schedule = %d: %s", response.Code, response.Body.String())
	}

	// Offshore call lifecycle over HTTP.
	createBody := `{
		"call_id":"OFF-HTTP-0001","vessel_imo":"9074729","vessel_name":"MT HTTP VOYAGER",
		"terminal_code":"BONNY-SBM-1","terminal_kind":"SBM","buoy_id":"BUOY-07","agency_code":"NPA",
		"gross_tonnage":160000,"cargo_tonnes":300000,
		"mooring_window_start":"2026-06-01T08:00:00Z","mooring_window_end":"2026-06-02T08:00:00Z",
		"nominated_by":"npa-officer-1"}`
	response := env.call(t, http.MethodPost, "/v1/offshore-calls", npaToken, createBody, "Idempotency-Key", "http-off-0001")
	if response.Code != http.StatusCreated {
		t.Fatalf("create offshore call = %d: %s", response.Code, response.Body.String())
	}
	var call offshore.Call
	if err := json.Unmarshal(response.Body.Bytes(), &call); err != nil {
		t.Fatalf("decode call: %v", err)
	}
	// Idempotent replay through the API.
	if response := env.call(t, http.MethodPost, "/v1/offshore-calls", npaToken, createBody, "Idempotency-Key", "http-off-0001"); response.Code != http.StatusCreated {
		t.Fatalf("replay create = %d", response.Code)
	}
	transition := func(version int64, next, master string) offshore.Call {
		body := fmt.Sprintf(`{"expected_version":%d,"next":%q,"mooring_master":%q}`, version, next, master)
		response := env.call(t, http.MethodPost, "/v1/offshore-calls/OFF-HTTP-0001/transitions", npaToken, body)
		if response.Code != http.StatusOK {
			t.Fatalf("transition to %s = %d: %s", next, response.Code, response.Body.String())
		}
		var updated offshore.Call
		if err := json.Unmarshal(response.Body.Bytes(), &updated); err != nil {
			t.Fatalf("decode transition: %v", err)
		}
		return updated
	}
	cleared := transition(call.Version, "APPROACH_CLEARED", "")
	moored := transition(cleared.Version, "MOORED", "capt-http")

	// Assessment over HTTP: 300000t × 56 + 150000.
	assessBody := fmt.Sprintf(`{"idempotency_key":"http-assess-0001","as_of":%q}`, time.Now().UTC().Format(time.RFC3339Nano))
	response = env.call(t, http.MethodPost, "/v1/offshore-calls/OFF-HTTP-0001/assessments", npaToken, assessBody)
	if response.Code != http.StatusCreated {
		t.Fatalf("assess = %d: %s", response.Code, response.Body.String())
	}
	var assessment tariff.Assessment
	if err := json.Unmarshal(response.Body.Bytes(), &assessment); err != nil {
		t.Fatalf("decode assessment: %v", err)
	}
	if want := int64(300000*56 + 150000); assessment.TotalMinor != want {
		t.Fatalf("assessment total = %d, want %d", assessment.TotalMinor, want)
	}
	// Replay through the API returns the retained assessment (exactly once).
	response = env.call(t, http.MethodPost, "/v1/offshore-calls/OFF-HTTP-0001/assessments", npaToken, assessBody)
	var replayed tariff.Assessment
	if err := json.Unmarshal(response.Body.Bytes(), &replayed); err != nil || replayed.AssessmentID != assessment.AssessmentID {
		t.Fatalf("assessment replay = %d %v", response.Code, err)
	}

	// API/BRI manifest ingest over HTTP with an authority-signed artifact.
	payload := manifests.Payload{
		ManifestReference: "MNF-HTTP-00001", VoyageReference: "VOY-2026-0999",
		CallReference: "CRZ-HTTP-0001", ManifestKind: "CRUISE", VesselIMO: "9234567",
		Records: []manifests.Record{
			{RecordType: "PAX", FamilyName: "Okafor", GivenName: "Adaeze", DateOfBirth: "1988-04-12", Nationality: "NGA", DocumentNumber: "A50123456", Sex: "F"},
			{RecordType: "PAX", FamilyName: "BadDoc", GivenName: "Case", DateOfBirth: "1988-04-12", Nationality: "NGA", DocumentNumber: "x"},
		},
	}
	payloadJSON, _ := json.Marshal(payload)
	envelope, err := events.Message(manifests.EventTypeSubmit, events.TopicManifests,
		payload.ManifestReference, payload.ManifestReference, payloadJSON, nil,
		events.Provenance{PrincipalID: "cruise-line-apidata", PrincipalRole: "manifest-authority"}, time.Now().UTC(), env.authoritySign)
	if err != nil {
		t.Fatalf("sign manifest: %v", err)
	}
	artifact, _ := json.Marshal(envelope)
	response = env.call(t, http.MethodPost, "/v1/manifests", authorityToken, string(artifact))
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("ingest with one bad record = %d, want 422 (accepted-with-rejections): %s", response.Code, response.Body.String())
	}
	var manifest manifests.Manifest
	if err := json.Unmarshal(response.Body.Bytes(), &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.Status != manifests.StatusAcceptedWithRejections || manifest.RecordsAccepted != 1 || manifest.RecordsRejected != 1 {
		t.Fatalf("manifest = %+v", manifest)
	}
	response = env.call(t, http.MethodGet, "/v1/manifest-rejections?manifest_id="+manifest.ManifestID, authorityToken, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), manifests.ReasonInvalidDocumentNumber) {
		t.Fatalf("rejections = %d: %s", response.Code, response.Body.String())
	}
	// The manifest-authority role is mandatory for ingest.
	if response := env.call(t, http.MethodPost, "/v1/manifests", npaToken, string(artifact)); response.Code != http.StatusForbidden {
		t.Fatalf("ingest by npa-officer = %d, want 403", response.Code)
	}

	// Cruise call over HTTP, extending a real port call prepared via the store.
	if err := portcall.NewStore(env.pool).RegisterAgencyProfile(env.ctx, portcall.AgencyProfileRegistration{
		ProfileID: "npa-cruise-profile", Version: "1", AgencyCode: "NPA",
		ProfileSHA256: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RegisteredBy:  "vesselops-tester", Active: true,
	}); err != nil {
		t.Fatalf("register agency profile: %v", err)
	}
	if _, err := portcall.NewStore(env.pool).Create(env.ctx, "pc-http-0001", portcall.CreateRequest{
		CallID: "PC-HTTP-0001", VesselIMO: "9234567", PortCode: "LAGOS",
		DeclarationRef: "DECL-2026-0001", SubmittedBy: "vesselops-tester",
		AgencyProfileID: "npa-cruise-profile", AgencyProfileVersion: "1",
	}); err != nil {
		t.Fatalf("create port call: %v", err)
	}
	cruiseBody := `{"call_id":"CRZ-HTTP-0001","port_call_id":"PC-HTTP-0001","cruise_line":"West Africa Coastal Cruises","vessel_name":"MV CALABAR STAR","pax_count":800,"created_by":"placeholder"}`
	response = env.call(t, http.MethodPost, "/v1/cruise-calls", npaToken, cruiseBody, "Idempotency-Key", "http-crz-0001")
	if response.Code != http.StatusCreated {
		t.Fatalf("create cruise call = %d: %s", response.Code, response.Body.String())
	}
	var cruiseCall cruise.Call
	if err := json.Unmarshal(response.Body.Bytes(), &cruiseCall); err != nil {
		t.Fatalf("decode cruise call: %v", err)
	}
	if cruiseCall.PaxBand != cruise.BandMedium || cruiseCall.CreatedBy != "npa-officer-1" {
		t.Fatalf("cruise call band=%s created_by=%s (server must pin registrar)", cruiseCall.PaxBand, cruiseCall.CreatedBy)
	}
	duesBody := fmt.Sprintf(`{"idempotency_key":"http-dues-0001","as_of":%q}`, time.Now().UTC().Format(time.RFC3339Nano))
	response = env.call(t, http.MethodPost, "/v1/cruise-calls/CRZ-HTTP-0001/dues-assessments", npaToken, duesBody)
	if response.Code != http.StatusCreated {
		t.Fatalf("assess dues = %d: %s", response.Code, response.Body.String())
	}
	var dues tariff.Assessment
	if err := json.Unmarshal(response.Body.Bytes(), &dues); err != nil {
		t.Fatalf("decode dues: %v", err)
	}
	if want := int64(800 * 1000); dues.TotalMinor != want {
		t.Fatalf("dues total = %d, want %d (800 pax × US$10.00)", dues.TotalMinor, want)
	}
	_ = moored
}
