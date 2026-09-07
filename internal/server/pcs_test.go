package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs"
	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs/ais"
	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs/linkage"
	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs/tos"
	"github.com/munisp/blueeconomy-port-interoperability/internal/portcall"
)

// PCS handler tests exercise the /v1/pcs surface through the wired server.
// The NSW anchor lookup needs PostgreSQL, so linkage queries are covered
// by the linkage package unit tests with fakes; here we assert the
// fail-closed and role-gating contracts plus the AIS status honesty.

type fakePortCallLookup struct{}

func (fakePortCallLookup) ListByIMO(context.Context, string, int) ([]portcall.PortCall, error) {
	return nil, nil
}

func pcsRequest(t *testing.T, service *pcs.Service, role, path string) *httptest.ResponseRecorder {
	t.Helper()
	config := testConfig()
	// pgxpool.New parses without connecting; PCS tests never reach the pool.
	pool, err := pgxpool.New(context.Background(), "postgres://127.0.0.1:1/unreachable")
	if err != nil {
		t.Fatalf("build test pool: %v", err)
	}
	t.Cleanup(pool.Close)
	config.Pool = pool
	config.PCS = service
	handler, err := New(config)
	if err != nil {
		t.Fatalf("wire server: %v", err)
	}
	request := loopbackRequest(http.MethodGet, path, "")
	if role != "" {
		request.Header.Set("Authorization", "Bearer "+mintToken(t, "pcs-user", role))
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestPCSStatusReportsUnconfiguredHonestly(t *testing.T) {
	response := pcsRequest(t, &pcs.Service{}, RoleNPAOfficer, "/v1/pcs/status")
	if response.Code != http.StatusOK {
		t.Fatalf("pcs status = %d, want 200 (%s)", response.Code, response.Body.String())
	}
	var body struct {
		AIS     ais.Stats       `json:"ais"`
		TOS     pcs.TOSStatus   `json:"tos"`
		Linkage map[string]bool `json:"linkage"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if body.AIS.Configured || body.TOS.Configured || body.Linkage[linkage.SourceNSW] {
		t.Fatalf("unconfigured service must report configured:false everywhere: %+v", body)
	}
	if body.TOS.Circuit != "unconfigured" {
		t.Fatalf("TOS circuit must be unconfigured, got %q", body.TOS.Circuit)
	}
}

func TestPCSRoutesFailClosedWithoutService(t *testing.T) {
	for _, path := range []string{"/v1/pcs/status", "/v1/pcs/ais/status", "/v1/pcs/ais/positions", "/v1/pcs/tos/berths?port_code=APAPA", "/v1/pcs/portcalls?imo=9074729"} {
		response := pcsRequest(t, nil, RoleNPAOfficer, path)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s = %d, want 503 (%s)", path, response.Code, response.Body.String())
		}
	}
}

func TestPCSRoutesRequireOperationalRole(t *testing.T) {
	service := &pcs.Service{}
	response := pcsRequest(t, service, "", "/v1/pcs/status")
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated = %d, want 401", response.Code)
	}
	response = pcsRequest(t, service, RoleTrader, "/v1/pcs/status")
	if response.Code != http.StatusForbidden {
		t.Fatalf("trader role = %d, want 403", response.Code)
	}
	response = pcsRequest(t, service, RolePortOperatorAdmin, "/v1/pcs/status")
	if response.Code != http.StatusOK {
		t.Fatalf("port-operator-admin = %d, want 200", response.Code)
	}
}

func TestPCSAISPositionsFailClosedWhenFeedUnconfigured(t *testing.T) {
	response := pcsRequest(t, &pcs.Service{}, RoleNPAOfficer, "/v1/pcs/ais/positions")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("positions without feed = %d, want 503", response.Code)
	}
}

func TestPCSAISStatusReflectsIngestion(t *testing.T) {
	ingester, err := ais.NewIngester(nil, ais.NewMemoryStore(), time.Minute)
	if err != nil {
		t.Fatalf("new ingester: %v", err)
	}
	response := pcsRequest(t, &pcs.Service{AIS: ingester}, RoleNPAOfficer, "/v1/pcs/ais/status")
	if response.Code != http.StatusOK {
		t.Fatalf("ais status = %d, want 200", response.Code)
	}
	var stats ais.Stats
	if err := json.Unmarshal(response.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if stats.Configured || stats.MessagesReceived != 0 {
		t.Fatalf("status must honestly show an unconfigured idle feed: %+v", stats)
	}
}

func TestPCSLinkByIMORejectsInvalidIMO(t *testing.T) {
	linker, err := linkage.NewLinker(fakePortCallLookup{}, nil, nil)
	if err != nil {
		t.Fatalf("new linker: %v", err)
	}
	response := pcsRequest(t, &pcs.Service{Linker: linker}, RoleNPAOfficer, "/v1/pcs/portcalls?imo=9074728")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid imo = %d, want 400 (%s)", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "9074729") {
		t.Fatal("error must not echo fabricated vessel data")
	}
}

func TestPCSTOSBerthsFailClosedWhenUnconfigured(t *testing.T) {
	response := pcsRequest(t, &pcs.Service{}, RoleNPAOfficer, "/v1/pcs/tos/berths?port_code=APAPA")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("berths without TOS = %d, want 503", response.Code)
	}
}

var _ = tos.ErrUnconfigured // typed-error mapping is covered in the tos package
