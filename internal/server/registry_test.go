package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/registry"
	"github.com/stretchr/testify/require"
)

// fakeRegistry implements RegistryStore without a database; handler
// behaviour (roles, idempotency, validation, error mapping) is covered here
// and storage semantics are covered by the real-PG registry store tests.
type fakeRegistry struct {
	vessel       registry.Vessel
	registerErr  error
	verify       registry.Verification
	verifyErr    error
	permit       registry.CabotagePermit
	eligibility  registry.Eligibility
	permitErr    error
	decideErr    error
	transitionEr error
}

func (fake fakeRegistry) Register(context.Context, string, registry.RegisterVesselRequest, registry.Principal) (registry.Vessel, error) {
	return fake.vessel, fake.registerErr
}
func (fake fakeRegistry) Get(_ context.Context, vesselID string) (registry.Vessel, error) {
	if fake.registerErr != nil {
		return registry.Vessel{}, fake.registerErr
	}
	return registry.Vessel{VesselID: vesselID, Status: registry.VesselApplication}, nil
}
func (fake fakeRegistry) List(context.Context, registry.VesselStatus, int) ([]registry.Vessel, error) {
	return []registry.Vessel{}, nil
}
func (fake fakeRegistry) OwnershipHistory(context.Context, string) ([]registry.OwnershipEntry, error) {
	return []registry.OwnershipEntry{}, nil
}
func (fake fakeRegistry) Transition(context.Context, string, string, registry.VesselStatus, string, registry.Principal) (registry.Vessel, error) {
	return registry.Vessel{}, fake.transitionEr
}
func (fake fakeRegistry) TransferOwnership(context.Context, string, string, string, string, time.Time, registry.Principal) (registry.OwnershipEntry, error) {
	return registry.OwnershipEntry{}, nil
}
func (fake fakeRegistry) RegisterSeafarer(context.Context, string, registry.RegisterSeafarerRequest, registry.Principal) (registry.Seafarer, error) {
	return registry.Seafarer{Status: registry.SeafarerActive}, nil
}
func (fake fakeRegistry) IssueCertificate(context.Context, string, registry.IssueCertificateRequest, registry.Principal) (registry.Certificate, error) {
	return registry.Certificate{Status: registry.CertificateActive}, nil
}
func (fake fakeRegistry) TransitionCertificate(context.Context, string, string, registry.CertificateStatus, registry.Principal) (registry.Certificate, error) {
	return registry.Certificate{}, nil
}
func (fake fakeRegistry) VerifyCertificate(context.Context, string, string) (registry.Verification, error) {
	return fake.verify, fake.verifyErr
}
func (fake fakeRegistry) UpsertCabotageRule(context.Context, string, registry.CabotageRule, registry.Principal) (registry.CabotageRule, error) {
	return registry.CabotageRule{Status: "ACTIVE"}, nil
}
func (fake fakeRegistry) ApplyPermit(context.Context, string, registry.ApplyPermitRequest, registry.Principal) (registry.CabotagePermit, registry.Eligibility, error) {
	return fake.permit, fake.eligibility, fake.permitErr
}
func (fake fakeRegistry) DecidePermit(context.Context, string, string, bool, registry.Principal) (registry.CabotagePermit, error) {
	return registry.CabotagePermit{}, fake.decideErr
}
func (fake fakeRegistry) GetPermit(context.Context, string) (registry.CabotagePermit, error) {
	return registry.CabotagePermit{}, nil
}
func (fake fakeRegistry) FlagViolation(context.Context, string, registry.Violation, registry.Principal) (registry.Violation, error) {
	return registry.Violation{Status: "OPEN"}, nil
}
func (fake fakeRegistry) ResolveViolation(context.Context, string, string, registry.Principal) (registry.Violation, error) {
	return registry.Violation{Status: "RESOLVED"}, nil
}

// registryTestHandler wires the shared test handler with the registry seam
// overridden, matching the newWiredHandler pattern (unreachable pool; these
// handler tests never reach the database).
func registryTestHandler(t *testing.T, fake RegistryStore) http.Handler {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://127.0.0.1:1/unreachable")
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	config := testConfig()
	config.Pool = pool
	config.Registry = fake
	handler, err := New(config)
	require.NoError(t, err)
	return handler
}

func authedRequest(t *testing.T, method, path, body string, roles ...string) *http.Request {
	t.Helper()
	request := loopbackRequest(method, path, body)
	request.Header.Set("Idempotency-Key", "test-idem-"+method+"-"+path)
	request.Header.Set("Authorization", "Bearer "+mintToken(t, "registry-tester", roles...))
	return request
}

func TestRegisterVesselRequiresRole(t *testing.T) {
	handler := registryTestHandler(t, fakeRegistry{})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authedRequest(t, http.MethodPost, "/v1/registry/vessels", `{}`, RoleTrader))
	require.Equal(t, http.StatusForbidden, recorder.Code)
}

func TestRegisterVesselHappyPath(t *testing.T) {
	fake := fakeRegistry{vessel: registry.Vessel{VesselID: "vessel-001", IMONumber: "9074729", Status: registry.VesselApplication}}
	handler := registryTestHandler(t, fake)
	recorder := httptest.NewRecorder()
	body := `{"vesselId":"vessel-001","imoNumber":"9074729","mmsi":"657123456","vesselName":"MV Lagoon Star","flagState":"NG","classSociety":"DNV","grossTonnage":15000,"buildYear":2010,"buildCountry":"NG","ownerName":"Lagoon Shipping Ltd","ownerCountry":"NG"}`
	handler.ServeHTTP(recorder, authedRequest(t, http.MethodPost, "/v1/registry/vessels", body, RoleRegistryOfficer))
	require.Equal(t, http.StatusCreated, recorder.Code, recorder.Body.String())
	var vessel registry.Vessel
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &vessel))
	require.Equal(t, "vessel-001", vessel.VesselID)
}

func TestRegisterVesselValidationFailureMaps400(t *testing.T) {
	fake := fakeRegistry{registerErr: errors.New("IMO number \"0000000x\" fails the weighted mod-10 check digit")}
	handler := registryTestHandler(t, fake)
	recorder := httptest.NewRecorder()
	body := `{"vesselId":"vessel-001","imoNumber":"0000000x","mmsi":"657123456","vesselName":"MV X","flagState":"NG","classSociety":"DNV","grossTonnage":1,"buildYear":2010,"buildCountry":"NG","ownerName":"O","ownerCountry":"NG"}`
	handler.ServeHTTP(recorder, authedRequest(t, http.MethodPost, "/v1/registry/vessels", body, RoleRegistryOfficer))
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestRegistryErrorMapping(t *testing.T) {
	handler := registryTestHandler(t, fakeRegistry{registerErr: registry.ErrNotFound})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authedRequest(t, http.MethodGet, "/v1/registry/vessels/vessel-x", "", RoleRegistryOfficer))
	require.Equal(t, http.StatusNotFound, recorder.Code)

	conflictHandler := registryTestHandler(t, fakeRegistry{transitionEr: registry.ErrMakerChecker})
	recorder = httptest.NewRecorder()
	conflictHandler.ServeHTTP(recorder, authedRequest(t, http.MethodPost, "/v1/registry/vessels/vessel-1/transitions", `{"target":"REGISTRATION"}`, RoleRegistryOfficer))
	require.Equal(t, http.StatusConflict, recorder.Code)
}

func TestVerifyCertificateRequiresVerifierRole(t *testing.T) {
	handler := registryTestHandler(t, fakeRegistry{})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authedRequest(t, http.MethodGet, "/v1/registry/certificates/verify?certificateNumber=NG-1", "", RoleRegistryOfficer))
	require.Equal(t, http.StatusForbidden, recorder.Code)
}

func TestVerifyCertificateHappyPath(t *testing.T) {
	fake := fakeRegistry{verify: registry.Verification{CertificateNumber: "NG-1", Outcome: "VALID", UsageID: "usage-1"}}
	handler := registryTestHandler(t, fake)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authedRequest(t, http.MethodGet, "/v1/registry/certificates/verify?certificateNumber=NG-1", "", RoleRegistryVerifier))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var verification registry.Verification
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &verification))
	require.Equal(t, "VALID", verification.Outcome)
}

func TestApplyPermitConflictOnIneligible(t *testing.T) {
	fake := fakeRegistry{permitErr: registry.ErrConflict}
	handler := registryTestHandler(t, fake)
	recorder := httptest.NewRecorder()
	body := `{"permitId":"permit-1","vesselId":"vessel-001","nationalOwnershipPct":100,"tradeRoute":"Lagos-Port Harcourt"}`
	handler.ServeHTTP(recorder, authedRequest(t, http.MethodPost, "/v1/registry/cabotage-permits", body, RoleRegistryOfficer))
	require.Equal(t, http.StatusConflict, recorder.Code)
}

func TestDecidePermitMakerCheckerConflict(t *testing.T) {
	fake := fakeRegistry{decideErr: registry.ErrMakerChecker}
	handler := registryTestHandler(t, fake)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authedRequest(t, http.MethodPost, "/v1/registry/cabotage-permits/permit-1/decision", `{"approve":true}`, RoleRegistryOfficer))
	require.Equal(t, http.StatusConflict, recorder.Code)
}

func TestNewFailsClosedWithoutRegistry(t *testing.T) {
	config := testConfig()
	config.Registry = nil
	if _, err := New(config); err == nil {
		t.Fatal("server must fail closed without a registry store")
	}
}
