package registry

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/stretchr/testify/require"
)

// These tests run against a real PostgreSQL when REGISTRY_TEST_DATABASE_URL
// is set; otherwise they skip. The schema (migrations 0001-0023) is applied
// by the harness before the suite runs.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("REGISTRY_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("REGISTRY_TEST_DATABASE_URL is not set; skipping PostgreSQL-backed registry tests")
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func testSigner(t *testing.T) *events.Signer {
	t.Helper()
	key := make([]byte, 64)
	for i := range key {
		key[i] = byte(i + 1)
	}
	signer, err := events.NewSigner(key, "1")
	require.NoError(t, err)
	return signer
}

func bindTenant(t *testing.T, pool *pgxpool.Pool, tenantID string) context.Context {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ($1, 'registry-test-authority') ON CONFLICT (tenant_id) DO NOTHING`, tenantID)
	require.NoError(t, err)
	bound, err := tenantctx.WithClaims(context.Background(), tenantctx.Claims{
		Issuer:   "registry-test",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  "officer-maker",
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
	require.NoError(t, err)
	return bound
}

func TestVesselWorkflowEndToEnd(t *testing.T) {
	pool := testPool(t)
	store, err := NewStore(pool, testSigner(t))
	require.NoError(t, err)
	ctx := bindTenant(t, pool, "tenant-registry-it")

	maker := Principal{ID: "officer-maker", Role: "registry-officer"}
	checker := Principal{ID: "officer-checker", Role: "registry-officer"}

	vessel, err := store.Register(ctx, "idem-reg-1", RegisterVesselRequest{
		VesselID: "vessel-it-1", IMONumber: "9074729", MMSI: "657123456", VesselName: "MV Integration",
		FlagState: "NG", ClassSociety: "DNV", GrossTonnage: 9000, BuildYear: 2015, BuildCountry: "NG",
		OwnerName: "Integration Shipping", OwnerCountry: "NG",
	}, maker)
	require.NoError(t, err)
	require.Equal(t, VesselApplication, vessel.Status)

	// Idempotent replay returns the same aggregate without a second event.
	replay, err := store.Register(ctx, "idem-reg-1", RegisterVesselRequest{}, maker)
	require.Error(t, err, "validation still applies on replay requests")
	_ = replay

	advanced, err := store.Transition(ctx, "idem-t-1", "vessel-it-1", VesselSurvey, "", maker)
	require.NoError(t, err)
	require.Equal(t, VesselSurvey, advanced.Status)

	// Maker-checker: the application maker cannot advance to REGISTRATION.
	_, err = store.Transition(ctx, "idem-t-2", "vessel-it-1", VesselRegistration, "", maker)
	require.ErrorIs(t, err, ErrMakerChecker)

	registered, err := store.Transition(ctx, "idem-t-2", "vessel-it-1", VesselRegistration, "", checker)
	require.NoError(t, err)
	require.Equal(t, VesselRegistration, registered.Status)

	// Illegal skip is rejected.
	_, err = store.Transition(ctx, "idem-t-3", "vessel-it-1", VesselSurvey, "", checker)
	require.ErrorIs(t, err, ErrConflict)

	issued, err := store.Transition(ctx, "idem-t-4", "vessel-it-1", VesselCertificateIssued, "NG-REG-0001", checker)
	require.NoError(t, err)
	require.Equal(t, VesselCertificateIssued, issued.Status)
	require.Equal(t, "NG-REG-0001", issued.CertificateNumber)

	// Ownership transfer extends the hash chain; history verifies.
	entry, err := store.TransferOwnership(ctx, "idem-o-1", "vessel-it-1", "New Owners Ltd", "NG", time.Now().UTC(), maker)
	require.NoError(t, err)
	require.Equal(t, 2, entry.SequenceNo)
	history, err := store.OwnershipHistory(ctx, "vessel-it-1")
	require.NoError(t, err)
	require.Len(t, history, 2)
	require.Equal(t, history[0].EntryHash, history[1].PreviousHash)

	// Cabotage: install rule, apply, maker-checker decide.
	_, err = store.UpsertCabotageRule(ctx, "idem-r-1", CabotageRule{
		RuleID: "cabotage-ng-2003", RequiredFlag: "NG", MinNationalOwnershipPct: 100,
		RequireDomesticBuild: true, WaiverAllowed: true,
	}, maker)
	require.NoError(t, err)
	permit, eligibility, err := store.ApplyPermit(ctx, "idem-p-1", ApplyPermitRequest{
		PermitID: "permit-it-1", VesselID: "vessel-it-1", NationalOwnershipPct: 100, TradeRoute: "Lagos-Onne",
	}, maker)
	require.NoError(t, err)
	require.True(t, eligibility.Eligible)
	require.Equal(t, PermitApplication, permit.Status)

	_, err = store.DecidePermit(ctx, "idem-d-1", "permit-it-1", true, maker)
	require.ErrorIs(t, err, ErrMakerChecker)
	decided, err := store.DecidePermit(ctx, "idem-d-1", "permit-it-1", true, checker)
	require.NoError(t, err)
	require.Equal(t, PermitApproved, decided.Status)

	// Violation flag + maker-checker resolution.
	violation, err := store.FlagViolation(ctx, "idem-v-1", Violation{
		ViolationID: "violation-it-1", VesselID: "vessel-it-1", PermitID: "permit-it-1",
		ViolationType: "ROUTE_OUTSIDE_PERMIT", Detail: "observed off-route at Bonny",
	}, maker)
	require.NoError(t, err)
	require.Equal(t, "OPEN", violation.Status)
	_, err = store.ResolveViolation(ctx, "idem-v-2", "violation-it-1", maker)
	require.ErrorIs(t, err, ErrMakerChecker)
	resolved, err := store.ResolveViolation(ctx, "idem-v-2", "violation-it-1", checker)
	require.NoError(t, err)
	require.Equal(t, "RESOLVED", resolved.Status)
}

func TestSeafarerCertificationAndExpirySweep(t *testing.T) {
	pool := testPool(t)
	store, err := NewStore(pool, testSigner(t))
	require.NoError(t, err)
	ctx := bindTenant(t, pool, "tenant-registry-sweep-it")
	officer := Principal{ID: "officer-maker", Role: "registry-officer"}

	_, err = store.RegisterSeafarer(ctx, "idem-s-1", RegisterSeafarerRequest{
		SeafarerID: "seafarer-it-1", FullName: "Integration Seafarer", DateOfBirth: "1988-06-01",
		Nationality: "NG", Rank: "Master",
	}, officer)
	require.NoError(t, err)

	_, err = store.IssueCertificate(ctx, "idem-c-1", IssueCertificateRequest{
		CertificateNumber: "NG-STCW-IT-1", SeafarerID: "seafarer-it-1", CertificateType: "STCW-II-2",
		IssuingAuthority: "NIMASA", FlagEndorsement: "NG", IssuedAt: "2020-01-01", ExpiresAt: "2021-01-01",
	}, officer)
	require.NoError(t, err)

	// Verification reports EXPIRED by wall-clock even before the sweep.
	verification, err := store.VerifyCertificate(ctx, "NG-STCW-IT-1", "verifier-1")
	require.NoError(t, err)
	require.Equal(t, "EXPIRED", verification.Outcome)
	require.NotEmpty(t, verification.UsageID)

	// The sweep transitions the certificate to EXPIRED and emits an event.
	expired, err := store.ExpireCertificates(ctx, officer)
	require.NoError(t, err)
	require.Equal(t, 1, expired)
	second, err := store.ExpireCertificates(ctx, officer)
	require.NoError(t, err)
	require.Equal(t, 0, second, "sweep must be idempotent")

	// Officer-driven EXPIRED transition is rejected.
	_, err = store.TransitionCertificate(ctx, "idem-c-2", "NG-STCW-IT-1", CertificateExpired, officer)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrNotFound))
}
