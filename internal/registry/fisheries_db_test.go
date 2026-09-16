package registry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// PostgreSQL-backed lifecycle test; skipped unless
// REGISTRY_TEST_DATABASE_URL is set (migrations 0001-0026 applied).
func TestFisheriesPermitLifecycleEndToEnd(t *testing.T) {
	pool := testPool(t)
	store, err := NewStore(pool, testSigner(t))
	require.NoError(t, err)
	ctx := bindTenant(t, pool, "tenant-fisheries-it")

	maker := Principal{ID: "officer-maker", Role: "registry-officer"}
	checker := Principal{ID: "officer-checker", Role: "registry-officer"}

	// Vessel must reach CERTIFICATE_ISSUED before it can hold a permit.
	_, err = store.Register(ctx, "idem-fv-1", RegisterVesselRequest{
		VesselID: "vessel-fish-1", IMONumber: "9074729", MMSI: "657123457", VesselName: "FV Integration",
		FlagState: "NG", ClassSociety: "DNV", GrossTonnage: 250, BuildYear: 2018, BuildCountry: "NG",
		OwnerName: "Integration Shipping", OwnerCountry: "NG",
	}, maker)
	require.NoError(t, err)
	_, err = store.Transition(ctx, "idem-fv-2", "vessel-fish-1", VesselSurvey, "", maker)
	require.NoError(t, err)
	_, err = store.Transition(ctx, "idem-fv-3", "vessel-fish-1", VesselRegistration, "", checker)
	require.NoError(t, err)
	_, err = store.Transition(ctx, "idem-fv-4", "vessel-fish-1", VesselCertificateIssued, "RC-1001", maker)
	require.NoError(t, err)

	now := time.Now().UTC()
	apply := ApplyFisheriesPermitRequest{
		PermitID: "fp-it-1", PermitNumber: "FISH-NG-2026-9001", PermitType: FisheriesArtisanal,
		VesselID: "vessel-fish-1", OwnerName: "Integration Shipping",
		ValidFrom: now, ValidTo: now.Add(180 * 24 * time.Hour),
	}
	permit, err := store.ApplyFisheriesPermit(ctx, "idem-fp-1", apply, maker)
	require.NoError(t, err)
	require.Equal(t, FisheriesApplication, permit.Status)

	// Owner linkage fails closed.
	mismatched := apply
	mismatched.PermitID = "fp-it-x"
	mismatched.PermitNumber = "FISH-NG-2026-9002"
	mismatched.OwnerName = "Someone Else"
	_, err = store.ApplyFisheriesPermit(ctx, "idem-fp-x", mismatched, maker)
	require.ErrorIs(t, err, ErrConflict)

	// Maker-checker on the decision.
	_, err = store.DecideFisheriesPermit(ctx, "idem-fp-2", "fp-it-1", true, maker)
	require.ErrorIs(t, err, ErrMakerChecker)
	granted, err := store.DecideFisheriesPermit(ctx, "idem-fp-2", "fp-it-1", true, checker)
	require.NoError(t, err)
	require.Equal(t, FisheriesGranted, granted.Status)

	// Verification: GRANTED inside the window is valid; minimal disclosure.
	verification, err := store.VerifyFisheriesPermit(ctx, "FISH-NG-2026-9001")
	require.NoError(t, err)
	require.True(t, verification.Valid)
	require.Equal(t, FisheriesArtisanal, verification.PermitType)

	// Suspend -> verification flips invalid -> reinstate -> revoke (terminal).
	_, err = store.TransitionFisheriesPermit(ctx, "idem-fp-3", "fp-it-1", FisheriesSuspended, "IUU investigation", checker)
	require.NoError(t, err)
	verification, err = store.VerifyFisheriesPermit(ctx, "FISH-NG-2026-9001")
	require.NoError(t, err)
	require.False(t, verification.Valid)
	_, err = store.TransitionFisheriesPermit(ctx, "idem-fp-4", "fp-it-1", FisheriesGranted, "investigation cleared", checker)
	require.NoError(t, err)
	revoked, err := store.TransitionFisheriesPermit(ctx, "idem-fp-5", "fp-it-1", FisheriesRevoked, "licence withdrawn", checker)
	require.NoError(t, err)
	require.Equal(t, FisheriesRevoked, revoked.Status)
	_, err = store.TransitionFisheriesPermit(ctx, "idem-fp-6", "fp-it-1", FisheriesGranted, "revived", checker)
	require.ErrorIs(t, err, ErrConflict)

	// Audit trail records the full lifecycle in order.
	trail, err := store.FisheriesAuditTrail(ctx, "fp-it-1")
	require.NoError(t, err)
	actions := make([]string, 0, len(trail))
	for _, entry := range trail {
		actions = append(actions, entry.Action)
	}
	require.Equal(t, []string{"APPLIED", "GRANTED", "SUSPENDED", "REINSTATED", "REVOKED"}, actions)

	// Unknown permit number fails closed on verification.
	_, err = store.VerifyFisheriesPermit(ctx, "FISH-NG-0000-0000")
	require.ErrorIs(t, err, ErrNotFound)
}
