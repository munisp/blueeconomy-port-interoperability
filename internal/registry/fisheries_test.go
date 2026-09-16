package registry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func validFisheriesApplication() ApplyFisheriesPermitRequest {
	return ApplyFisheriesPermitRequest{
		PermitID:     "fp-1",
		PermitNumber: "FISH-NG-2026-0001",
		PermitType:   FisheriesArtisanal,
		VesselID:     "vessel-1",
		OwnerName:    "Integration Shipping",
		ValidFrom:    time.Now().UTC(),
		ValidTo:      time.Now().UTC().Add(365 * 24 * time.Hour),
	}
}

func TestFisheriesApplicationValidation(t *testing.T) {
	require.NoError(t, validateFisheriesRequest(validFisheriesApplication()))

	missingVessel := validFisheriesApplication()
	missingVessel.VesselID = ""
	require.ErrorContains(t, validateFisheriesRequest(missingVessel), "vesselId")

	aquacultureWithoutSite := validFisheriesApplication()
	aquacultureWithoutSite.PermitType = FisheriesAquaculture
	aquacultureWithoutSite.VesselID = ""
	require.ErrorContains(t, validateFisheriesRequest(aquacultureWithoutSite), "siteReference")

	aquaculture := validFisheriesApplication()
	aquaculture.PermitType = FisheriesAquaculture
	aquaculture.VesselID = ""
	aquaculture.SiteReference = "lease-lagos-lagoon-07"
	require.NoError(t, validateFisheriesRequest(aquaculture))

	badType := validFisheriesApplication()
	badType.PermitType = "TRAWLER"
	require.ErrorContains(t, validateFisheriesRequest(badType), "not admitted")

	shortNumber := validFisheriesApplication()
	shortNumber.PermitNumber = "AB1"
	require.ErrorContains(t, validateFisheriesRequest(shortNumber), "permitNumber")

	badWindow := validFisheriesApplication()
	badWindow.ValidTo = badWindow.ValidFrom
	require.ErrorContains(t, validateFisheriesRequest(badWindow), "validTo")
}

func TestFisheriesLifecycleTransitions(t *testing.T) {
	// APPLICATION is decided only via Decide; the transition machine covers
	// GRANTED/SUSPENDED/REVOKED.
	require.False(t, validFisheriesTransition(FisheriesApplication, FisheriesGranted))
	require.True(t, validFisheriesTransition(FisheriesGranted, FisheriesSuspended))
	require.True(t, validFisheriesTransition(FisheriesGranted, FisheriesRevoked))
	require.True(t, validFisheriesTransition(FisheriesSuspended, FisheriesGranted)) // reinstatement
	require.True(t, validFisheriesTransition(FisheriesSuspended, FisheriesRevoked))
	require.False(t, validFisheriesTransition(FisheriesRevoked, FisheriesGranted)) // terminal
	require.False(t, validFisheriesTransition(FisheriesRevoked, FisheriesSuspended))
}

func TestFisheriesPermitTypeAdmission(t *testing.T) {
	require.True(t, FisheriesArtisanal.admitted())
	require.True(t, FisheriesIndustrial.admitted())
	require.True(t, FisheriesAquaculture.admitted())
	require.False(t, FisheriesPermitType("RECREATIONAL").admitted())
}
