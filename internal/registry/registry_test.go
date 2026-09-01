package registry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func validVesselRequest() RegisterVesselRequest {
	return RegisterVesselRequest{
		VesselID:     "vessel-001",
		IMONumber:    "9074729", // canonical IMO worked example
		MMSI:         "657123456",
		VesselName:   "MV Lagoon Star",
		FlagState:    "NG",
		ClassSociety: "DNV",
		GrossTonnage: 15000,
		BuildYear:    2010,
		BuildCountry: "NG",
		OwnerName:    "Lagoon Shipping Ltd",
		OwnerCountry: "NG",
	}
}

func TestRegisterVesselRequestValidateAcceptsValid(t *testing.T) {
	require.NoError(t, validVesselRequest().Validate())
}

func TestRegisterVesselRequestValidateRejectsBadIMO(t *testing.T) {
	request := validVesselRequest()
	request.IMONumber = "9074728" // wrong check digit
	require.ErrorContains(t, request.Validate(), "IMO")
	request.IMONumber = "907472" // too short
	require.Error(t, request.Validate())
}

func TestRegisterVesselRequestValidateRejectsBadMMSI(t *testing.T) {
	request := validVesselRequest()
	request.MMSI = "999123456" // unallocated MID
	require.ErrorContains(t, request.Validate(), "MMSI")
	request.MMSI = "65712345" // too short
	require.Error(t, request.Validate())
}

func TestRegisterVesselRequestValidateRejectsBadFields(t *testing.T) {
	cases := map[string]func(*RegisterVesselRequest){
		"vessel id":     func(r *RegisterVesselRequest) { r.VesselID = "bad id!" },
		"flag":          func(r *RegisterVesselRequest) { r.FlagState = "NGA" },
		"tonnage":       func(r *RegisterVesselRequest) { r.GrossTonnage = 0 },
		"build year":    func(r *RegisterVesselRequest) { r.BuildYear = 1700 },
		"build country": func(r *RegisterVesselRequest) { r.BuildCountry = "ng" },
		"owner country": func(r *RegisterVesselRequest) { r.OwnerCountry = "" },
		"owner name":    func(r *RegisterVesselRequest) { r.OwnerName = "  " },
	}
	for name, mutate := range cases {
		request := validVesselRequest()
		mutate(&request)
		require.Error(t, request.Validate(), name)
	}
}

func TestVesselStateMachineForwardPath(t *testing.T) {
	require.True(t, CanTransition(VesselApplication, VesselSurvey))
	require.True(t, CanTransition(VesselSurvey, VesselRegistration))
	require.True(t, CanTransition(VesselRegistration, VesselCertificateIssued))
}

func TestVesselStateMachineSuspensionAndReinstatement(t *testing.T) {
	require.True(t, CanTransition(VesselCertificateIssued, VesselSuspended))
	require.True(t, CanTransition(VesselSuspended, VesselRegistration))
	require.True(t, CanTransition(VesselRegistration, VesselSuspended))
}

func TestVesselStateMachineRejectsIllegal(t *testing.T) {
	illegal := [][2]VesselStatus{
		{VesselApplication, VesselRegistration},      // skips survey
		{VesselApplication, VesselCertificateIssued}, // skips survey+registration
		{VesselSurvey, VesselCertificateIssued},      // skips registration
		{VesselDeregistered, VesselApplication},      // terminal
		{VesselDeregistered, VesselSurvey},           // terminal
		{VesselCertificateIssued, VesselApplication}, // no way back
		{VesselSuspended, VesselCertificateIssued},   // reinstate to registration first
		{"", VesselSurvey},                           // unknown state fails closed
		{VesselSurvey, ""},
	}
	for _, pair := range illegal {
		require.False(t, CanTransition(pair[0], pair[1]), "%s -> %s must be illegal", pair[0], pair[1])
	}
}

func TestCertificateStateMachineReservedExpiry(t *testing.T) {
	require.Contains(t, certificateTransitions[CertificateActive], CertificateExpired)
	require.Empty(t, certificateTransitions[CertificateRevoked])
	require.Empty(t, certificateTransitions[CertificateExpired])
	require.Contains(t, certificateTransitions[CertificateSuspended], CertificateActive)
}

func TestIssueCertificateRequestValidate(t *testing.T) {
	valid := IssueCertificateRequest{
		CertificateNumber: "NG-STCW-2025-0001",
		SeafarerID:        "seafarer-001",
		CertificateType:   "STCW-II-1",
		IssuingAuthority:  "NIMASA",
		FlagEndorsement:   "NG",
		IssuedAt:          "2025-01-01",
		ExpiresAt:         "2030-01-01",
	}
	require.NoError(t, valid.Validate())

	badType := valid
	badType.CertificateType = "STCW-IX-9"
	require.ErrorContains(t, badType.Validate(), "STCW")

	badWindow := valid
	badWindow.ExpiresAt = "2024-12-31"
	require.ErrorContains(t, badWindow.Validate(), "expiresAt")

	badEndorsement := valid
	badEndorsement.FlagEndorsement = "NGA"
	require.Error(t, badEndorsement.Validate())

	badNumber := valid
	badNumber.CertificateNumber = "abc"
	require.Error(t, badNumber.Validate())
}

func TestRegisterSeafarerRequestValidate(t *testing.T) {
	valid := RegisterSeafarerRequest{
		SeafarerID:  "seafarer-001",
		FullName:    "Adaeze Okafor",
		DateOfBirth: "1990-04-12",
		Nationality: "NG",
		Rank:        "Chief Mate",
	}
	require.NoError(t, valid.Validate())

	tooYoung := valid
	tooYoung.DateOfBirth = time.Now().UTC().AddDate(-10, 0, 0).Format("2006-01-02")
	require.Error(t, tooYoung.Validate())

	badDate := valid
	badDate.DateOfBirth = "12-04-1990"
	require.Error(t, badDate.Validate())
}

func TestOwnershipHashChainDeterministic(t *testing.T) {
	genesis := genesisHash("vessel-001", "9074729")
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, genesis)
	at := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	first := chainHash(genesis, "vessel-001", 1, "Lagoon Shipping Ltd", "NG", at, at, "officer-1")
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, first)
	// Same inputs recompute identically; any single-field change diverges.
	require.Equal(t, first, chainHash(genesis, "vessel-001", 1, "Lagoon Shipping Ltd", "NG", at, at, "officer-1"))
	require.NotEqual(t, first, chainHash(genesis, "vessel-001", 1, "Other Owner", "NG", at, at, "officer-1"))
	require.NotEqual(t, first, chainHash(first, "vessel-001", 2, "Lagoon Shipping Ltd", "NG", at, at, "officer-1"))
	// Genesis commits to the vessel identity: chains cannot be transplanted.
	require.NotEqual(t, genesis, genesisHash("vessel-002", "9074729"))
	require.NotEqual(t, genesis, genesisHash("vessel-001", "9434187"))
}
