// Package registry implements the Phase 12 ministry-coverage modules: the
// ship registry (IMO/MMSI-validated vessel registration with maker-checker
// workflow and hash-chained ownership history), seafarer STCW certification
// (issue/expiry, flag endorsements, metered third-party verification and an
// expiry sweep) and cabotage enforcement (configurable Nigerian Coastal
// Trade eligibility rules, permit workflow and violation flags). All stores
// are tenant-scoped under RLS via tenantdb.WithTx and lifecycle events are
// JWS-signed into the platform outbox in the same transaction as the
// mutation.
package registry

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/imonumber"
	"github.com/munisp/blueeconomy-port-interoperability/internal/mmsinumber"
)

// ErrNotFound is returned when the addressed aggregate is absent or not
// visible to the tenant.
var ErrNotFound = errors.New("registry aggregate not found")

// ErrConflict is returned when a state-machine transition is not legal from
// the aggregate's current status, or when a uniqueness rule (open permit,
// live IMO registration) is violated.
var ErrConflict = errors.New("registry state conflict")

// ErrMakerChecker is returned when the acting principal is the same person
// who performed the preceding maker step; dual control forbids self-check.
var ErrMakerChecker = errors.New("maker-checker violation: checker must differ from maker")

var (
	countryCode = regexp.MustCompile(`^[A-Z]{2}$`)
	identifier  = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)
)

// VesselStatus is the ship-registry registration workflow state.
type VesselStatus string

const (
	VesselApplication       VesselStatus = "APPLICATION"
	VesselSurvey            VesselStatus = "SURVEY"
	VesselRegistration      VesselStatus = "REGISTRATION"
	VesselCertificateIssued VesselStatus = "CERTIFICATE_ISSUED"
	VesselSuspended         VesselStatus = "SUSPENDED"
	VesselDeregistered      VesselStatus = "DEREGISTERED"
)

// vesselTransitions is the closed registration state machine. CERTIFICATE_
// ISSUED is the terminal success state; SUSPENDED is reachable from any
// post-application state on flag-administration action and returns to
// REGISTRATION on reinstatement; DEREGISTERED is terminal from any state.
var vesselTransitions = map[VesselStatus][]VesselStatus{
	VesselApplication:       {VesselSurvey, VesselDeregistered},
	VesselSurvey:            {VesselRegistration, VesselSuspended, VesselDeregistered},
	VesselRegistration:      {VesselCertificateIssued, VesselSuspended, VesselDeregistered},
	VesselCertificateIssued: {VesselSuspended, VesselDeregistered},
	VesselSuspended:         {VesselRegistration, VesselDeregistered},
	VesselDeregistered:      {},
}

// CanTransition reports whether the vessel state machine permits
// from → to. Unknown states never transition (fail closed).
func CanTransition(from, to VesselStatus) bool {
	for _, next := range vesselTransitions[from] {
		if next == to {
			return true
		}
	}
	return false
}

// Vessel is a registered (or registering) vessel aggregate.
type Vessel struct {
	VesselID          string       `json:"vesselId"`
	IMONumber         string       `json:"imoNumber"`
	MMSI              string       `json:"mmsi"`
	VesselName        string       `json:"vesselName"`
	FlagState         string       `json:"flagState"`
	ClassSociety      string       `json:"classSociety"`
	GrossTonnage      int          `json:"grossTonnage"`
	BuildYear         int          `json:"buildYear"`
	BuildCountry      string       `json:"buildCountry"`
	OwnerName         string       `json:"ownerName"`
	OwnerCountry      string       `json:"ownerCountry"`
	Status            VesselStatus `json:"status"`
	CertificateNumber string       `json:"certificateNumber,omitempty"`
	CreatedBy         string       `json:"createdBy"`
	CreatedAt         time.Time    `json:"createdAt"`
	UpdatedAt         time.Time    `json:"updatedAt"`
	Version           int          `json:"version"`
}

// RegisterVesselRequest opens a vessel registration application. IMO and
// MMSI check validation is fail-closed before any persistence.
type RegisterVesselRequest struct {
	VesselID     string `json:"vesselId"`
	IMONumber    string `json:"imoNumber"`
	MMSI         string `json:"mmsi"`
	VesselName   string `json:"vesselName"`
	FlagState    string `json:"flagState"`
	ClassSociety string `json:"classSociety"`
	GrossTonnage int    `json:"grossTonnage"`
	BuildYear    int    `json:"buildYear"`
	BuildCountry string `json:"buildCountry"`
	OwnerName    string `json:"ownerName"`
	OwnerCountry string `json:"ownerCountry"`
}

// Validate enforces the registration invariants fail-closed.
func (request RegisterVesselRequest) Validate() error {
	if !identifier.MatchString(request.VesselID) {
		return errors.New("vesselId must be 1-64 characters of [A-Za-z0-9._:-]")
	}
	if !imonumber.Valid(request.IMONumber) {
		return fmt.Errorf("IMO number %q fails the weighted mod-10 check digit", request.IMONumber)
	}
	if !mmsinumber.Valid(request.MMSI) {
		return fmt.Errorf("MMSI %q is not a nine-digit ship-station identity with an admitted MID", request.MMSI)
	}
	if strings.TrimSpace(request.VesselName) == "" || len(request.VesselName) > 256 {
		return errors.New("vesselName must be 1-256 characters")
	}
	if !countryCode.MatchString(request.FlagState) {
		return errors.New("flagState must be an ISO 3166-1 alpha-2 code")
	}
	if strings.TrimSpace(request.ClassSociety) == "" || len(request.ClassSociety) > 128 {
		return errors.New("classSociety must be 1-128 characters")
	}
	if request.GrossTonnage <= 0 {
		return errors.New("grossTonnage must be positive")
	}
	if request.BuildYear < 1800 || request.BuildYear > 2100 {
		return errors.New("buildYear must be between 1800 and 2100")
	}
	if !countryCode.MatchString(request.BuildCountry) {
		return errors.New("buildCountry must be an ISO 3166-1 alpha-2 code")
	}
	if strings.TrimSpace(request.OwnerName) == "" || len(request.OwnerName) > 256 {
		return errors.New("ownerName must be 1-256 characters")
	}
	if !countryCode.MatchString(request.OwnerCountry) {
		return errors.New("ownerCountry must be an ISO 3166-1 alpha-2 code")
	}
	return nil
}

// OwnershipEntry is one hash-chained ownership-history record.
type OwnershipEntry struct {
	VesselID      string    `json:"vesselId"`
	SequenceNo    int       `json:"sequenceNo"`
	OwnerName     string    `json:"ownerName"`
	OwnerCountry  string    `json:"ownerCountry"`
	EffectiveFrom time.Time `json:"effectiveFrom"`
	RecordedBy    string    `json:"recordedBy"`
	RecordedAt    time.Time `json:"recordedAt"`
	PreviousHash  string    `json:"previousHash"`
	EntryHash     string    `json:"entryHash"`
}
