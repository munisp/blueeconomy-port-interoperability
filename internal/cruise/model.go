// Package cruise implements the cruise vessel port-call object. A cruise
// call extends the existing platform port-call model (port_calls) with
// passenger count bands, excursion manifests, cruise dues assessment hooks
// (per-passenger charges from the versioned CRUISE_DUES tariff schedule —
// the NPA US$10/head passenger-charge class, recomputed on the final
// manifest) and terminal/berth allocation for cruise tenders.
package cruise

import (
	"errors"
	"regexp"
	"strings"
	"time"
)

// Status is the cruise call workflow state.
type Status string

const (
	StatusPlanned   Status = "PLANNED"
	StatusConfirmed Status = "CONFIRMED"
	StatusArrived   Status = "ARRIVED"
	StatusCompleted Status = "COMPLETED"
	StatusCancelled Status = "CANCELLED"
)

// PaxBand is the deterministic passenger-count band of a call.
type PaxBand string

const (
	BandSmall  PaxBand = "SMALL"  // < 500 pax
	BandMedium PaxBand = "MEDIUM" // 500-1499 pax
	BandLarge  PaxBand = "LARGE"  // 1500-3999 pax
	BandMega   PaxBand = "MEGA"   // >= 4000 pax
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with a retained cruise record")
	ErrNotFound            = errors.New("cruise call not found")
	ErrInvalidTransition   = errors.New("invalid cruise call transition")
	ErrOptimisticConflict  = errors.New("cruise call changed concurrently")
	ErrExcursionInvalid    = errors.New("excursion manifest is invalid")
	ErrAllocationInvalid   = errors.New("tender allocation is invalid")
)

var (
	callIDPattern   = regexp.MustCompile(`^[A-Za-z0-9._:-]{2,64}$`)
	terminalPattern = regexp.MustCompile(`^[A-Z0-9-]{2,16}$`)
	berthPattern    = regexp.MustCompile(`^[A-Z0-9-]{1,16}$`)
)

// BandFor deterministically maps a passenger count to its band.
func BandFor(paxCount int) PaxBand {
	switch {
	case paxCount < 500:
		return BandSmall
	case paxCount < 1500:
		return BandMedium
	case paxCount < 4000:
		return BandLarge
	default:
		return BandMega
	}
}

// CreateRequest registers a cruise call over an existing port call.
type CreateRequest struct {
	CallID     string `json:"call_id"`
	PortCallID string `json:"port_call_id"`
	CruiseLine string `json:"cruise_line"`
	VesselName string `json:"vessel_name"`
	PaxCount   int    `json:"pax_count"`
	CreatedBy  string `json:"created_by"`
}

// Call is a cruise call aggregate.
type Call struct {
	CreateRequest
	PaxBand   PaxBand   `json:"pax_band"`
	Status    Status    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Version   int64     `json:"version"`
}

func canonical(value string, min, max int) bool {
	return len(value) >= min && len(value) <= max && strings.TrimSpace(value) == value
}

func (request CreateRequest) Validate() error {
	if !callIDPattern.MatchString(request.CallID) {
		return errors.New("call_id must be 2-64 canonical characters")
	}
	if request.PortCallID == "" || len(request.PortCallID) > 256 || strings.TrimSpace(request.PortCallID) != request.PortCallID {
		return errors.New("port_call_id must reference an existing platform port call")
	}
	if !canonical(request.CruiseLine, 2, 256) {
		return errors.New("cruise_line must be canonical text between 2 and 256 characters")
	}
	if !canonical(request.VesselName, 2, 256) {
		return errors.New("vessel_name must be canonical text between 2 and 256 characters")
	}
	if request.PaxCount <= 0 {
		return errors.New("pax_count must be positive")
	}
	if !canonical(request.CreatedBy, 2, 256) {
		return errors.New("created_by must be canonical text between 2 and 256 characters")
	}
	return nil
}

// Matches reports whether a retained call is the exact replay of a request.
func (call Call) Matches(request CreateRequest) bool {
	return call.CallID == request.CallID && call.PortCallID == request.PortCallID &&
		call.CruiseLine == request.CruiseLine && call.VesselName == request.VesselName &&
		call.PaxCount == request.PaxCount && call.CreatedBy == request.CreatedBy
}

// ValidTransition encodes the cruise call workflow: PLANNED → CONFIRMED →
// ARRIVED → COMPLETED, with CANCELLED before arrival.
func ValidTransition(current, next Status) bool {
	switch current {
	case StatusPlanned:
		return next == StatusConfirmed || next == StatusCancelled
	case StatusConfirmed:
		return next == StatusArrived || next == StatusCancelled
	case StatusArrived:
		return next == StatusCompleted
	default:
		return false
	}
}

// Excursion is one excursion manifest attached to a call.
type Excursion struct {
	ExcursionID  string    `json:"excursion_id"`
	CallID       string    `json:"call_id"`
	Name         string    `json:"name"`
	Operator     string    `json:"operator"`
	PaxCount     int       `json:"pax_count"`
	Status       string    `json:"status"`
	RegisteredBy string    `json:"registered_by"`
	RegisteredAt time.Time `json:"registered_at"`
}

// TenderAllocation is a berth window reserved for cruise tenders.
type TenderAllocation struct {
	AllocationID string    `json:"allocation_id"`
	CallID       string    `json:"call_id"`
	TerminalCode string    `json:"terminal_code"`
	BerthCode    string    `json:"berth_code"`
	WindowStart  time.Time `json:"window_start"`
	WindowEnd    time.Time `json:"window_end"`
	AllocatedBy  string    `json:"allocated_by"`
	AllocatedAt  time.Time `json:"allocated_at"`
}
