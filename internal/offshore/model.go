// Package offshore implements the offshore terminal-call object for tanker
// calls at SBM/SPM/FPSO terminals — calls that never touch a berth but carry
// the highest revenue density in the system. It covers call registration,
// berthing/mooring windows, the mooring-master workflow, append-only
// operational events (hose connection, loading-arm operations, custody-
// transfer metering) and departure, with fees assessed deterministically
// against the versioned tariff schedule (internal/tariff).
package offshore

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Status is the mooring-master workflow state of an offshore terminal call.
type Status string

const (
	StatusNominated          Status = "NOMINATED"
	StatusApproachCleared    Status = "APPROACH_CLEARED"
	StatusMoored             Status = "MOORED"
	StatusHoseConnected      Status = "HOSE_CONNECTED"
	StatusLoading            Status = "LOADING"
	StatusCustodyTransferred Status = "CUSTODY_TRANSFERRED"
	StatusDisconnected       Status = "DISCONNECTED"
	StatusDeparted           Status = "DEPARTED"
	StatusCancelled          Status = "CANCELLED"
)

// TerminalKind classifies the offshore installation.
type TerminalKind string

const (
	TerminalSBM  TerminalKind = "SBM"
	TerminalSPM  TerminalKind = "SPM"
	TerminalFPSO TerminalKind = "FPSO"
)

// EventType classifies append-only operational events.
type EventType string

const (
	EventHoseConnection      EventType = "HOSE_CONNECTION"
	EventLoadingArmStart     EventType = "LOADING_ARM_START"
	EventLoadingArmStop      EventType = "LOADING_ARM_STOP"
	EventCustodyMeterReading EventType = "CUSTODY_METER_READING"
	EventMooringNote         EventType = "MOORING_NOTE"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with a retained offshore call")
	ErrNotFound            = errors.New("offshore terminal call not found")
	ErrInvalidTransition   = errors.New("invalid offshore call transition")
	ErrOptimisticConflict  = errors.New("offshore terminal call changed concurrently")
	ErrEventRejected       = errors.New("operational event is not valid for the current call state")
)

var (
	callIDPattern     = regexp.MustCompile(`^[A-Za-z0-9._:-]{2,64}$`)
	imoPattern        = regexp.MustCompile(`^[0-9]{7}$`)
	terminalPattern   = regexp.MustCompile(`^[A-Z0-9-]{2,16}$`)
	agencyCodePattern = regexp.MustCompile(`^[A-Z]{2,8}$`)
)

// CreateRequest registers a tanker call at an offshore terminal.
type CreateRequest struct {
	CallID             string       `json:"call_id"`
	VesselIMO          string       `json:"vessel_imo"`
	VesselName         string       `json:"vessel_name"`
	TerminalCode       string       `json:"terminal_code"`
	TerminalKind       TerminalKind `json:"terminal_kind"`
	BuoyID             string       `json:"buoy_id"`
	AgencyCode         string       `json:"agency_code"`
	GrossTonnage       int64        `json:"gross_tonnage"`
	CargoTonnes        int64        `json:"cargo_tonnes"`
	MooringWindowStart time.Time    `json:"mooring_window_start"`
	MooringWindowEnd   time.Time    `json:"mooring_window_end"`
	NominatedBy        string       `json:"nominated_by"`
}

// Call is an offshore terminal call aggregate.
type Call struct {
	CreateRequest
	Status        Status    `json:"status"`
	MooringMaster *string   `json:"mooring_master,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	Version       int64     `json:"version"`
}

func canonical(value string, min, max int) bool {
	return len(value) >= min && len(value) <= max && strings.TrimSpace(value) == value
}

func (request CreateRequest) Validate() error {
	if !callIDPattern.MatchString(request.CallID) {
		return errors.New("call_id must be 2-64 canonical characters")
	}
	if !imoPattern.MatchString(request.VesselIMO) {
		return errors.New("vessel_imo must be exactly seven digits")
	}
	if !canonical(request.VesselName, 2, 256) {
		return errors.New("vessel_name must be canonical text between 2 and 256 characters")
	}
	if !terminalPattern.MatchString(request.TerminalCode) {
		return errors.New("terminal_code must be 2-16 uppercase alphanumeric/dash characters")
	}
	switch request.TerminalKind {
	case TerminalSBM, TerminalSPM, TerminalFPSO:
	default:
		return errors.New("terminal_kind must be SBM, SPM or FPSO")
	}
	if !canonical(request.BuoyID, 1, 64) {
		return errors.New("buoy_id must be canonical text of at most 64 characters")
	}
	if !agencyCodePattern.MatchString(request.AgencyCode) {
		return fmt.Errorf("agency_code must be two to eight uppercase letters (NPA, NIMASA, NIWA, FMMBE, CBN)")
	}
	if request.GrossTonnage <= 0 {
		return errors.New("gross_tonnage must be positive")
	}
	if request.CargoTonnes < 0 {
		return errors.New("cargo_tonnes must be non-negative")
	}
	if request.MooringWindowStart.IsZero() || !request.MooringWindowEnd.After(request.MooringWindowStart) {
		return errors.New("mooring window must be a non-empty interval")
	}
	if !canonical(request.NominatedBy, 2, 256) {
		return errors.New("nominated_by must be canonical text between 2 and 256 characters")
	}
	return nil
}

// Matches reports whether a retained call is the exact replay of a request
// (idempotency-key conflict detection).
func (call Call) Matches(request CreateRequest) bool {
	return call.CallID == request.CallID && call.VesselIMO == request.VesselIMO &&
		call.VesselName == request.VesselName && call.TerminalCode == request.TerminalCode &&
		call.TerminalKind == request.TerminalKind && call.BuoyID == request.BuoyID &&
		call.AgencyCode == request.AgencyCode && call.GrossTonnage == request.GrossTonnage &&
		call.CargoTonnes == request.CargoTonnes &&
		call.MooringWindowStart.Equal(request.MooringWindowStart.UTC()) &&
		call.MooringWindowEnd.Equal(request.MooringWindowEnd.UTC()) &&
		call.NominatedBy == request.NominatedBy
}

// ValidTransition encodes the mooring-master workflow: NOMINATED through
// DEPARTED in order, with CANCELLED as the pre-mooring abort. Terminal states
// accept no further transitions.
func ValidTransition(current, next Status) bool {
	switch current {
	case StatusNominated:
		return next == StatusApproachCleared || next == StatusCancelled
	case StatusApproachCleared:
		return next == StatusMoored || next == StatusCancelled
	case StatusMoored:
		return next == StatusHoseConnected
	case StatusHoseConnected:
		return next == StatusLoading
	case StatusLoading:
		return next == StatusCustodyTransferred
	case StatusCustodyTransferred:
		return next == StatusDisconnected
	case StatusDisconnected:
		return next == StatusDeparted
	default:
		return false
	}
}

// OperationalEvent is one append-only event on a call.
type OperationalEvent struct {
	EventID    string    `json:"event_id"`
	CallID     string    `json:"call_id"`
	EventType  EventType `json:"event_type"`
	RecordedBy string    `json:"recorded_by"`
	RecordedAt time.Time `json:"recorded_at"`
	Remarks    string    `json:"remarks"`
	// Custody-transfer metering readings (m³). The transferred volume is
	// derived as closing minus opening; it is never client-supplied.
	MeterID        *string `json:"meter_id,omitempty"`
	MeterOpeningM3 *string `json:"meter_opening_m3,omitempty"`
	MeterClosingM3 *string `json:"meter_closing_m3,omitempty"`
}

// EventAllowed encodes which operational events are recordable in which
// workflow states (fail closed: events outside the operating window are
// rejected, never silently dropped).
func EventAllowed(status Status, eventType EventType) bool {
	switch eventType {
	case EventHoseConnection:
		return status == StatusMoored || status == StatusHoseConnected
	case EventLoadingArmStart, EventLoadingArmStop:
		return status == StatusHoseConnected || status == StatusLoading
	case EventCustodyMeterReading:
		return status == StatusLoading || status == StatusCustodyTransferred
	case EventMooringNote:
		switch status {
		case StatusNominated, StatusApproachCleared, StatusMoored, StatusHoseConnected,
			StatusLoading, StatusCustodyTransferred, StatusDisconnected:
			return true
		}
		return false
	default:
		return false
	}
}
