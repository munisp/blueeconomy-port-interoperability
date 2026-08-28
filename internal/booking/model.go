// Package booking implements the eCallUp 2.0 per-truck port access booking
// domain: a fail-closed state machine, terminal slot capacity management,
// payment intents, gate scans and offline-mode reconciliation.
package booking

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type Status string

const (
	StatusDrafted                Status = "DRAFTED"
	StatusPendingSync            Status = "PENDING_SYNC"
	StatusSlotReserved           Status = "SLOT_RESERVED"
	StatusPaid                   Status = "PAID"
	StatusValidationPending      Status = "VALIDATION_PENDING"
	StatusGateApproved           Status = "GATE_APPROVED"
	StatusCompleted              Status = "COMPLETED"
	StatusCancelled              Status = "CANCELLED"
	StatusExpired                Status = "EXPIRED"
	StatusRejected               Status = "REJECTED"
	StatusReconciliationRequired Status = "RECONCILIATION_REQUIRED"
)

type Channel string

const (
	ChannelWeb     Channel = "WEB"
	ChannelUSSD    Channel = "USSD"
	ChannelOffline Channel = "OFFLINE"
)

var (
	ErrNotFound            = errors.New("booking resource not found")
	ErrInvalidTransition   = errors.New("invalid booking state transition")
	ErrOptimisticConflict  = errors.New("booking changed concurrently")
	ErrIdempotencyConflict = errors.New("request id conflicts with a retained booking")
	ErrSlotUnavailable     = errors.New("terminal slot has no remaining capacity")
	ErrSlotWindow          = errors.New("slot time window is not valid for this operation")
	ErrGateDenied          = errors.New("gate scan does not satisfy booking, slot and payment checks")
	ErrPaymentInvalid      = errors.New("payment intent or confirmation is not valid for this booking")
)

// transitions is the complete fail-closed booking state machine. Any pair not
// listed here is prohibited.
var transitions = map[Status]map[Status]bool{
	StatusDrafted: {
		StatusSlotReserved: true,
		StatusCancelled:    true,
	},
	StatusPendingSync: {
		StatusSlotReserved:           true, // reconciliation: capacity confirmed on reconnect
		StatusReconciliationRequired: true, // reconciliation: conflict, never silent
		StatusCancelled:              true,
	},
	StatusSlotReserved: {
		StatusPaid:      true,
		StatusExpired:   true,
		StatusCancelled: true,
	},
	StatusPaid: {
		StatusGateApproved:      true,
		StatusValidationPending: true, // customs cross-validation starts
		StatusExpired:           true,
		StatusCancelled:         true,
	},
	StatusValidationPending: {
		StatusPaid:      true, // customs validation matched: gate eligibility restored
		StatusRejected:  true, // customs validation mismatch: fail closed with reason code
		StatusExpired:   true,
		StatusCancelled: true,
	},
	StatusGateApproved: {
		StatusCompleted: true,
	},
	StatusRejected: {
		StatusCancelled: true,
	},
	StatusReconciliationRequired: {
		StatusSlotReserved: true, // operator resolves conflict against a different slot
		StatusCancelled:    true,
	},
}

func ValidTransition(current, next Status) bool {
	return transitions[current][next]
}

var (
	truckPlatePattern     = regexp.MustCompile(`^[A-Z0-9][A-Z0-9-]{2,15}$`)
	msisdnPattern         = regexp.MustCompile(`^\+[0-9]{8,15}$`)
	terminalPattern       = regexp.MustCompile(`^[A-Z][A-Z0-9-]{1,31}$`)
	portCodePattern       = regexp.MustCompile(`^[A-Z]{2,8}$`)
	declarationRefPattern = regexp.MustCompile(`^[A-Z0-9][A-Z0-9/-]{3,63}$`)
)

type CreateRequest struct {
	RequestID     string    `json:"request_id"`
	TruckPlate    string    `json:"truck_plate"`
	TruckerMSISDN string    `json:"trucker_msisdn"`
	TerminalID    string    `json:"terminal_id"`
	Channel       Channel   `json:"channel"`
	AmountKobo    int64     `json:"amount_kobo"`
	ExpiresAt     time.Time `json:"expires_at"`
	// Optional Nigeria Customs cargo declaration binding. When present, the
	// booking must clear customs cross-validation before gate approval.
	CargoDeclarationRef string `json:"cargo_declaration_ref,omitempty"`
	DeclaredWeightKg    int64  `json:"declared_weight_kg,omitempty"`
	ConsigneeID         string `json:"consignee_id,omitempty"`
	OperatorID          string `json:"operator_id,omitempty"`
}

type Booking struct {
	BookingID            string    `json:"booking_id"`
	TenantID             string    `json:"tenant_id"`
	RequestID            string    `json:"request_id"`
	TruckPlate           string    `json:"truck_plate"`
	TruckerMSISDN        string    `json:"trucker_msisdn"`
	TerminalID           string    `json:"terminal_id"`
	SlotID               *string   `json:"slot_id,omitempty"`
	Channel              Channel   `json:"channel"`
	Status               Status    `json:"status"`
	AmountKobo           int64     `json:"amount_kobo"`
	Currency             string    `json:"currency"`
	CargoDeclarationRef  *string   `json:"cargo_declaration_ref,omitempty"`
	DeclaredWeightKg     *int64    `json:"declared_weight_kg,omitempty"`
	ConsigneeID          *string   `json:"consignee_id,omitempty"`
	OperatorID           *string   `json:"operator_id,omitempty"`
	PaymentReceiptRef    *string   `json:"payment_receipt_ref,omitempty"`
	GateID               *string   `json:"gate_id,omitempty"`
	LedgerCommitHash     *string   `json:"ledger_commit_hash,omitempty"`
	ReconciliationReason *string   `json:"reconciliation_reason,omitempty"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
	ExpiresAt            time.Time `json:"expires_at"`
	Version              int64     `json:"version"`
}

type Slot struct {
	SlotID     string    `json:"slot_id"`
	TerminalID string    `json:"terminal_id"`
	PortCode   string    `json:"port_code"`
	StartsAt   time.Time `json:"starts_at"`
	EndsAt     time.Time `json:"ends_at"`
	Capacity   int       `json:"capacity"`
	Reserved   int       `json:"reserved"`
	CreatedAt  time.Time `json:"created_at"`
}

type PaymentIntent struct {
	IntentID      string    `json:"intent_id"`
	BookingID     string    `json:"booking_id"`
	RequestID     string    `json:"request_id"`
	AmountKobo    int64     `json:"amount_kobo"`
	Currency      string    `json:"currency"`
	MojaloopTxRef string    `json:"mojaloop_tx_ref"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
}

type GateScan struct {
	ScanID       string    `json:"scan_id"`
	BookingID    string    `json:"booking_id"`
	GateID       string    `json:"gate_id"`
	ScannedBy    string    `json:"scanned_by"`
	Decision     string    `json:"decision"`
	DenialReason *string   `json:"denial_reason,omitempty"`
	ScannedAt    time.Time `json:"scanned_at"`
}

func (request CreateRequest) Validate() error {
	if len(request.RequestID) < 8 || len(request.RequestID) > 128 || request.RequestID != strings.TrimSpace(request.RequestID) {
		return errors.New("request_id must be canonical text between 8 and 128 characters")
	}
	if !truckPlatePattern.MatchString(request.TruckPlate) {
		return errors.New("truck_plate must be an uppercase registration plate")
	}
	if !msisdnPattern.MatchString(request.TruckerMSISDN) {
		return errors.New("trucker_msisdn must be E.164")
	}
	if !terminalPattern.MatchString(request.TerminalID) {
		return errors.New("terminal_id is invalid")
	}
	switch request.Channel {
	case ChannelWeb, ChannelUSSD, ChannelOffline:
	default:
		return errors.New("channel must be WEB, USSD or OFFLINE")
	}
	if request.AmountKobo <= 0 {
		return errors.New("amount_kobo must be positive")
	}
	if request.ExpiresAt.IsZero() || request.ExpiresAt.Before(time.Now().UTC()) {
		return errors.New("expires_at must be in the future")
	}
	if request.CargoDeclarationRef == "" {
		if request.DeclaredWeightKg != 0 || request.ConsigneeID != "" || request.OperatorID != "" {
			return errors.New("customs declaration fields require cargo_declaration_ref")
		}
		return nil
	}
	if !declarationRefPattern.MatchString(request.CargoDeclarationRef) {
		return errors.New("cargo_declaration_ref is invalid")
	}
	if request.DeclaredWeightKg <= 0 {
		return errors.New("declared_weight_kg must be positive when a cargo declaration is referenced")
	}
	if len(request.ConsigneeID) < 2 || len(request.ConsigneeID) > 128 || request.ConsigneeID != strings.TrimSpace(request.ConsigneeID) {
		return errors.New("consignee_id must be canonical text between 2 and 128 characters")
	}
	if len(request.OperatorID) < 2 || len(request.OperatorID) > 128 || request.OperatorID != strings.TrimSpace(request.OperatorID) {
		return errors.New("operator_id must be canonical text between 2 and 128 characters")
	}
	return nil
}

func ValidateSlotWindow(startsAt, endsAt time.Time, capacity int) error {
	if !endsAt.After(startsAt) {
		return errors.New("slot ends_at must be after starts_at")
	}
	if capacity < 1 {
		return errors.New("slot capacity must be at least 1")
	}
	return nil
}

func ValidateTerminalID(value string) error {
	if !terminalPattern.MatchString(value) {
		return fmt.Errorf("terminal_id %q is invalid", value)
	}
	return nil
}

func ValidatePortCode(value string) error {
	if !portCodePattern.MatchString(value) {
		return fmt.Errorf("port_code %q is invalid", value)
	}
	return nil
}
