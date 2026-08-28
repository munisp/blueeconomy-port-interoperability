// Package declarations implements the customs declaration domain imported
// from the singlewindow donor as a platform-conformant Go capability: a
// fail-closed lifecycle state machine
//
//	DRAFT -> SUBMITTED -> RISK_ASSESSED -> GREEN_LANE|YELLOW_LANE|RED_LANE
//	     -> CLEARED|REJECTED
//
// with amendments as superseding revisions, tenant-scoped RLS storage,
// envelope v1.0 lifecycle events on trade.declarations.v1 and a fail-closed
// risk-scoring boundary (SCORING_UNAVAILABLE terminal state — never a hash
// or heuristic fallback).
package declarations

import (
	"errors"
	"regexp"
	"strings"
	"time"
)

type Status string

const (
	StatusDraft              Status = "DRAFT"
	StatusSubmitted          Status = "SUBMITTED"
	StatusRiskAssessed       Status = "RISK_ASSESSED"
	StatusGreenLane          Status = "GREEN_LANE"
	StatusYellowLane         Status = "YELLOW_LANE"
	StatusRedLane            Status = "RED_LANE"
	StatusCleared            Status = "CLEARED"
	StatusRejected           Status = "REJECTED"
	StatusScoringUnavailable Status = "SCORING_UNAVAILABLE"
	StatusSuperseded         Status = "SUPERSEDED"
)

// RiskLane is the WCO-style selectivity lane assigned by the lane rules.
type RiskLane string

const (
	LaneGreen  RiskLane = "GREEN"
	LaneYellow RiskLane = "YELLOW"
	LaneRed    RiskLane = "RED"
)

// LaneStatus maps a risk lane to its lifecycle status.
func LaneStatus(lane RiskLane) Status {
	switch lane {
	case LaneGreen:
		return StatusGreenLane
	case LaneYellow:
		return StatusYellowLane
	default:
		return StatusRedLane
	}
}

type DeclarationType string

const (
	TypeImport          DeclarationType = "IMPORT"
	TypeExport          DeclarationType = "EXPORT"
	TypeTransit         DeclarationType = "TRANSIT"
	TypeReExport        DeclarationType = "RE_EXPORT"
	TypeTemporaryImport DeclarationType = "TEMPORARY_IMPORT"
)

var (
	ErrNotFound            = errors.New("declaration resource not found")
	ErrInvalidTransition   = errors.New("invalid declaration state transition")
	ErrOptimisticConflict  = errors.New("declaration changed concurrently")
	ErrIdempotencyConflict = errors.New("request id conflicts with a retained declaration")
	ErrDeclarationInvalid  = errors.New("declaration does not satisfy the business rules")
	ErrNotCleared          = errors.New("declaration has no clearance certificate before CLEARED")
	ErrPermitInvalid       = errors.New("a linked OGA permit is not approved or has expired")
)

// transitions is the complete fail-closed declaration state machine. Any pair
// not listed here is prohibited. Every non-terminal state escapes through
// SUPERSEDED (an amendment writes a fresh DRAFT revision that must be
// re-submitted and re-scored); CLEARED and SUPERSEDED are terminal.
var transitions = map[Status]map[Status]bool{
	StatusDraft: {
		StatusSubmitted:  true,
		StatusSuperseded: true,
	},
	StatusSubmitted: {
		StatusRiskAssessed:       true,
		StatusScoringUnavailable: true,
		StatusRejected:           true,
		StatusSuperseded:         true,
	},
	StatusRiskAssessed: {
		StatusGreenLane:  true,
		StatusYellowLane: true,
		StatusRedLane:    true,
		StatusSuperseded: true,
	},
	StatusGreenLane: {
		StatusCleared:    true,
		StatusRejected:   true,
		StatusSuperseded: true,
	},
	StatusYellowLane: {
		StatusCleared:    true,
		StatusRejected:   true,
		StatusSuperseded: true,
	},
	StatusRedLane: {
		StatusCleared:    true,
		StatusRejected:   true,
		StatusSuperseded: true,
	},
	StatusRejected: {
		StatusSuperseded: true,
	},
	StatusScoringUnavailable: {
		StatusSuperseded: true,
	},
}

// ValidTransition reports whether current -> next is permitted.
func ValidTransition(current, next Status) bool {
	return transitions[current][next]
}

// Amendable reports whether the declaration may be amended into a new DRAFT
// revision: anything that has not reached a terminal decision (CLEARED) or
// already been superseded. Amending an assessed or laned declaration starts a
// fresh DRAFT revision that must be re-submitted and re-scored.
func Amendable(status Status) bool {
	return ValidTransition(status, StatusSuperseded)
}

var (
	declarationRefPattern = regexp.MustCompile(`^[A-Z0-9][A-Z0-9/-]{3,63}$`)
	countryPattern        = regexp.MustCompile(`^[A-Z]{2}$`)
	currencyPattern       = regexp.MustCompile(`^[A-Z]{3}$`)
)

// CreateRequest carries every field needed to register a declaration draft.
// Money is integer minor units of invoice_currency; rates are basis points.
type CreateRequest struct {
	RequestID            string          `json:"request_id"`
	DeclarationRef       string          `json:"declaration_ref"`
	UCR                  string          `json:"ucr,omitempty"`
	DeclarationType      DeclarationType `json:"declaration_type"`
	HSCode               string          `json:"hs_code"`
	GoodsDescription     string          `json:"goods_description"`
	CountryOfOrigin      string          `json:"country_of_origin"`
	CountryOfDestination string          `json:"country_of_destination,omitempty"`
	PortOfEntry          string          `json:"port_of_entry"`
	GrossWeightKg        int64           `json:"gross_weight_kg"`
	NetWeightKg          int64           `json:"net_weight_kg"`
	NumberOfPackages     int             `json:"number_of_packages"`
	ConsigneeID          string          `json:"consignee_id"`
	OperatorID           string          `json:"operator_id"`
	IsAEO                bool            `json:"is_aeo,omitempty"`
	InvoiceAmountMinor   int64           `json:"invoice_amount_minor"`
	FreightAmountMinor   int64           `json:"freight_amount_minor,omitempty"`
	InsuranceAmountMinor int64           `json:"insurance_amount_minor,omitempty"`
	InvoiceCurrency      string          `json:"invoice_currency"`
	TariffBPS            int             `json:"tariff_bps"`
	VatBPS               int             `json:"vat_bps"`
	LevyBPS              int             `json:"levy_bps,omitempty"`
	ExciseBPS            int             `json:"excise_bps,omitempty"`
}

// Validate enforces the declaration business invariants, including the WCO
// HS-code format rule. Dots and spaces are stripped before validation, per
// the ported rule.
func (request CreateRequest) Validate() error {
	if len(request.RequestID) < 8 || len(request.RequestID) > 128 || request.RequestID != strings.TrimSpace(request.RequestID) {
		return errors.New("request_id must be canonical text between 8 and 128 characters")
	}
	if !declarationRefPattern.MatchString(request.DeclarationRef) {
		return errors.New("declaration_ref is invalid")
	}
	if request.UCR != "" && (len(request.UCR) < 8 || len(request.UCR) > 64) {
		return errors.New("ucr must be between 8 and 64 characters")
	}
	switch request.DeclarationType {
	case TypeImport, TypeExport, TypeTransit, TypeReExport, TypeTemporaryImport:
	default:
		return errors.New("declaration_type must be IMPORT, EXPORT, TRANSIT, RE_EXPORT or TEMPORARY_IMPORT")
	}
	if _, err := NormalizeHSCode(request.HSCode); err != nil {
		return err
	}
	if len(request.GoodsDescription) < 10 || len(request.GoodsDescription) > 4096 {
		return errors.New("goods_description must be between 10 and 4096 characters")
	}
	if !countryPattern.MatchString(request.CountryOfOrigin) {
		return errors.New("country_of_origin must be an ISO-2 uppercase code")
	}
	if request.CountryOfDestination != "" && !countryPattern.MatchString(request.CountryOfDestination) {
		return errors.New("country_of_destination must be an ISO-2 uppercase code")
	}
	if len(request.PortOfEntry) < 2 || len(request.PortOfEntry) > 64 {
		return errors.New("port_of_entry must be between 2 and 64 characters")
	}
	if request.GrossWeightKg <= 0 {
		return errors.New("gross_weight_kg must be positive")
	}
	if request.NetWeightKg <= 0 || request.NetWeightKg > request.GrossWeightKg {
		return errors.New("net_weight_kg must be positive and no greater than gross_weight_kg")
	}
	if request.NumberOfPackages <= 0 {
		return errors.New("number_of_packages must be positive")
	}
	if len(request.ConsigneeID) < 2 || len(request.ConsigneeID) > 128 || request.ConsigneeID != strings.TrimSpace(request.ConsigneeID) {
		return errors.New("consignee_id must be canonical text between 2 and 128 characters")
	}
	if len(request.OperatorID) < 2 || len(request.OperatorID) > 128 || request.OperatorID != strings.TrimSpace(request.OperatorID) {
		return errors.New("operator_id must be canonical text between 2 and 128 characters")
	}
	if request.InvoiceAmountMinor <= 0 {
		return errors.New("invoice_amount_minor must be positive")
	}
	if request.FreightAmountMinor < 0 || request.InsuranceAmountMinor < 0 {
		return errors.New("freight and insurance amounts must be non-negative")
	}
	if !currencyPattern.MatchString(request.InvoiceCurrency) {
		return errors.New("invoice_currency must be an ISO-3 uppercase code")
	}
	for name, bps := range map[string]int{
		"tariff_bps": request.TariffBPS, "vat_bps": request.VatBPS,
		"levy_bps": request.LevyBPS, "excise_bps": request.ExciseBPS,
	} {
		if bps < 0 || bps > 10000 {
			return errors.New(name + " must be between 0 and 10000 basis points")
		}
	}
	return nil
}

// Declaration is one revision of a customs declaration. Amendments create a
// new row under the same DeclarationRef with Revision+1; the superseded row
// keeps its full audit state.
type Declaration struct {
	DeclarationID        string          `json:"declaration_id"`
	TenantID             string          `json:"tenant_id"`
	RequestID            string          `json:"request_id"`
	DeclarationRef       string          `json:"declaration_ref"`
	UCR                  *string         `json:"ucr,omitempty"`
	Revision             int             `json:"revision"`
	SupersedesID         *string         `json:"supersedes_id,omitempty"`
	TraderID             string          `json:"trader_id"`
	DeclarationType      DeclarationType `json:"declaration_type"`
	Status               Status          `json:"status"`
	RiskLane             *RiskLane       `json:"risk_lane,omitempty"`
	RiskScore            *int            `json:"risk_score,omitempty"`
	ScoringModel         *string         `json:"scoring_model,omitempty"`
	ScoringError         *string         `json:"scoring_error,omitempty"`
	HSCode               string          `json:"hs_code"`
	GoodsDescription     string          `json:"goods_description"`
	CountryOfOrigin      string          `json:"country_of_origin"`
	CountryOfDestination *string         `json:"country_of_destination,omitempty"`
	PortOfEntry          string          `json:"port_of_entry"`
	GrossWeightKg        int64           `json:"gross_weight_kg"`
	NetWeightKg          int64           `json:"net_weight_kg"`
	NumberOfPackages     int             `json:"number_of_packages"`
	ConsigneeID          string          `json:"consignee_id"`
	OperatorID           string          `json:"operator_id"`
	IsAEO                bool            `json:"is_aeo"`
	InvoiceAmountMinor   int64           `json:"invoice_amount_minor"`
	FreightAmountMinor   int64           `json:"freight_amount_minor"`
	InsuranceAmountMinor int64           `json:"insurance_amount_minor"`
	InvoiceCurrency      string          `json:"invoice_currency"`
	TariffBPS            int             `json:"tariff_bps"`
	VatBPS               int             `json:"vat_bps"`
	LevyBPS              int             `json:"levy_bps"`
	ExciseBPS            int             `json:"excise_bps"`
	DutyMinor            *int64          `json:"duty_minor,omitempty"`
	VatMinor             *int64          `json:"vat_minor,omitempty"`
	LevyMinor            *int64          `json:"levy_minor,omitempty"`
	ExciseMinor          *int64          `json:"excise_minor,omitempty"`
	TotalDutyMinor       *int64          `json:"total_duty_minor,omitempty"`
	RejectionReason      *string         `json:"rejection_reason,omitempty"`
	SubmittedAt          *time.Time      `json:"submitted_at,omitempty"`
	ClearedAt            *time.Time      `json:"cleared_at,omitempty"`
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
	Version              int64           `json:"version"`
}

// Permit is an OGA (Other Government Agency) permit routed against a
// declaration; the model port of the singlewindow oga_permits table.
type Permit struct {
	PermitID      string     `json:"permit_id"`
	DeclarationID string     `json:"declaration_id"`
	AgencyCode    string     `json:"agency_code"`
	AgencyName    string     `json:"agency_name"`
	PermitType    *string    `json:"permit_type,omitempty"`
	PermitNumber  *string    `json:"permit_number,omitempty"`
	Status        string     `json:"status"`
	SLADeadline   *time.Time `json:"sla_deadline,omitempty"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	RespondedAt   *time.Time `json:"responded_at,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

// Permit lifecycle states.
const (
	PermitPending   = "PENDING"
	PermitApproved  = "APPROVED"
	PermitRejected  = "REJECTED"
	PermitExpired   = "EXPIRED"
	PermitSuspended = "SUSPENDED"
)

// ClearanceCertificate is the tamper-evident release document issued
// atomically with the CLEARED transition.
type ClearanceCertificate struct {
	CertificateID     string    `json:"certificate_id"`
	DeclarationID     string    `json:"declaration_id"`
	CertificateNumber string    `json:"certificate_number"`
	IssuedBy          string    `json:"issued_by"`
	PayloadSHA256     string    `json:"payload_sha256"`
	IssuedAt          time.Time `json:"issued_at"`
}

// Principal describes the verified actor behind a declaration mutation; it
// becomes the provenance block of every emitted platform event.
type Principal struct {
	ID   string
	Role string
}

// Lifecycle event types on trade.declarations.v1.
const (
	EventSubmitted          = "trade.declaration.submitted.v1"
	EventRiskAssessed       = "trade.declaration.risk-assessed.v1"
	EventScoringUnavailable = "trade.declaration.scoring-unavailable.v1"
	EventCleared            = "trade.declaration.cleared.v1"
	EventRejected           = "trade.declaration.rejected.v1"
	EventAmended            = "trade.declaration.amended.v1"
)
