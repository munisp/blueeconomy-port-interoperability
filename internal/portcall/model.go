package portcall

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/imonumber"
)

type Status string

const (
	StatusDraft     Status = "DRAFT"
	StatusSubmitted Status = "SUBMITTED"
	StatusAccepted  Status = "ACCEPTED"
	StatusRejected  Status = "REJECTED"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with retained port call")
	ErrNotFound            = errors.New("port call not found")
	ErrInvalidTransition   = errors.New("invalid port call transition")
	ErrOptimisticConflict  = errors.New("port call changed concurrently")
	ErrDocumentConflict    = errors.New("document declaration conflicts with retained evidence")
	ErrClearanceConflict   = errors.New("clearance decision conflicts with retained decision")
	ErrClearanceInvalid    = errors.New("clearance decision is not valid for this port call")
)

var portCodePattern = regexp.MustCompile(`^[A-Z]{2,8}$`)

type CreateRequest struct {
	CallID               string `json:"call_id"`
	VesselIMO            string `json:"vessel_imo"`
	PortCode             string `json:"port_code"`
	DeclarationRef       string `json:"declaration_reference"`
	SubmittedBy          string `json:"submitted_by"`
	AgencyProfileID      string `json:"agency_profile_id"`
	AgencyProfileVersion string `json:"agency_profile_version"`
}

type AgencyProfileRegistration struct {
	ProfileID     string `json:"profile_id"`
	Version       string `json:"version"`
	AgencyCode    string `json:"agency_code"`
	ProfileSHA256 string `json:"profile_sha256"`
	RegisteredBy  string `json:"registered_by"`
	Active        bool   `json:"active"`
}

type DocumentStatus string

type ClearanceDecision string

const (
	DocumentDeclared  DocumentStatus    = "DECLARED"
	DocumentVerified  DocumentStatus    = "VERIFIED"
	DocumentRejected  DocumentStatus    = "REJECTED"
	ClearanceApproved ClearanceDecision = "APPROVED"
	ClearanceRejected ClearanceDecision = "REJECTED"
)

type DocumentDeclarationRequest struct {
	DocumentType string `json:"document_type"`
	MediaType    string `json:"media_type"`
	SizeBytes    int64  `json:"size_bytes"`
	SHA256       string `json:"sha256"`
	DeclaredBy   string `json:"declared_by"`
}

type DocumentReviewRequest struct {
	ExpectedVersion int64          `json:"expected_version"`
	Status          DocumentStatus `json:"status"`
	ReviewedBy      string         `json:"reviewed_by"`
	Reason          string         `json:"reason"`
}

type DocumentDeclaration struct {
	DocumentID string `json:"document_id"`
	CallID     string `json:"call_id"`
	DocumentDeclarationRequest
	Status         DocumentStatus `json:"status"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	Version        int64          `json:"version"`
	ReviewedBy     *string        `json:"reviewed_by,omitempty"`
	ReviewedReason *string        `json:"reviewed_reason,omitempty"`
	ReviewedAt     *time.Time     `json:"reviewed_at,omitempty"`
}

type PortCall struct {
	CreateRequest
	Status    Status    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Version   int64     `json:"version"`
}

func (request DocumentDeclarationRequest) Validate() error {
	for name, value := range map[string]string{"document_type": request.DocumentType, "media_type": request.MediaType, "sha256": request.SHA256, "declared_by": request.DeclaredBy} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 256 {
			return fmt.Errorf("%s must be canonical non-empty text of at most 256 characters", name)
		}
	}
	if !regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._:-]{0,63}$`).MatchString(request.DocumentType) {
		return errors.New("document_type is invalid")
	}
	if !regexp.MustCompile(`^[A-Za-z0-9.+-]+/[A-Za-z0-9.+-]+$`).MatchString(request.MediaType) {
		return errors.New("media_type is invalid")
	}
	if request.SizeBytes <= 0 || request.SizeBytes > 104857600 {
		return errors.New("size_bytes must be between 1 and 104857600")
	}
	if !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(request.SHA256) {
		return errors.New("sha256 must be a lowercase sha256 digest")
	}
	return nil
}

func (request CreateRequest) Validate() error {
	fields := map[string]string{
		"call_id":                request.CallID,
		"vessel_imo":             request.VesselIMO,
		"port_code":              request.PortCode,
		"declaration_reference":  request.DeclarationRef,
		"submitted_by":           request.SubmittedBy,
		"agency_profile_id":      request.AgencyProfileID,
		"agency_profile_version": request.AgencyProfileVersion,
	}
	for name, value := range fields {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 256 {
			return fmt.Errorf("%s must be canonical non-empty text of at most 256 characters", name)
		}
	}
	if !imonumber.Valid(request.VesselIMO) {
		return errors.New("vessel_imo must be a seven-digit IMO number with a valid check digit")
	}
	if !portCodePattern.MatchString(request.PortCode) {
		return errors.New("port_code must contain two to eight uppercase letters")
	}
	return nil
}

func (call PortCall) Matches(request CreateRequest) bool {
	return call.CallID == request.CallID && call.VesselIMO == request.VesselIMO &&
		call.PortCode == request.PortCode && call.DeclarationRef == request.DeclarationRef &&
		call.SubmittedBy == request.SubmittedBy && call.AgencyProfileID == request.AgencyProfileID && call.AgencyProfileVersion == request.AgencyProfileVersion
}

func ValidTransition(current, next Status) bool {
	switch current {
	case StatusDraft:
		return next == StatusSubmitted || next == StatusRejected
	case StatusSubmitted:
		return next == StatusAccepted || next == StatusRejected
	default:
		return false
	}
}

type DocumentSupersessionRequest struct {
	OriginalDocumentID    string `json:"original_document_id"`
	ReplacementDocumentID string `json:"replacement_document_id"`
	Reason                string `json:"reason"`
	SupersededBy          string `json:"superseded_by"`
}

type ClearanceAmendmentRequest struct {
	ExpectedVersion int64             `json:"expected_version"`
	Decision        ClearanceDecision `json:"decision"`
	Reason          string            `json:"reason"`
	AmendedBy       string            `json:"amended_by"`
}

func validateWorkflowText(value string, max int) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= max
}

type PartnerCapabilities struct {
	Service                    string              `json:"service"`
	APIVersion                 string              `json:"api_version"`
	ImplementedOperations      []string            `json:"implemented_operations"`
	SupportedDocumentStatus    []DocumentStatus    `json:"supported_document_status"`
	SupportedClearanceDecision []ClearanceDecision `json:"supported_clearance_decision"`
	ExternalProfileRequired    bool                `json:"external_profile_required"`
}
