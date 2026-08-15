package portcall

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
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
)

var portCodePattern = regexp.MustCompile(`^[A-Z]{2,8}$`)
var imoPattern = regexp.MustCompile(`^[0-9]{7}$`)

type CreateRequest struct {
	CallID         string `json:"call_id"`
	VesselIMO      string `json:"vessel_imo"`
	PortCode       string `json:"port_code"`
	DeclarationRef string `json:"declaration_reference"`
	SubmittedBy    string `json:"submitted_by"`
}

type PortCall struct {
	CreateRequest
	Status    Status    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Version   int64     `json:"version"`
}

func (request CreateRequest) Validate() error {
	fields := map[string]string{
		"call_id":               request.CallID,
		"vessel_imo":            request.VesselIMO,
		"port_code":             request.PortCode,
		"declaration_reference": request.DeclarationRef,
		"submitted_by":          request.SubmittedBy,
	}
	for name, value := range fields {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 256 {
			return fmt.Errorf("%s must be canonical non-empty text of at most 256 characters", name)
		}
	}
	if !imoPattern.MatchString(request.VesselIMO) {
		return errors.New("vessel_imo must be exactly seven digits")
	}
	if !portCodePattern.MatchString(request.PortCode) {
		return errors.New("port_code must contain two to eight uppercase letters")
	}
	return nil
}

func (call PortCall) Matches(request CreateRequest) bool {
	return call.CallID == request.CallID && call.VesselIMO == request.VesselIMO &&
		call.PortCode == request.PortCode && call.DeclarationRef == request.DeclarationRef &&
		call.SubmittedBy == request.SubmittedBy
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
