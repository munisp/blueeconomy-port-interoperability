package customs

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/munisp/blueeconomy-port-interoperability/internal/declarations"
)

// DeclarationLookup is the read side of the declaration engine the local
// validator needs. declarations.Store implements it.
type DeclarationLookup interface {
	HeadByRef(ctx context.Context, declarationRef string) (declarations.Declaration, error)
}

// LocalValidator validates bookings against the platform's own declaration
// engine (internal/declarations) instead of the external HTTPS surface. It is
// the same fail-closed contract as HTTPValidator: a missing declaration is
// ErrDeclarationNotFound (a rule mismatch); any storage failure means the
// validator is unreachable and the booking must stay VALIDATION_PENDING.
type LocalValidator struct {
	lookup DeclarationLookup
}

// NewLocalValidator wires the local backend; a missing lookup fails closed.
func NewLocalValidator(lookup DeclarationLookup) (*LocalValidator, error) {
	if lookup == nil {
		return nil, errors.New("local customs validator requires the declaration store")
	}
	return &LocalValidator{lookup: lookup}, nil
}

// Declaration resolves the live revision of the declaration ref and maps it
// onto the cross-validation surface contract. A CLEARED declaration is VALID;
// every other lifecycle state is surfaced verbatim so Evaluate rejects it
// with DECLARATION_STATUS_INVALID — exact reason-code parity with the HTTP
// backend.
func (validator *LocalValidator) Declaration(ctx context.Context, declarationRef string) (Declaration, error) {
	if strings.TrimSpace(declarationRef) == "" {
		return Declaration{}, errors.New("declaration reference is required")
	}
	declaration, err := validator.lookup.HeadByRef(ctx, declarationRef)
	if errors.Is(err, declarations.ErrNotFound) {
		return Declaration{}, ErrDeclarationNotFound
	}
	if err != nil {
		return Declaration{}, fmt.Errorf("local declaration lookup: %w", err)
	}
	status := string(declaration.Status)
	if declaration.Status == declarations.StatusCleared {
		status = declarationStatusValid
	}
	return Declaration{
		DeclarationRef: declaration.DeclarationRef,
		Status:         status,
		WeightKg:       declaration.GrossWeightKg,
		ConsigneeID:    declaration.ConsigneeID,
		OperatorID:     declaration.OperatorID,
	}, nil
}
