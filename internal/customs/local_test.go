package customs

import (
	"context"
	"errors"
	"testing"

	"github.com/munisp/blueeconomy-port-interoperability/internal/declarations"
)

// staticLookup is a test double for the declaration engine read side.
type staticLookup struct {
	declaration declarations.Declaration
	err         error
}

func (lookup staticLookup) HeadByRef(context.Context, string) (declarations.Declaration, error) {
	return lookup.declaration, lookup.err
}

func localDeclaration(status declarations.Status) declarations.Declaration {
	return declarations.Declaration{
		DeclarationID:  "6f4b8c1e-0000-4000-8000-000000000001",
		DeclarationRef: "NCS-2026-ABC123",
		Status:         status,
		GrossWeightKg:  10000,
		ConsigneeID:    "consignee-dangote-01",
		OperatorID:     "operator-apapa-01",
	}
}

func TestNewLocalValidatorFailsClosed(t *testing.T) {
	if _, err := NewLocalValidator(nil); err == nil {
		t.Fatal("a missing declaration lookup must fail closed")
	}
}

func TestLocalValidatorMapsNotFoundAndOutage(t *testing.T) {
	validator, err := NewLocalValidator(staticLookup{err: declarations.ErrNotFound})
	if err != nil {
		t.Fatalf("build validator: %v", err)
	}
	if _, err := validator.Declaration(context.Background(), "NCS-MISSING"); !errors.Is(err, ErrDeclarationNotFound) {
		t.Fatalf("missing declaration = %v, want ErrDeclarationNotFound (mismatch, not outage)", err)
	}
	outage, err := NewLocalValidator(staticLookup{err: errors.New("connection refused")})
	if err != nil {
		t.Fatalf("build validator: %v", err)
	}
	if _, err := outage.Declaration(context.Background(), "NCS-2026-ABC123"); err == nil || errors.Is(err, ErrDeclarationNotFound) {
		t.Fatalf("storage failure = %v, want an unreachable-validator error, never a mismatch", err)
	}
	if _, err := validator.Declaration(context.Background(), "  "); err == nil {
		t.Fatal("blank declaration ref must be rejected")
	}
}

// TestLocalValidatorReasonCodeParity proves the local backend produces the
// exact same MATCH/MISMATCH decisions and reason codes as the HTTP backend
// for the same underlying declaration state: both feed the shared Evaluate
// rules, and a CLEARED declaration is surfaced as VALID.
func TestLocalValidatorReasonCodeParity(t *testing.T) {
	expectation := expectation()
	cases := []struct {
		name       string
		status     declarations.Status
		weightKg   int64
		consignee  string
		operator   string
		wantStatus string // what the HTTP surface would report
		decision   string
		reason     string
	}{
		{"cleared is valid", declarations.StatusCleared, 10000, "consignee-dangote-01", "operator-apapa-01", declarationStatusValid, DecisionMatch, ""},
		{"draft invalid", declarations.StatusDraft, 10000, "consignee-dangote-01", "operator-apapa-01", "DRAFT", DecisionMismatch, ReasonDeclarationInvalid},
		{"submitted invalid", declarations.StatusSubmitted, 10000, "consignee-dangote-01", "operator-apapa-01", "SUBMITTED", DecisionMismatch, ReasonDeclarationInvalid},
		{"yellow lane invalid", declarations.StatusYellowLane, 10000, "consignee-dangote-01", "operator-apapa-01", "YELLOW_LANE", DecisionMismatch, ReasonDeclarationInvalid},
		{"rejected invalid", declarations.StatusRejected, 10000, "consignee-dangote-01", "operator-apapa-01", "REJECTED", DecisionMismatch, ReasonDeclarationInvalid},
		{"scoring unavailable invalid", declarations.StatusScoringUnavailable, 10000, "consignee-dangote-01", "operator-apapa-01", "SCORING_UNAVAILABLE", DecisionMismatch, ReasonDeclarationInvalid},
		{"weight tolerance", declarations.StatusCleared, 11000, "consignee-dangote-01", "operator-apapa-01", declarationStatusValid, DecisionMismatch, ReasonWeightTolerance},
		{"consignee mismatch", declarations.StatusCleared, 10000, "consignee-other", "operator-apapa-01", declarationStatusValid, DecisionMismatch, ReasonConsigneeMismatch},
		{"operator mismatch", declarations.StatusCleared, 10000, "consignee-dangote-01", "operator-other", declarationStatusValid, DecisionMismatch, ReasonOperatorMismatch},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			local := localDeclaration(testCase.status)
			local.GrossWeightKg = testCase.weightKg
			local.ConsigneeID = testCase.consignee
			local.OperatorID = testCase.operator
			validator, err := NewLocalValidator(staticLookup{declaration: local})
			if err != nil {
				t.Fatalf("build validator: %v", err)
			}
			declaration, err := validator.Declaration(context.Background(), "NCS-2026-ABC123")
			if err != nil {
				t.Fatalf("local declaration: %v", err)
			}
			if declaration.Status != testCase.wantStatus {
				t.Fatalf("surfaced status = %q, want %q", declaration.Status, testCase.wantStatus)
			}
			// HTTP backend parity: the same surfaced record must reach the
			// same decision through Evaluate.
			localEvaluation := Evaluate(declaration, expectation)
			httpEvaluation := Evaluate(Declaration{
				DeclarationRef: "NCS-2026-ABC123",
				Status:         testCase.wantStatus,
				WeightKg:       testCase.weightKg,
				ConsigneeID:    testCase.consignee,
				OperatorID:     testCase.operator,
			}, expectation)
			if localEvaluation.Decision != httpEvaluation.Decision || localEvaluation.ReasonCode != httpEvaluation.ReasonCode {
				t.Fatalf("parity broken: local %s/%s vs http %s/%s",
					localEvaluation.Decision, localEvaluation.ReasonCode, httpEvaluation.Decision, httpEvaluation.ReasonCode)
			}
			if localEvaluation.Decision != testCase.decision || localEvaluation.ReasonCode != testCase.reason {
				t.Fatalf("decision = %s/%s, want %s/%s", localEvaluation.Decision, localEvaluation.ReasonCode, testCase.decision, testCase.reason)
			}
		})
	}
	// Not-found parity: both backends map a missing declaration to
	// ErrDeclarationNotFound, which the booking workflow turns into
	// DECLARATION_NOT_FOUND.
	if !errors.Is(func() error {
		validator, _ := NewLocalValidator(staticLookup{err: declarations.ErrNotFound})
		_, err := validator.Declaration(context.Background(), "NCS-MISSING")
		return err
	}(), ErrDeclarationNotFound) {
		t.Fatal("local backend must map missing declarations to ErrDeclarationNotFound")
	}
}

func TestLocalValidatorSurfacesGrossWeight(t *testing.T) {
	local := localDeclaration(declarations.StatusCleared)
	local.GrossWeightKg = 25000
	validator, err := NewLocalValidator(staticLookup{declaration: local})
	if err != nil {
		t.Fatalf("build validator: %v", err)
	}
	declaration, err := validator.Declaration(context.Background(), "NCS-2026-ABC123")
	if err != nil {
		t.Fatalf("local declaration: %v", err)
	}
	if declaration.WeightKg != 25000 || declaration.DeclarationRef != "NCS-2026-ABC123" {
		t.Fatalf("declaration = %+v", declaration)
	}
}
