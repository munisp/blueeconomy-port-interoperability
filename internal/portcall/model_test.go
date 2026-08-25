package portcall

import (
	"errors"
	"testing"
)

func validCreate() CreateRequest {
	return CreateRequest{
		CallID:               "call-001",
		VesselIMO:            "1234567",
		PortCode:             "LAGOS",
		DeclarationRef:       "decl-001",
		SubmittedBy:          "agent-001",
		AgencyProfileID:      "npa-lagos",
		AgencyProfileVersion: "2026-08-16",
	}
}

func TestCreateRequestValidation(t *testing.T) {
	if err := validCreate().Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	invalid := validCreate()
	invalid.VesselIMO = "123"
	if err := invalid.Validate(); err == nil {
		t.Fatal("invalid IMO accepted")
	}
	invalid = validCreate()
	invalid.PortCode = "lagos"
	if err := invalid.Validate(); err == nil {
		t.Fatal("lowercase port code accepted")
	}
}

func TestIdempotencyMatchRequiresImmutableFields(t *testing.T) {
	request := validCreate()
	call := PortCall{CreateRequest: request}
	if !call.Matches(request) {
		t.Fatal("exact request did not match")
	}
	request.DeclarationRef = "different"
	if call.Matches(request) {
		t.Fatal("conflicting request matched")
	}
	if !errors.Is(ErrIdempotencyConflict, ErrIdempotencyConflict) {
		t.Fatal("conflict sentinel is not usable")
	}
}

func TestValidTransitions(t *testing.T) {
	for _, transition := range [][2]Status{
		{StatusDraft, StatusSubmitted},
		{StatusSubmitted, StatusAccepted},
		{StatusSubmitted, StatusRejected},
	} {
		if !ValidTransition(transition[0], transition[1]) {
			t.Fatalf("expected transition %s -> %s", transition[0], transition[1])
		}
	}
	if ValidTransition(StatusAccepted, StatusDraft) || ValidTransition(StatusRejected, StatusAccepted) {
		t.Fatal("terminal state transition accepted")
	}
}
