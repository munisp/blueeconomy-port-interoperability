package booking

import (
	"testing"
	"time"
)

func TestStateMachineAllowsHappyPath(t *testing.T) {
	path := []Status{StatusDrafted, StatusSlotReserved, StatusPaid, StatusGateApproved, StatusCompleted}
	for index := 0; index+1 < len(path); index++ {
		if !ValidTransition(path[index], path[index+1]) {
			t.Fatalf("transition %s -> %s must be allowed", path[index], path[index+1])
		}
	}
}

func TestStateMachineOfflineSyncPaths(t *testing.T) {
	allowed := [][2]Status{
		{StatusPendingSync, StatusSlotReserved},
		{StatusPendingSync, StatusReconciliationRequired},
		{StatusPendingSync, StatusCancelled},
		{StatusReconciliationRequired, StatusSlotReserved},
		{StatusReconciliationRequired, StatusCancelled},
	}
	for _, pair := range allowed {
		if !ValidTransition(pair[0], pair[1]) {
			t.Fatalf("transition %s -> %s must be allowed", pair[0], pair[1])
		}
	}
}

func TestStateMachineRejectsViolations(t *testing.T) {
	violations := [][2]Status{
		{StatusDrafted, StatusPaid},                // cannot pay before a slot is reserved
		{StatusDrafted, StatusGateApproved},        // gate without payment
		{StatusSlotReserved, StatusGateApproved},   // gate before payment
		{StatusPaid, StatusSlotReserved},           // no backward moves
		{StatusCompleted, StatusCancelled},         // terminal state
		{StatusCancelled, StatusDrafted},           // terminal state
		{StatusExpired, StatusPaid},                // terminal state
		{StatusGateApproved, StatusPaid},           // no backward moves
		{StatusGateApproved, StatusCancelled},      // approved entry cannot vanish
		{StatusPendingSync, StatusPaid},            // offline booking must reconcile first
		{StatusPendingSync, StatusGateApproved},    // offline booking must never reach the gate directly
		{StatusReconciliationRequired, StatusPaid}, // conflict must be resolved first
		{StatusReconciliationRequired, StatusGateApproved},
	}
	for _, pair := range violations {
		if ValidTransition(pair[0], pair[1]) {
			t.Fatalf("transition %s -> %s must be prohibited (fail-closed)", pair[0], pair[1])
		}
	}
}

func TestStateMachineHasNoSelfTransitions(t *testing.T) {
	for _, status := range []Status{StatusDrafted, StatusPendingSync, StatusSlotReserved, StatusPaid, StatusGateApproved, StatusCompleted, StatusCancelled, StatusExpired, StatusReconciliationRequired} {
		if ValidTransition(status, status) {
			t.Fatalf("self transition %s -> %s must be prohibited", status, status)
		}
	}
}

func TestCreateRequestValidation(t *testing.T) {
	valid := CreateRequest{
		RequestID:     "req-0001-abcd",
		TruckPlate:    "LAG-123-XY",
		TruckerMSISDN: "+2348012345678",
		TerminalID:    "APAPA-T1",
		Channel:       ChannelWeb,
		AmountKobo:    250000,
		ExpiresAt:     time.Now().Add(2 * time.Hour),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	mutations := []func(*CreateRequest){
		func(r *CreateRequest) { r.RequestID = "short" },
		func(r *CreateRequest) { r.TruckPlate = "lowercase-plate!" },
		func(r *CreateRequest) { r.TruckerMSISDN = "08012345678" },
		func(r *CreateRequest) { r.TerminalID = "1lower" },
		func(r *CreateRequest) { r.Channel = "SMOKE" },
		func(r *CreateRequest) { r.AmountKobo = 0 },
		func(r *CreateRequest) { r.ExpiresAt = time.Now().Add(-time.Hour) },
	}
	invalid := make([]CreateRequest, 0, len(mutations))
	for _, mutate := range mutations {
		request := valid
		mutate(&request)
		invalid = append(invalid, request)
	}
	for index, request := range invalid {
		if err := request.Validate(); err == nil {
			t.Fatalf("invalid request %d was accepted", index)
		}
	}
}
