package queue

import (
	"testing"
)

func TestStateMachineAllowsHappyPath(t *testing.T) {
	path := []Status{StatusRequested, StatusQueued, StatusCalledUp, StatusEnRoute, StatusArrived}
	for index := 0; index+1 < len(path); index++ {
		if !ValidTransition(path[index], path[index+1]) {
			t.Fatalf("transition %s -> %s must be allowed", path[index], path[index+1])
		}
	}
}

func TestStateMachineFailClosedBranches(t *testing.T) {
	allowed := [][2]Status{
		{StatusCalledUp, StatusArrived},                 // gate scan confirms arrival directly
		{StatusCalledUp, StatusForfeited},               // grace window elapsed
		{StatusEnRoute, StatusForfeited},                // grace window elapsed on the road
		{StatusQueued, StatusExpired},                   // stale queued request
		{StatusQueued, StatusCancelled},                 // trucker/operator cancels
		{StatusRequested, StatusCancelled},              // cancel before position assignment
		{StatusQueued, StatusReconciliationRequired},    // conflict surfaced, never silent
		{StatusCalledUp, StatusReconciliationRequired},  // conflict during call-up
		{StatusReconciliationRequired, StatusQueued},    // operator re-queues at the tail
		{StatusReconciliationRequired, StatusCancelled}, // or cancels outright
	}
	for _, pair := range allowed {
		if !ValidTransition(pair[0], pair[1]) {
			t.Fatalf("transition %s -> %s must be allowed", pair[0], pair[1])
		}
	}
}

func TestStateMachineRejectsViolations(t *testing.T) {
	violations := [][2]Status{
		{StatusRequested, StatusCalledUp},              // cannot call up before queueing
		{StatusRequested, StatusArrived},               // arrival without call-up
		{StatusQueued, StatusArrived},                  // queued trucks must wait for call-up
		{StatusQueued, StatusEnRoute},                  // cannot depart before call-up
		{StatusEnRoute, StatusCalledUp},                // no backward moves
		{StatusCalledUp, StatusQueued},                 // call-up cannot be silently undone
		{StatusArrived, StatusCancelled},               // terminal state
		{StatusArrived, StatusForfeited},               // terminal state
		{StatusForfeited, StatusQueued},                // forfeiture is final
		{StatusCancelled, StatusQueued},                // terminal state
		{StatusExpired, StatusCalledUp},                // terminal state
		{StatusReconciliationRequired, StatusCalledUp}, // conflict must be resolved first
		{StatusReconciliationRequired, StatusArrived},  // conflict must never reach the gate
	}
	for _, pair := range violations {
		if ValidTransition(pair[0], pair[1]) {
			t.Fatalf("transition %s -> %s must be prohibited (fail-closed)", pair[0], pair[1])
		}
	}
}

func TestStateMachineHasNoSelfTransitions(t *testing.T) {
	for _, status := range []Status{StatusRequested, StatusQueued, StatusCalledUp, StatusEnRoute, StatusArrived, StatusCancelled, StatusExpired, StatusForfeited, StatusReconciliationRequired} {
		if ValidTransition(status, status) {
			t.Fatalf("self transition %s -> %s must be prohibited", status, status)
		}
	}
}

func TestCreateRequestValidation(t *testing.T) {
	valid := CreateRequest{
		IdempotencyKey: "queue-req-0001",
		TruckPlate:     "LAG-123-XY",
		TruckerMSISDN:  "+2348012345678",
		TerminalID:     "APAPA-T1",
		PriorityClass:  ClassStandard,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	withBooking := valid
	withBooking.BookingID = "booking-0001"
	if err := withBooking.Validate(); err != nil {
		t.Fatalf("valid request with booking rejected: %v", err)
	}
	mutations := []func(*CreateRequest){
		func(r *CreateRequest) { r.IdempotencyKey = "short" },
		func(r *CreateRequest) { r.IdempotencyKey = " padded-key " },
		func(r *CreateRequest) { r.TruckPlate = "lowercase-plate!" },
		func(r *CreateRequest) { r.TruckerMSISDN = "08012345678" },
		func(r *CreateRequest) { r.TerminalID = "1lower" },
		func(r *CreateRequest) { r.PriorityClass = "VIP" },
	}
	for index, mutate := range mutations {
		request := valid
		mutate(&request)
		if err := request.Validate(); err == nil {
			t.Fatalf("invalid request %d was accepted", index)
		}
	}
}

func TestPriorityClassRanking(t *testing.T) {
	if !(ClassPerishable.rank() < ClassPriority.rank() && ClassPriority.rank() < ClassStandard.rank()) {
		t.Fatal("priority classes must rank PERISHABLE < PRIORITY < STANDARD")
	}
}
