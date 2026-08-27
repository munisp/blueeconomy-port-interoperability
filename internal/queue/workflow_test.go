package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func callUpInput() CallUpWorkflowInput {
	return CallUpWorkflowInput{
		QueueRequestID: "queue-request-0001",
		TenantID:       "tenant-apapa-port",
		PrincipalID:    "callup-engine",
		TerminalID:     "APAPA-T1",
		GraceDeadline:  time.Now().UTC().Add(90 * time.Minute),
	}
}

func TestCallUpWorkflowArrivesBeforeGraceDeadline(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var activities *CallUpActivities
	input := callUpInput()
	env.OnActivity(activities.ArrivalCheck, mock.Anything, input, "GATE-A").Return(nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalArrivalConfirmed, arrivalConfirmedSignal{GateID: "GATE-A"})
	}, time.Minute)
	env.RegisterDelayedCallback(func() {
		response, err := env.QueryWorkflow(QueryCallUpObserver)
		if err != nil {
			t.Errorf("observer query: %v", err)
			return
		}
		var state CallUpObserverState
		if err := response.Get(&state); err != nil {
			t.Errorf("decode observer state: %v", err)
			return
		}
		if state.QueueRequestID != input.QueueRequestID || state.Stage != "AWAITING_ARRIVAL" {
			t.Errorf("observer state = %#v, want AWAITING_ARRIVAL", state)
		}
	}, 30*time.Second)

	env.ExecuteWorkflow(ECallUpCallUpWorkflow, input)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var result CallUpWorkflowResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.Outcome != string(StatusArrived) {
		t.Fatalf("result = %#v, want ARRIVED", result)
	}
	env.AssertExpectations(t)
}

func TestCallUpWorkflowForfeitsWhenGraceWindowElapses(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var activities *CallUpActivities
	input := callUpInput()
	env.OnActivity(activities.ForfeitCallUp, mock.Anything, input).Return(nil)

	// No arrival signal: the grace-window timer fires and forfeits.
	env.ExecuteWorkflow(ECallUpCallUpWorkflow, input)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var result CallUpWorkflowResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.Outcome != string(StatusForfeited) {
		t.Fatalf("result = %#v, want FORFEITED", result)
	}
	env.AssertExpectations(t)
}

func TestCallUpWorkflowFailsWhenArrivalCheckRejects(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var activities *CallUpActivities
	input := callUpInput()
	// A forged arrival signal must fail the workflow, not retry forever.
	env.OnActivity(activities.ArrivalCheck, mock.Anything, input, "GATE-X").
		Return(temporal.NewNonRetryableApplicationError("arrival is not persisted", "ArrivalRejected", nil))

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalArrivalConfirmed, arrivalConfirmedSignal{GateID: "GATE-X"})
	}, time.Minute)

	env.ExecuteWorkflow(ECallUpCallUpWorkflow, input)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("workflow must fail when the arrival check rejects the signal")
	}
}

func TestCallUpWorkflowRejectsBlankIdentifiers(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(ECallUpCallUpWorkflow, CallUpWorkflowInput{})
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("workflow without identifiers must fail closed")
	}
}
