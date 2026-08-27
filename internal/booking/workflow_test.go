package booking

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func workflowInput() WorkflowInput {
	now := time.Now().UTC()
	return WorkflowInput{
		BookingID:        "booking-0001",
		TenantID:         "tenant-apapa-port",
		PrincipalID:      "trucker-1",
		AmountKobo:       250000,
		FgnShareKobo:     6250,
		ExpectedVersion:  2,
		PaymentDeadline:  now.Add(time.Hour),
		GateScanDeadline: now.Add(2 * time.Hour),
	}
}

func TestWorkflowCompletesAfterPaymentAndGateScanSignals(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var activities *Activities
	input := workflowInput()
	env.OnActivity(activities.ReceiptCheck, mock.Anything, input, "rcpt-0001").Return(nil)
	env.OnActivity(activities.CommitLedger, mock.Anything, input).Return("sha256:abc123", nil)
	env.OnActivity(activities.AuditCommit, mock.Anything, input, "sha256:abc123").Return(nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalPaymentConfirmed, paymentConfirmedSignal{ReceiptRef: "rcpt-0001"})
	}, time.Minute)
	env.RegisterDelayedCallback(func() {
		// The observer query must expose live progress mid-flight.
		response, err := env.QueryWorkflow(QueryObserver)
		if err != nil {
			t.Errorf("observer query: %v", err)
			return
		}
		var state ObserverState
		if err := response.Get(&state); err != nil {
			t.Errorf("decode observer state: %v", err)
			return
		}
		if state.BookingID != input.BookingID || state.Receipt != "rcpt-0001" || state.Stage != "AWAITING_GATE_SCAN" {
			t.Errorf("observer state = %#v, want AWAITING_GATE_SCAN with receipt", state)
		}
	}, 90*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalGateScan, gateScanSignal{ScanID: "scan-0001"})
	}, 2*time.Minute)

	env.ExecuteWorkflow(ECallUpBookingWorkflow, input)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var result WorkflowResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.Outcome != string(StatusCompleted) || result.LedgerCommitHash != "sha256:abc123" {
		t.Fatalf("result = %#v, want COMPLETED with ledger hash", result)
	}
	env.AssertExpectations(t)
}

func TestWorkflowExpiresWhenPaymentNeverArrives(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var activities *Activities
	input := workflowInput()
	env.OnActivity(activities.ExpireBooking, mock.Anything, input).Return(nil)

	env.ExecuteWorkflow(ECallUpBookingWorkflow, input)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var result WorkflowResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.Outcome != string(StatusExpired) {
		t.Fatalf("result = %#v, want EXPIRED", result)
	}
	env.AssertExpectations(t)
}

func TestWorkflowExpiresWhenGateScanNeverArrives(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var activities *Activities
	input := workflowInput()
	env.OnActivity(activities.ReceiptCheck, mock.Anything, input, "rcpt-0002").Return(nil)
	env.OnActivity(activities.ExpireBooking, mock.Anything, input).Return(nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalPaymentConfirmed, paymentConfirmedSignal{ReceiptRef: "rcpt-0002"})
	}, time.Minute)

	env.ExecuteWorkflow(ECallUpBookingWorkflow, input)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var result WorkflowResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.Outcome != string(StatusExpired) {
		t.Fatalf("result = %#v, want EXPIRED", result)
	}
	env.AssertExpectations(t)
}

func TestWorkflowFailsWhenReceiptCheckFails(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var activities *Activities
	input := workflowInput()
	// Non-retryable: a forged receipt must fail the workflow, not retry forever.
	env.OnActivity(activities.ReceiptCheck, mock.Anything, input, "rcpt-forged").
		Return(temporal.NewNonRetryableApplicationError("payment receipt is not persisted", "ReceiptRejected", nil))

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalPaymentConfirmed, paymentConfirmedSignal{ReceiptRef: "rcpt-forged"})
	}, time.Minute)

	env.ExecuteWorkflow(ECallUpBookingWorkflow, input)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("workflow must fail when the receipt check rejects the signal")
	}
}
