package booking

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
)

// Orchestrator is the server's boundary to the Temporal booking workflow.
type Orchestrator interface {
	StartBookingWorkflow(ctx context.Context, input WorkflowInput) error
	SignalPaymentConfirmed(ctx context.Context, bookingID, receiptRef string) error
	SignalGateScan(ctx context.Context, bookingID, scanID string) error
	ObserverState(ctx context.Context, bookingID string) (ObserverState, error)
}

type TemporalOrchestrator struct {
	client    client.Client
	taskQueue string
}

// NewTemporalOrchestrator dials Temporal. Address, namespace and task queue
// are mandatory — the orchestration path fails closed when unconfigured.
func NewTemporalOrchestrator(address, namespace, taskQueue string) (*TemporalOrchestrator, error) {
	if strings.TrimSpace(address) == "" || strings.TrimSpace(namespace) == "" || strings.TrimSpace(taskQueue) == "" {
		return nil, errors.New("TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE and TEMPORAL_TASK_QUEUE are required")
	}
	// Temporal OTel client interceptor: workflow starts carry the caller's
	// trace context into the workflow (no-op when telemetry is disabled).
	tracingInterceptor, err := temporalotel.NewTracingInterceptor(temporalotel.TracerOptions{})
	if err != nil {
		return nil, fmt.Errorf("build temporal tracing interceptor: %w", err)
	}
	temporalClient, err := client.Dial(client.Options{
		HostPort:     address,
		Namespace:    namespace,
		Interceptors: []interceptor.ClientInterceptor{tracingInterceptor},
	})
	if err != nil {
		return nil, fmt.Errorf("dial temporal: %w", err)
	}
	return &TemporalOrchestrator{client: temporalClient, taskQueue: taskQueue}, nil
}

func (orchestrator *TemporalOrchestrator) Close() {
	orchestrator.client.Close()
}

func (orchestrator *TemporalOrchestrator) StartBookingWorkflow(ctx context.Context, input WorkflowInput) error {
	_, err := orchestrator.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "ecallup-booking-" + input.BookingID,
		TaskQueue: orchestrator.taskQueue,
	}, ECallUpBookingWorkflow, input)
	if err != nil {
		if temporal.IsWorkflowExecutionAlreadyStartedError(err) {
			return nil // idempotent start: workflow id equals booking id
		}
		return fmt.Errorf("start booking workflow: %w", err)
	}
	return nil
}

func (orchestrator *TemporalOrchestrator) SignalPaymentConfirmed(ctx context.Context, bookingID, receiptRef string) error {
	if receiptRef == "" {
		return errors.New("receipt reference is required")
	}
	return orchestrator.client.SignalWorkflow(ctx, "ecallup-booking-"+bookingID, "", SignalPaymentConfirmed, paymentConfirmedSignal{ReceiptRef: receiptRef})
}

func (orchestrator *TemporalOrchestrator) SignalGateScan(ctx context.Context, bookingID, scanID string) error {
	if scanID == "" {
		return errors.New("scan id is required")
	}
	return orchestrator.client.SignalWorkflow(ctx, "ecallup-booking-"+bookingID, "", SignalGateScan, gateScanSignal{ScanID: scanID})
}

func (orchestrator *TemporalOrchestrator) ObserverState(ctx context.Context, bookingID string) (ObserverState, error) {
	response, err := orchestrator.client.QueryWorkflow(ctx, "ecallup-booking-"+bookingID, "", QueryObserver)
	if err != nil {
		return ObserverState{}, fmt.Errorf("query booking workflow: %w", err)
	}
	var state ObserverState
	if err := response.Get(&state); err != nil {
		return ObserverState{}, fmt.Errorf("decode observer state: %w", err)
	}
	return state, nil
}
