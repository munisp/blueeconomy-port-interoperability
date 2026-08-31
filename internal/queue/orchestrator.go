package queue

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

// CallUpOrchestrator is the server's boundary to the Temporal call-up
// workflow.
type CallUpOrchestrator interface {
	StartCallUpWorkflow(ctx context.Context, input CallUpWorkflowInput) error
	SignalArrivalConfirmed(ctx context.Context, queueRequestID, gateID string) error
	CallUpObserverState(ctx context.Context, queueRequestID string) (CallUpObserverState, error)
}

type TemporalCallUpOrchestrator struct {
	client    client.Client
	taskQueue string
}

// NewTemporalCallUpOrchestrator dials Temporal. Address, namespace and task
// queue are mandatory — the orchestration path fails closed when unconfigured.
func NewTemporalCallUpOrchestrator(address, namespace, taskQueue string) (*TemporalCallUpOrchestrator, error) {
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
	return &TemporalCallUpOrchestrator{client: temporalClient, taskQueue: taskQueue}, nil
}

func (orchestrator *TemporalCallUpOrchestrator) Close() {
	orchestrator.client.Close()
}

// StartCallUpWorkflow is idempotent: the workflow id equals the queue request
// id, so replays and sweeper passes never duplicate an execution.
func (orchestrator *TemporalCallUpOrchestrator) StartCallUpWorkflow(ctx context.Context, input CallUpWorkflowInput) error {
	_, err := orchestrator.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "ecallup-callup-" + input.QueueRequestID,
		TaskQueue: orchestrator.taskQueue,
	}, ECallUpCallUpWorkflow, input)
	if err != nil {
		if temporal.IsWorkflowExecutionAlreadyStartedError(err) {
			return nil // idempotent start: workflow id equals queue request id
		}
		return fmt.Errorf("start call-up workflow: %w", err)
	}
	return nil
}

func (orchestrator *TemporalCallUpOrchestrator) SignalArrivalConfirmed(ctx context.Context, queueRequestID, gateID string) error {
	if gateID == "" {
		return errors.New("gate id is required")
	}
	return orchestrator.client.SignalWorkflow(ctx, "ecallup-callup-"+queueRequestID, "", SignalArrivalConfirmed, arrivalConfirmedSignal{GateID: gateID})
}

func (orchestrator *TemporalCallUpOrchestrator) CallUpObserverState(ctx context.Context, queueRequestID string) (CallUpObserverState, error) {
	response, err := orchestrator.client.QueryWorkflow(ctx, "ecallup-callup-"+queueRequestID, "", QueryCallUpObserver)
	if err != nil {
		return CallUpObserverState{}, fmt.Errorf("query call-up workflow: %w", err)
	}
	var state CallUpObserverState
	if err := response.Get(&state); err != nil {
		return CallUpObserverState{}, fmt.Errorf("decode call-up observer state: %w", err)
	}
	return state, nil
}
