package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"go.temporal.io/sdk/workflow"
)

const (
	// SignalArrivalConfirmed carries the gate id that confirmed the arrival.
	SignalArrivalConfirmed = "arrival-confirmed"
	// QueryCallUpObserver exposes a read-only call-up progress snapshot.
	QueryCallUpObserver = "callup-observer"
)

// CallUpWorkflowInput starts one workflow instance per queue request
// (workflow id = ecallup-callup-<queue request id>, so restarts are
// idempotent).
type CallUpWorkflowInput struct {
	QueueRequestID string
	TenantID       string
	PrincipalID    string
	TerminalID     string
	GraceDeadline  time.Time
}

type CallUpWorkflowResult struct {
	QueueRequestID string `json:"queue_request_id"`
	Outcome        string `json:"outcome"` // ARRIVED or FORFEITED
}

// CallUpObserverState is the queryable call-up progress snapshot.
type CallUpObserverState struct {
	QueueRequestID string `json:"queue_request_id"`
	Stage          string `json:"stage"`
	GateID         string `json:"gate_id,omitempty"`
	UpdatedAt      string `json:"updated_at"`
}

type arrivalConfirmedSignal struct {
	GateID string `json:"gate_id"`
}

// CallUpActivities wraps the side-effecting call-up steps. The store is
// mandatory — activities fail closed without it.
type CallUpActivities struct {
	Store *Store
}

func NewCallUpActivities(store *Store) (*CallUpActivities, error) {
	if store == nil {
		return nil, errors.New("call-up activities require a queue store")
	}
	return &CallUpActivities{Store: store}, nil
}

// callUpActivityCtx rebuilds the tenant context for activity executions,
// which run outside the request middleware chain.
func callUpActivityCtx(ctx context.Context, tenantID, principalID string) (context.Context, error) {
	return tenantctx.WithClaims(ctx, tenantctx.Claims{
		Issuer:   "ecallup-callup-workflow",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  principalID,
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
}

func callUpPrincipal(input CallUpWorkflowInput) Principal {
	return Principal{ID: input.PrincipalID, Role: "callup-workflow"}
}

// ArrivalCheck verifies that the arrival signalled into the workflow was
// actually persisted by the arrive path before the workflow closes.
func (activities *CallUpActivities) ArrivalCheck(ctx context.Context, input CallUpWorkflowInput, gateID string) error {
	activityCtx, err := callUpActivityCtx(ctx, input.TenantID, input.PrincipalID)
	if err != nil {
		return err
	}
	request, err := activities.Store.Get(activityCtx, input.QueueRequestID)
	if err != nil {
		return fmt.Errorf("arrival check load: %w", err)
	}
	if request.Status != StatusArrived || request.GateID == nil || *request.GateID != gateID {
		return errors.New("arrival is not persisted for this queue request")
	}
	return nil
}

// ForfeitCallUp fails the call-up closed when the grace window passes without
// an arrival, and chains the next-in-queue promotion. Already-arrived or
// cancelled requests make the activity a no-op (idempotent timer races).
func (activities *CallUpActivities) ForfeitCallUp(ctx context.Context, input CallUpWorkflowInput) error {
	activityCtx, err := callUpActivityCtx(ctx, input.TenantID, input.PrincipalID)
	if err != nil {
		return err
	}
	request, err := activities.Store.Get(activityCtx, input.QueueRequestID)
	if err != nil {
		return fmt.Errorf("forfeit load: %w", err)
	}
	if request.Status != StatusCalledUp && request.Status != StatusEnRoute {
		return nil
	}
	_, _, err = activities.Store.Forfeit(activityCtx, input.QueueRequestID, "call-up grace window elapsed", callUpPrincipal(input))
	return err
}

// ECallUpCallUpWorkflow supervises one call-up: it waits for the gate
// arrival signal until the grace deadline, then forfeits and promotes the
// next in queue.
func ECallUpCallUpWorkflow(ctx workflow.Context, input CallUpWorkflowInput) (CallUpWorkflowResult, error) {
	if input.QueueRequestID == "" || input.TenantID == "" {
		return CallUpWorkflowResult{}, errors.New("queue request id and tenant id are required")
	}
	options := workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
	}
	ctx = workflow.WithActivityOptions(ctx, options)
	var activities *CallUpActivities

	state := CallUpObserverState{QueueRequestID: input.QueueRequestID, Stage: "AWAITING_ARRIVAL", UpdatedAt: workflow.Now(ctx).UTC().Format(time.RFC3339)}
	if err := workflow.SetQueryHandler(ctx, QueryCallUpObserver, func() (CallUpObserverState, error) {
		return state, nil
	}); err != nil {
		return CallUpWorkflowResult{}, err
	}
	update := func(stage string) {
		state.Stage = stage
		state.UpdatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
	}

	var arrival arrivalConfirmedSignal
	selector := workflow.NewSelector(ctx)
	arrivalChannel := workflow.GetSignalChannel(ctx, SignalArrivalConfirmed)
	selector.AddReceive(arrivalChannel, func(channel workflow.ReceiveChannel, _ bool) {
		channel.Receive(ctx, &arrival)
	})
	selector.AddFuture(workflow.NewTimer(ctx, time.Until(input.GraceDeadline)), func(workflow.Future) {})
	selector.Select(ctx)

	if arrival.GateID == "" {
		update("GRACE_WINDOW_ELAPSED")
		if err := workflow.ExecuteActivity(ctx, activities.ForfeitCallUp, input).Get(ctx, nil); err != nil {
			return CallUpWorkflowResult{}, fmt.Errorf("forfeit call-up: %w", err)
		}
		return CallUpWorkflowResult{QueueRequestID: input.QueueRequestID, Outcome: string(StatusForfeited)}, nil
	}
	state.GateID = arrival.GateID
	update("ARRIVAL_CHECK")
	if err := workflow.ExecuteActivity(ctx, activities.ArrivalCheck, input, arrival.GateID).Get(ctx, nil); err != nil {
		return CallUpWorkflowResult{}, fmt.Errorf("arrival check: %w", err)
	}
	update("ARRIVED")
	return CallUpWorkflowResult{QueueRequestID: input.QueueRequestID, Outcome: string(StatusArrived)}, nil
}
