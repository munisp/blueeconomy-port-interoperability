package securechain

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"go.temporal.io/sdk/workflow"
)

// ExpiryWorkflowID prefixes the per-tenant expiry sweep workflow.
const ExpiryWorkflowID = "securechain-expiry-"

// ExpiryWorkflowInput drives one tenant's periodic expiry sweep.
type ExpiryWorkflowInput struct {
	TenantID string
	// SweepInterval is the delay between sweeps (workflow-side timer).
	SweepInterval time.Duration
	// Sweeps bounds the run so the workflow history stays small; the
	// scheduler restarts it (continue-as-new cadence is owned by the worker).
	Sweeps int
}

type ExpiryWorkflowResult struct {
	TenantID     string   `json:"tenant_id"`
	ExpiredChain []string `json:"expired_chains"`
}

// ExpiryActivities wraps the side-effecting expiry step. The store is
// mandatory — the activity fails closed without it.
type ExpiryActivities struct {
	Store *Store
}

func NewExpiryActivities(store *Store) (*ExpiryActivities, error) {
	if store == nil {
		return nil, errors.New("secure-chain expiry activities require a store")
	}
	return &ExpiryActivities{Store: store}, nil
}

// ExpireDue expires every ACTIVE chain of the tenant whose expiry has
// passed, revoking open links and emitting chain_expired envelopes.
func (activities *ExpiryActivities) ExpireDue(ctx context.Context, tenantID string) ([]string, error) {
	if tenantID == "" {
		return nil, errors.New("tenant id is required")
	}
	bound, err := tenantctx.WithClaims(ctx, tenantctx.Claims{
		Issuer:   "securechain-expiry-workflow",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  "booking-worker",
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		return nil, fmt.Errorf("bind tenant claims: %w", err)
	}
	return activities.Store.ExpireDue(bound, Principal{ID: "booking-worker", Role: "securechain-expiry"})
}

// SecureChainExpiryWorkflow sweeps one tenant for expired release chains on
// a timer. Each sweep is one activity, so a crash between sweeps loses at
// most one interval; already-expired chains make restarts idempotent.
func SecureChainExpiryWorkflow(ctx workflow.Context, input ExpiryWorkflowInput) (ExpiryWorkflowResult, error) {
	if input.TenantID == "" {
		return ExpiryWorkflowResult{}, errors.New("tenant id is required")
	}
	if input.SweepInterval <= 0 {
		input.SweepInterval = time.Hour
	}
	if input.Sweeps <= 0 {
		input.Sweeps = 24
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 2 * time.Minute})
	var activities *ExpiryActivities
	result := ExpiryWorkflowResult{TenantID: input.TenantID}
	for sweep := 0; sweep < input.Sweeps; sweep++ {
		var expired []string
		if err := workflow.ExecuteActivity(ctx, activities.ExpireDue, input.TenantID).Get(ctx, &expired); err != nil {
			return result, fmt.Errorf("secure-chain expiry sweep %d: %w", sweep, err)
		}
		result.ExpiredChain = append(result.ExpiredChain, expired...)
		if sweep+1 < input.Sweeps {
			if err := workflow.Sleep(ctx, input.SweepInterval); err != nil {
				return result, err
			}
		}
	}
	return result, nil
}
