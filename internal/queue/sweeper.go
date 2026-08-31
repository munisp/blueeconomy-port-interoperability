package queue

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// sweepTenantFailures counts per-tenant sweep failures so operators can alert
// on a tenant stuck without grace-window forfeiture or queue promotion.
var sweepTenantFailures = expvar.NewMap("callup_sweep_tenant_failures")

// Sweeper runs the periodic call-up reconciliation across every active
// tenant: grace-window forfeiture (chaining next-in-queue promotions),
// backfill of freed call-up capacity and idempotent grace-window workflow
// starts. Booking and queue tables are tenant-scoped under RLS, so each
// tenant is swept inside its own tenant-bound context and one tenant's
// failure never skips the rest.
type Sweeper struct {
	pool    *pgxpool.Pool
	store   *Store
	callUps CallUpOrchestrator
	// onlyTenant optionally restricts the sweep to a single tenant
	// (WORKER_TENANT_ID); empty sweeps every active tenant.
	onlyTenant string
	// staleAfter is the maximum age of a QUEUED entry before the sweep
	// expires it; it must be positive so dead queue entries cannot park a
	// terminal's waiting list forever.
	staleAfter time.Duration
}

// NewSweeper wires the multi-tenant call-up sweeper; every dependency is
// mandatory. onlyTenant may be empty (sweep all active tenants). staleAfter
// bounds how long a QUEUED entry may wait before ExpireStale retires it.
func NewSweeper(pool *pgxpool.Pool, store *Store, callUps CallUpOrchestrator, onlyTenant string, staleAfter time.Duration) (*Sweeper, error) {
	if pool == nil || store == nil || callUps == nil {
		return nil, errors.New("call-up sweeper requires a database pool, a queue store and a call-up orchestrator")
	}
	if staleAfter <= 0 {
		return nil, errors.New("call-up sweeper requires a positive stale-entry cutoff")
	}
	return &Sweeper{pool: pool, store: store, callUps: callUps, onlyTenant: onlyTenant, staleAfter: staleAfter}, nil
}

// SweepOnce runs one reconcile + workflow-restart cycle across every active
// tenant (or only the configured tenant restriction). Tenant failures are
// isolated: each failure is logged and counted, the remaining tenants are
// still swept, and the first error is returned at the end.
func (sweeper *Sweeper) SweepOnce(ctx context.Context) error {
	tenants, err := sweeper.sweepTenants(ctx)
	if err != nil {
		return err
	}
	var firstErr error
	for _, tenantID := range tenants {
		if err := sweeper.sweepTenant(ctx, tenantID); err != nil {
			sweepTenantFailures.Add(tenantID, 1)
			log.Printf("call-up sweep tenant %s: %v", tenantID, err)
			if firstErr == nil {
				firstErr = fmt.Errorf("sweep tenant %s: %w", tenantID, err)
			}
		}
	}
	return firstErr
}

func (sweeper *Sweeper) sweepTenants(ctx context.Context) ([]string, error) {
	if sweeper.onlyTenant != "" {
		return []string{sweeper.onlyTenant}, nil
	}
	rows, err := sweeper.pool.Query(ctx, `SELECT tenant_id FROM platform_tenants WHERE active ORDER BY tenant_id`)
	if err != nil {
		return nil, fmt.Errorf("list active tenants: %w", err)
	}
	defer rows.Close()
	var tenants []string
	for rows.Next() {
		var tenantID string
		if err := rows.Scan(&tenantID); err != nil {
			return nil, fmt.Errorf("scan tenant: %w", err)
		}
		tenants = append(tenants, tenantID)
	}
	return tenants, rows.Err()
}

// sweepTenant forfeits elapsed grace windows (chaining next-in-queue
// promotions), fills freed call-up capacity and idempotently starts a
// grace-window workflow for every active call-up of one tenant. Each store
// operation runs inside its own tenant-scoped transaction (tenantdb.WithTx).
func (sweeper *Sweeper) sweepTenant(ctx context.Context, tenantID string) error {
	principal := Principal{ID: "booking-worker", Role: "callup-engine"}
	bound, err := tenantctx.WithClaims(ctx, tenantctx.Claims{
		Issuer:   "booking-worker",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  "booking-worker",
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		return fmt.Errorf("bind tenant claims: %w", err)
	}
	// Retire queue entries that waited past the stale cutoff BEFORE any
	// call-up promotion runs: a stale head-of-queue must expire, never be
	// called up. A QUEUED entry never held call-up capacity, but leaving it
	// forever parks the waiting list and misleads position reporting.
	if _, err := sweeper.store.ExpireStale(bound, time.Now().Add(-sweeper.staleAfter), principal); err != nil {
		return fmt.Errorf("expire stale queue entries: %w", err)
	}
	if _, err := sweeper.store.ReconcileCallUps(bound, principal); err != nil {
		return fmt.Errorf("reconcile call-ups: %w", err)
	}
	active, err := sweeper.store.ListActiveCallUps(bound)
	if err != nil {
		return fmt.Errorf("list active call-ups: %w", err)
	}
	for _, request := range active {
		if request.GraceDeadline == nil {
			continue
		}
		if err := sweeper.callUps.StartCallUpWorkflow(bound, CallUpWorkflowInput{
			QueueRequestID: request.QueueRequestID,
			TenantID:       request.TenantID,
			PrincipalID:    principal.ID,
			TerminalID:     request.TerminalID,
			GraceDeadline:  *request.GraceDeadline,
		}); err != nil {
			log.Printf("call-up sweep start workflow %s: %v", request.QueueRequestID, err)
		}
	}
	return nil
}
