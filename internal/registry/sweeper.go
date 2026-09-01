package registry

import (
	"context"
	"errors"
	"expvar"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// sweepTenantFailures counts per-tenant certificate-expiry sweep failures
// so operators can alert on a tenant stuck with lapsed certificates still
-- presenting ACTIVE.
var sweepTenantFailures = expvar.NewMap("registry_cert_sweep_tenant_failures")

// CertificateSweeper runs the periodic seafarer-certificate expiry across
// every active tenant, matching the call-up sweeper pattern: registry
// tables are tenant-scoped under RLS, so each tenant is swept inside its
// own tenant-bound context and one tenant's failure never skips the rest.
type CertificateSweeper struct {
	pool  *pgxpool.Pool
	store *Store
	// principal is the verified system identity recorded as provenance on
	// sweep-emitted registry.seafarer.v1 events.
	principal Principal
	// onlyTenant optionally restricts the sweep to a single tenant
	// (WORKER_TENANT_ID); empty sweeps every active tenant.
	onlyTenant string
	// now is injectable so the sweep cutoff is testable.
	now func() time.Time
}

// NewCertificateSweeper wires the multi-tenant certificate sweeper; every
// dependency is mandatory. onlyTenant may be empty (sweep all tenants).
func NewCertificateSweeper(pool *pgxpool.Pool, store *Store, principal Principal, onlyTenant string) (*CertificateSweeper, error) {
	if pool == nil || store == nil {
		return nil, errors.New("certificate sweeper requires a database pool and a registry store")
	}
	if !principal.valid() {
		return nil, errors.New("certificate sweeper requires a verified sweep principal")
	}
	return &CertificateSweeper{
		pool:       pool,
		store:      store,
		principal:  principal,
		onlyTenant: onlyTenant,
		now:        func() time.Time { return time.Now().UTC() },
	}, nil
}

// SweepOnce expires lapsed ACTIVE certificates in every active tenant (or
// only the configured tenant restriction). Tenant failures are isolated:
// each is logged and counted, the remaining tenants are still swept, and
// the first error is returned at the end.
func (sweeper *CertificateSweeper) SweepOnce(ctx context.Context) error {
	tenants, err := sweeper.sweepTenants(ctx)
	if err != nil {
		return err
	}
	var firstErr error
	for _, tenantID := range tenants {
		bound, err := tenantctx.WithClaims(ctx, tenantctx.Claims{
			Issuer:   "registry-sweeper",
			Audience: "s1-port-interoperability",
			TenantID: tenantID,
			Subject:  sweeper.principal.ID,
			Expires:  time.Now().Add(time.Hour).Unix(),
		})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if _, err := sweeper.store.ExpireCertificates(bound, sweeper.principal); err != nil {
			log.Printf("certificate expiry sweep: tenant %s: %v", tenantID, err)
			sweepTenantFailures.Add(tenantID, 1)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Run loops SweepOnce on the configured interval until ctx is cancelled.
func (sweeper *CertificateSweeper) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("certificate sweeper requires a positive interval")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := sweeper.SweepOnce(ctx); err != nil {
			log.Printf("certificate expiry sweep cycle: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// sweepTenants resolves the tenants to sweep: the single configured tenant,
// or every active platform tenant.
func (sweeper *CertificateSweeper) sweepTenants(ctx context.Context) ([]string, error) {
	if sweeper.onlyTenant != "" {
		return []string{sweeper.onlyTenant}, nil
	}
	rows, err := sweeper.pool.Query(ctx, `SELECT tenant_id FROM platform_tenants WHERE active ORDER BY tenant_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tenants := []string{}
	for rows.Next() {
		var tenantID string
		if err := rows.Scan(&tenantID); err != nil {
			return nil, err
		}
		tenants = append(tenants, tenantID)
	}
	return tenants, rows.Err()
}
