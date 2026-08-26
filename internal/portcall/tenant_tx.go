package portcall

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// beginTenantTx is the single transaction entry point for tenant-aware Store
// operations. The PostgreSQL setting is transaction-local and cannot leak
// through pooled connections after commit or rollback.
func (store *Store) beginTenantTx(ctx context.Context) (pgx.Tx, tenantctx.Claims, error) {
	claims, err := tenantctx.Tenant(ctx)
	if err != nil {
		return nil, tenantctx.Claims{}, err
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return nil, tenantctx.Claims{}, fmt.Errorf("begin tenant transaction: %w", err)
	}
	if _, err := tx.Exec(ctx, "SELECT set_config('app.tenant_id', $1, true)", claims.TenantID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, tenantctx.Claims{}, fmt.Errorf("set tenant transaction context: %w", err)
	}
	return tx, claims, nil
}

// WithTenantTx executes work inside a transaction with the authenticated
// tenant identifier set locally on the PostgreSQL session.
func (store *Store) WithTenantTx(ctx context.Context, work func(pgx.Tx, tenantctx.Claims) error) error {
	tx, claims, err := store.beginTenantTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := work(tx, claims); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tenant transaction: %w", err)
	}
	return nil
}
