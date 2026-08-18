package portcall

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// WithTenantTx is the required entry point for tenant-aware Store operations after
// the switch release. The PostgreSQL setting is transaction-local and cannot leak
// through pooled connections after commit or rollback.
func (store *Store) WithTenantTx(ctx context.Context, work func(pgx.Tx, tenantctx.Claims) error) error {
	claims, err := tenantctx.Tenant(ctx)
	if err != nil {
		return err
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tenant transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT set_config('app.tenant_id', $1, true)", claims.TenantID); err != nil {
		return fmt.Errorf("set tenant transaction context: %w", err)
	}
	if err := work(tx, claims); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tenant transaction: %w", err)
	}
	return nil
}
