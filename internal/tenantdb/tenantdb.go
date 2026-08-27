// Package tenantdb runs work inside a PostgreSQL transaction bound to the
// verified tenant claims in the context. The app.tenant_id setting is
// transaction-local and cannot leak through pooled connections after commit
// or rollback. All tenant-scoped stores must use this entry point.
package tenantdb

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

func WithTx(ctx context.Context, pool *pgxpool.Pool, work func(pgx.Tx, tenantctx.Claims) error) error {
	claims, err := tenantctx.Tenant(ctx)
	if err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
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
