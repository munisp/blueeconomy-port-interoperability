package portcall

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantdb"
)

// WithTenantTx is the required entry point for tenant-aware Store operations.
// The PostgreSQL setting is transaction-local and cannot leak through pooled
// connections after commit or rollback. Every public Store method runs through
// this wrapper so row-level security is always scoped to the verified caller.
func (store *Store) WithTenantTx(ctx context.Context, work func(pgx.Tx, tenantctx.Claims) error) error {
	return tenantdb.WithTx(ctx, store.pool, work)
}
