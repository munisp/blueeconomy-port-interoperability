#!/usr/bin/env bash
set -euo pipefail
store=${1:-internal/portcall/store.go}
test -r "$store" || { echo "missing Store source: $store" >&2; exit 2; }
printf '%s\n' 'S1_TENANT_TRANSACTION_STATIC_AUDIT'
printf '%s\n' 'Methods with legacy store.pool.Begin(ctx) transactions:'
awk '
/^func \(store \*Store\)/ { name=$0 }
/store\.pool\.Begin\(ctx\)/ { print "  " name }
' "$store"
printf '%s\n' 'Tenant wrapper entry point:'
grep -n 'func (store \*Store) WithTenantTx' internal/portcall/tenant_tx.go
legacy=$(grep -c 'store\.pool\.Begin(ctx)' "$store" || true)
wrapper=$(grep -c 'WithTenantTx(ctx' "$store" || true)
printf 'legacy_begin_calls=%s wrapper_calls_in_store=%s\n' "$legacy" "$wrapper"
if (( legacy > 0 )); then
  printf '%s\n' 'RESULT: NOT READY — legacy Store methods remain outside WithTenantTx.'
  exit 1
fi
printf '%s\n' 'RESULT: PASS — no legacy Store transaction start found.'
