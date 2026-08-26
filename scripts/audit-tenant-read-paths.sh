#!/usr/bin/env bash
set -euo pipefail

store=${1:-internal/portcall/store.go}
test -r "$store" || { echo "missing Store source: $store" >&2; exit 2; }

get_method=$(awk '
  /^func \(store \*Store\) Get\(ctx context\.Context/ { capture=1 }
  capture { print }
  capture && /^func \(store \*Store\) [A-Za-z]/ && $0 !~ /^func \(store \*Store\) Get\(ctx context\.Context/ { exit }
' "$store")

printf '%s\n' 'S1_TENANT_READ_PATH_STATIC_AUDIT'
if ! grep -Fq 'store.beginTenantTx(ctx)' <<<"$get_method"; then
  printf '%s\n' 'RESULT: NOT READY — Get does not begin a tenant-aware transaction.'
  exit 1
fi
if grep -Fq 'store.pool.QueryRow' <<<"$get_method"; then
  printf '%s\n' 'RESULT: NOT READY — Get queries a pooled connection without tenant transaction context.'
  exit 1
fi
if ! grep -Fq 'tx.QueryRow' <<<"$get_method"; then
  printf '%s\n' 'RESULT: NOT READY — Get does not execute through its tenant-aware transaction.'
  exit 1
fi

printf '%s\n' 'RESULT: PASS — Get uses transaction-local tenant context before querying tenant-scoped port calls.'
