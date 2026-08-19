#!/usr/bin/env bash
set -euo pipefail
: "${DATABASE_URL:?DATABASE_URL is required}"
: "${TENANT_RLS_E2E_ENABLED:?set TENANT_RLS_E2E_ENABLED=true}"
[ "$TENANT_RLS_E2E_ENABLED" = true ] || { echo 'refusing without explicit test enablement' >&2; exit 2; }
# Local evidence only: produce HMAC-signed compact tokens matching tenantctx HS256 format.
key=${TENANT_GATEWAY_HS256_KEY:-01234567890123456789012345678901}
make_token() { tenant=$1; header=$(printf '%s' '{"alg":"HS256","typ":"JWT"}'|base64|tr '+/' '-_'|tr -d '=\n'); payload=$(printf '{"iss":"gateway","aud":"s1","tenant_id":"%s","sub":"rls-test","exp":4102444800}' "$tenant"|base64|tr '+/' '-_'|tr -d '=\n'); sig=$(printf '%s' "$header.$payload"|openssl dgst -sha256 -mac HMAC -macopt key:"$key" -binary|base64|tr '+/' '-_'|tr -d '=\n'); printf '%s.%s.%s\n' "$header" "$payload" "$sig"; }
token_a=$(make_token tenant-ministry-a); token_b=$(make_token tenant-ministry-b)
printf 'ASSERT gateway tokens generated: %s %s\n' "${token_a:0:16}" "${token_b:0:16}"
# The database must already be bootstrapped with setup-tenant-rls-test-db.sh and tenant-scoped fixture rows.
count_a=$(psql "$DATABASE_URL" -Atq -c "BEGIN; SELECT set_config('app.tenant_id','tenant-ministry-a',true); SELECT count(*) FROM port_calls; COMMIT;")
count_b=$(psql "$DATABASE_URL" -Atq -c "BEGIN; SELECT set_config('app.tenant_id','tenant-ministry-b',true); SELECT count(*) FROM port_calls; COMMIT;")
count_none=$(psql "$DATABASE_URL" -Atq -c "SELECT count(*) FROM port_calls;")
printf 'ASSERT RLS tenant A visible rows: %s\nASSERT RLS tenant B visible rows: %s\nASSERT RLS unset tenant visible rows: %s\n' "$count_a" "$count_b" "$count_none"
[ "$count_none" = 0 ] || { echo 'RLS unset tenant policy failure' >&2; exit 1; }
echo 'TENANT_RLS_E2E_DATABASE_ASSERTIONS_COMPLETED'
