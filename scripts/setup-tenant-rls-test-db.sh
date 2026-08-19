#!/usr/bin/env bash
set -euo pipefail
: "${DATABASE_URL:?DATABASE_URL is required}"
: "${TENANT_RLS_TEST_ENABLED:?set TENANT_RLS_TEST_ENABLED=true}"
[ "$TENANT_RLS_TEST_ENABLED" = true ] || { echo 'refusing without explicit test enablement' >&2; exit 2; }
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
for n in 0001_port_calls.sql 0002_documents_clearance.sql 0003_document_review.sql 0004_document_supersession_clearance_amendment.sql 0005_agency_profiles.sql 0006_profile_binding_and_append_only_ledger.sql 0007_tenant_expand.sql; do psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$repo_root/db/migrations/$n"; done
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO platform_tenants(tenant_id,authority_reference) VALUES ('tenant-ministry-a','local-rls-test'),('tenant-ministry-b','local-rls-test');
-- This harness uses only deterministic disposable test mappings, never historical Ministry records.
INSERT INTO portcall_tenant_backfill_mappings(mapping_id,agency_code,tenant_id,mapping_sha256,approved_by,approved_at,effective_from)
VALUES (gen_random_uuid(),'AGENCY-A','tenant-ministry-a','sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','local-test',now(),'-infinity'),(gen_random_uuid(),'AGENCY-B','tenant-ministry-b','sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','local-test',now(),'-infinity');
SQL
# Test data must be inserted by the tenant-aware Store release and reconciled before enforcement.
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$repo_root/db/migrations/0008_tenant_rls_enforce.sql"
