#!/usr/bin/env bash
# Requires an approved CSV: agency_code,tenant_id,mapping_sha256,approved_by,approved_at,effective_from
set -euo pipefail
: "${DATABASE_URL:?DATABASE_URL is required}"
: "${TENANT_MAPPING_CSV:?TENANT_MAPPING_CSV is required}"
: "${TENANT_BACKFILL_AUTHORIZATION_REF:?approved mapping/backfill reference is required}"
test -r "$TENANT_MAPPING_CSV" || { echo 'mapping CSV is unreadable' >&2; exit 2; }
grep -qx 'agency_code,tenant_id,mapping_sha256,approved_by,approved_at,effective_from' <(head -n1 "$TENANT_MAPPING_CSV") || { echo 'mapping CSV header is invalid' >&2; exit 2; }
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -v mapping_file="$TENANT_MAPPING_CSV" -v authorization_ref="$TENANT_BACKFILL_AUTHORIZATION_REF" <<'SQL'
BEGIN;
CREATE TEMP TABLE approved_mapping (agency_code text, tenant_id text, mapping_sha256 text, approved_by text, approved_at timestamptz, effective_from timestamptz) ON COMMIT DROP;
\copy approved_mapping FROM :'mapping_file' WITH (FORMAT csv, HEADER true)
INSERT INTO platform_tenants(tenant_id, authority_reference) SELECT DISTINCT tenant_id, :'authorization_ref' FROM approved_mapping ON CONFLICT (tenant_id) DO NOTHING;
INSERT INTO portcall_tenant_backfill_mappings(mapping_id,agency_code,tenant_id,mapping_sha256,approved_by,approved_at,effective_from)
SELECT gen_random_uuid(),agency_code,tenant_id,mapping_sha256,approved_by,approved_at,effective_from FROM approved_mapping
ON CONFLICT (agency_code,effective_from) DO NOTHING;
WITH candidate AS (
 SELECT p.call_id, m.tenant_id FROM port_calls p JOIN port_agency_profile_versions v ON v.profile_id=p.agency_profile_id AND v.version=p.agency_profile_version JOIN approved_mapping m ON m.agency_code=v.agency_code AND m.effective_from<=p.created_at
 WHERE p.tenant_id IS NULL
), unique_candidate AS (SELECT call_id, min(tenant_id) tenant_id FROM candidate GROUP BY call_id HAVING count(DISTINCT tenant_id)=1)
UPDATE port_calls p SET tenant_id=c.tenant_id FROM unique_candidate c WHERE p.call_id=c.call_id;
INSERT INTO portcall_tenant_quarantine(quarantine_id,source_table,source_key,reason,evidence_sha256)
SELECT gen_random_uuid(),'port_calls',p.call_id,'UNASSIGNED_OR_AMBIGUOUS_MAPPING','sha256:'||repeat('0',64) FROM port_calls p WHERE p.tenant_id IS NULL
ON CONFLICT (source_table,source_key) DO NOTHING;
COMMIT;
SQL
