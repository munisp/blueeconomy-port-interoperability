-- EXPAND ONLY. Do not assign tenant_id or enable RLS until approved mapping/backfill release.
CREATE TABLE IF NOT EXISTS platform_tenants (
  tenant_id text PRIMARY KEY CHECK (tenant_id ~ '^tenant-[A-Za-z0-9._:-]{3,128}$'),
  authority_reference text NOT NULL CHECK (length(authority_reference) BETWEEN 8 AND 512),
  active boolean NOT NULL DEFAULT true,
  registered_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS portcall_tenant_backfill_mappings (
  mapping_id uuid PRIMARY KEY,
  agency_code text NOT NULL,
  tenant_id text NOT NULL REFERENCES platform_tenants(tenant_id),
  mapping_sha256 text NOT NULL CHECK (mapping_sha256 ~ '^sha256:[0-9a-f]{64}$'),
  approved_by text NOT NULL,
  approved_at timestamptz NOT NULL,
  effective_from timestamptz NOT NULL,
  UNIQUE (agency_code, effective_from)
);
CREATE TABLE IF NOT EXISTS portcall_tenant_quarantine (
  quarantine_id uuid PRIMARY KEY,
  source_table text NOT NULL,
  source_key text NOT NULL,
  reason text NOT NULL,
  evidence_sha256 text NOT NULL CHECK (evidence_sha256 ~ '^sha256:[0-9a-f]{64}$'),
  quarantined_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz,
  resolution_reference text,
  UNIQUE (source_table, source_key)
);
ALTER TABLE port_calls ADD COLUMN IF NOT EXISTS tenant_id text REFERENCES platform_tenants(tenant_id);
ALTER TABLE port_call_documents ADD COLUMN IF NOT EXISTS tenant_id text REFERENCES platform_tenants(tenant_id);
ALTER TABLE port_call_clearance_decisions ADD COLUMN IF NOT EXISTS tenant_id text REFERENCES platform_tenants(tenant_id);
ALTER TABLE port_call_outbox ADD COLUMN IF NOT EXISTS tenant_id text REFERENCES platform_tenants(tenant_id);
ALTER TABLE port_agency_profile_versions ADD COLUMN IF NOT EXISTS tenant_id text REFERENCES platform_tenants(tenant_id);
ALTER TABLE port_agency_profile_events ADD COLUMN IF NOT EXISTS tenant_id text REFERENCES platform_tenants(tenant_id);
CREATE INDEX IF NOT EXISTS port_calls_tenant_call_idx ON port_calls(tenant_id, call_id) WHERE tenant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS port_call_documents_tenant_call_idx ON port_call_documents(tenant_id, call_id) WHERE tenant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS port_call_outbox_tenant_created_idx ON port_call_outbox(tenant_id, created_at) WHERE tenant_id IS NOT NULL;
-- The following RLS commands belong to a later enforce migration only:
-- ALTER TABLE port_calls ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE port_calls FORCE ROW LEVEL SECURITY;
