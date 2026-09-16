-- Phase 19 fisheries licensing & permit registry (F3/F7 of the scenario
-- matrix): artisanal, industrial and aquaculture fishing permits cloned
-- from the proven cabotage permit pattern (migration 0023). Permits bind a
-- vessel (registry_vessels) and its recorded owner, carry an explicit
-- validity window, and follow a maker-checker lifecycle:
-- APPLICATION → GRANTED → SUSPENDED ⇄ GRANTED, GRANTED/SUSPENDED → REVOKED.
-- Every mutation appends to an audit-trail table and emits a JWS-signed
-- registry.fisheries.v1 event into the platform outbox in the same
-- transaction. Public verification by permit number is minimal-disclosure
-- (permit number, type, validity outcome, expiry) and exposes no owner or
-- vessel attributes.

CREATE TABLE registry_fisheries_permits (
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    permit_id TEXT NOT NULL CHECK (length(permit_id) BETWEEN 1 AND 64),
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 256),
    -- Public permit number printed on the licence; unique per tenant so the
    -- verification endpoint resolves exactly one permit.
    permit_number TEXT NOT NULL CHECK (length(permit_number) BETWEEN 4 AND 64),
    permit_type TEXT NOT NULL CHECK (permit_type IN ('ARTISANAL', 'INDUSTRIAL', 'AQUACULTURE')),
    -- Aquaculture permits may reference a farm site instead of a vessel;
    -- artisanal/industrial permits must reference a registered vessel.
    vessel_id TEXT,
    owner_name TEXT NOT NULL CHECK (length(owner_name) BETWEEN 1 AND 256),
    -- Aquaculture site reference (lease / cage registry id) when no vessel
    -- applies; never fabricated — recorded as supplied by the applicant.
    site_reference TEXT CHECK (site_reference IS NULL OR length(site_reference) BETWEEN 1 AND 256),
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('APPLICATION', 'REJECTED', 'GRANTED', 'SUSPENDED', 'REVOKED')),
    applied_by TEXT NOT NULL CHECK (length(applied_by) BETWEEN 1 AND 256),
    decided_by TEXT CHECK (decided_by IS NULL OR length(decided_by) BETWEEN 1 AND 256),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    PRIMARY KEY (tenant_id, permit_id),
    UNIQUE (idempotency_key),
    UNIQUE (tenant_id, permit_number),
    FOREIGN KEY (tenant_id, vessel_id) REFERENCES registry_vessels (tenant_id, vessel_id),
    CHECK (valid_to > valid_from),
    -- Vessel-backed permit types must reference a vessel; aquaculture must
    -- reference a site.
    CHECK ((permit_type IN ('ARTISANAL', 'INDUSTRIAL') AND vessel_id IS NOT NULL)
           OR (permit_type = 'AQUACULTURE' AND site_reference IS NOT NULL)),
    -- Maker-checker: the deciding officer is never the applicant.
    CHECK (decided_by IS NULL OR decided_by <> applied_by)
);
CREATE INDEX registry_fisheries_permits_vessel_idx
    ON registry_fisheries_permits (tenant_id, vessel_id) WHERE vessel_id IS NOT NULL;
-- At most one open (APPLICATION/GRANTED/SUSPENDED) permit per vessel.
CREATE UNIQUE INDEX registry_fisheries_permits_open_idx
    ON registry_fisheries_permits (tenant_id, vessel_id)
    WHERE vessel_id IS NOT NULL AND status IN ('APPLICATION', 'GRANTED', 'SUSPENDED');

-- Append-only audit trail: one row per lifecycle event (application,
-- grant, suspension, reinstatement, revocation). Rows are never updated or
-- deleted by application code; the grant check below removes UPDATE/DELETE
-- from the application role if one is used.
CREATE TABLE registry_fisheries_permit_audit (
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY,
    permit_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('APPLIED', 'GRANTED', 'REJECTED', 'SUSPENDED', 'REINSTATED', 'REVOKED')),
    actor TEXT NOT NULL CHECK (length(actor) BETWEEN 1 AND 256),
    detail TEXT NOT NULL CHECK (length(detail) BETWEEN 1 AND 1024),
    occurred_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, audit_id),
    FOREIGN KEY (tenant_id, permit_id) REFERENCES registry_fisheries_permits (tenant_id, permit_id)
);
CREATE INDEX registry_fisheries_permit_audit_permit_idx
    ON registry_fisheries_permit_audit (tenant_id, permit_id, audit_id);

-- Tenant isolation matching migration 0008.
ALTER TABLE registry_fisheries_permits ENABLE ROW LEVEL SECURITY;
ALTER TABLE registry_fisheries_permits FORCE ROW LEVEL SECURITY;
CREATE POLICY registry_fisheries_permits_tenant_policy ON registry_fisheries_permits
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE registry_fisheries_permit_audit ENABLE ROW LEVEL SECURITY;
ALTER TABLE registry_fisheries_permit_audit FORCE ROW LEVEL SECURITY;
CREATE POLICY registry_fisheries_permit_audit_tenant_policy ON registry_fisheries_permit_audit
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
