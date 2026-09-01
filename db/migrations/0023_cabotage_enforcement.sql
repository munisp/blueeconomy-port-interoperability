-- Phase 12 cabotage enforcement (Nigerian Coastal and Inland Shipping
-- (Cabotage) Act 2003). Eligibility criteria (flag, ownership, build) live
-- in a configurable rules table so amendments to the Act or ministerial
-- waiver policy are data changes, not code changes; evaluation is
-- fail-closed: with no ACTIVE rule row the eligibility check denies.
--
-- Permits follow a maker-checker workflow (APPLICATION → APPROVED /
-- REJECTED); violations are flagged against the vessel registry and the
-- permit. Lifecycle events (registry.cabotage.v1) are JWS-signed into the
-- shared platform_outbox in the same transaction as the mutation.

CREATE TABLE registry_cabotage_rules (
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    rule_id TEXT NOT NULL CHECK (length(rule_id) BETWEEN 1 AND 64),
    -- ISO 3166-1 alpha-2 flag state that a cabotage vessel must fly.
    required_flag TEXT NOT NULL CHECK (required_flag ~ '^[A-Z]{2}$'),
    -- Minimum percentage (0-100) of beneficial ownership that must be held
    -- by nationals of the required flag state (Cabotage Act s.22-23).
    min_national_ownership_pct INTEGER NOT NULL CHECK (min_national_ownership_pct BETWEEN 0 AND 100),
    -- Whether the vessel must have been built in the required flag state
    -- (Cabotage Act s.22 build criterion); waivers are captured per permit.
    require_domestic_build BOOLEAN NOT NULL,
    -- Whether a ministerial waiver may substitute for an unmet criterion.
    waiver_allowed BOOLEAN NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'RETIRED')),
    effective_from TIMESTAMPTZ NOT NULL,
    created_by TEXT NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, rule_id)
);
-- Exactly the latest ACTIVE rule row per tenant governs evaluation; the
-- partial unique index guarantees at most one.
CREATE UNIQUE INDEX registry_cabotage_rules_active_idx
    ON registry_cabotage_rules (tenant_id) WHERE status = 'ACTIVE';

CREATE TABLE registry_cabotage_permits (
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    permit_id TEXT NOT NULL CHECK (length(permit_id) BETWEEN 1 AND 64),
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 256),
    vessel_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    -- Evaluation snapshot (fail-closed audit): which criteria were met at
    -- decision time and the waiver reference when a criterion was waived.
    flag_criterion_met BOOLEAN NOT NULL,
    ownership_criterion_met BOOLEAN NOT NULL,
    build_criterion_met BOOLEAN NOT NULL,
    waiver_reference TEXT CHECK (waiver_reference IS NULL OR length(waiver_reference) BETWEEN 4 AND 256),
    national_ownership_pct INTEGER NOT NULL CHECK (national_ownership_pct BETWEEN 0 AND 100),
    trade_route TEXT NOT NULL CHECK (length(trade_route) BETWEEN 1 AND 256),
    status TEXT NOT NULL CHECK (status IN ('APPLICATION', 'APPROVED', 'REJECTED', 'REVOKED')),
    applied_by TEXT NOT NULL CHECK (length(applied_by) BETWEEN 1 AND 256),
    decided_by TEXT CHECK (decided_by IS NULL OR length(decided_by) BETWEEN 1 AND 256),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    PRIMARY KEY (tenant_id, permit_id),
    UNIQUE (idempotency_key),
    FOREIGN KEY (tenant_id, vessel_id) REFERENCES registry_vessels (tenant_id, vessel_id),
    FOREIGN KEY (tenant_id, rule_id) REFERENCES registry_cabotage_rules (tenant_id, rule_id),
    -- Maker-checker: the deciding officer is never the applicant.
    CHECK (decided_by IS NULL OR decided_by <> applied_by),
    -- A waiver reference is mandatory when a required criterion is unmet
    -- and the permit is nevertheless approvable.
    CHECK (status NOT IN ('APPROVED')
           OR (flag_criterion_met AND ownership_criterion_met AND build_criterion_met)
           OR waiver_reference IS NOT NULL)
);
CREATE INDEX registry_cabotage_permits_vessel_idx
    ON registry_cabotage_permits (tenant_id, vessel_id);
-- At most one open (APPLICATION/APPROVED) permit per vessel.
CREATE UNIQUE INDEX registry_cabotage_permits_open_idx
    ON registry_cabotage_permits (tenant_id, vessel_id) WHERE status IN ('APPLICATION', 'APPROVED');

CREATE TABLE registry_cabotage_violations (
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    violation_id TEXT NOT NULL CHECK (length(violation_id) BETWEEN 1 AND 64),
    vessel_id TEXT NOT NULL,
    permit_id TEXT,
    violation_type TEXT NOT NULL CHECK (violation_type IN (
        'NO_PERMIT',            -- coastal trade without any approved permit
        'EXPIRED_PERMIT',       -- trading after permit revocation/expiry
        'ROUTE_OUTSIDE_PERMIT', -- trading outside the permitted route
        'CRITERION_LAPSED'      -- eligibility criterion lapsed after grant
    )),
    detail TEXT NOT NULL CHECK (length(detail) BETWEEN 1 AND 1024),
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'RESOLVED')),
    flagged_by TEXT NOT NULL CHECK (length(flagged_by) BETWEEN 1 AND 256),
    flagged_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, violation_id),
    FOREIGN KEY (tenant_id, vessel_id) REFERENCES registry_vessels (tenant_id, vessel_id),
    FOREIGN KEY (tenant_id, permit_id) REFERENCES registry_cabotage_permits (tenant_id, permit_id),
    CHECK (status = 'OPEN' OR resolved_at IS NOT NULL)
);
CREATE INDEX registry_cabotage_violations_open_idx
    ON registry_cabotage_violations (tenant_id, vessel_id) WHERE status = 'OPEN';

-- Tenant isolation matching migration 0008.
ALTER TABLE registry_cabotage_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE registry_cabotage_rules FORCE ROW LEVEL SECURITY;
CREATE POLICY registry_cabotage_rules_tenant_policy ON registry_cabotage_rules
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE registry_cabotage_permits ENABLE ROW LEVEL SECURITY;
ALTER TABLE registry_cabotage_permits FORCE ROW LEVEL SECURITY;
CREATE POLICY registry_cabotage_permits_tenant_policy ON registry_cabotage_permits
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE registry_cabotage_violations ENABLE ROW LEVEL SECURITY;
ALTER TABLE registry_cabotage_violations FORCE ROW LEVEL SECURITY;
CREATE POLICY registry_cabotage_violations_tenant_policy ON registry_cabotage_violations
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
