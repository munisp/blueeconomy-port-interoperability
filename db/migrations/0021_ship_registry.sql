-- Phase 12 ship registry. Vessels are registered through a maker-checker
-- workflow (APPLICATION → SURVEY → REGISTRATION → CERTIFICATE_ISSUED) and
-- every ownership transfer is recorded in an append-only, hash-chained
-- history so registry tampering is detectable. IMO numbers are validated
-- against the weighted mod-10 check digit (internal/imonumber) and MMSIs
-- against the platform-admitted ITU MID table (internal/mmsinumber) at the
-- application layer before any row is written.
--
-- Lifecycle events (registry.vessel.v1) are JWS-signed into the shared
-- platform_outbox in the same transaction as the mutation, matching the
-- cruise/offshore emit pattern.

CREATE TABLE registry_vessels (
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    vessel_id TEXT NOT NULL CHECK (length(vessel_id) BETWEEN 1 AND 64),
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 256),
    imo_number TEXT NOT NULL CHECK (imo_number ~ '^[0-9]{7}$'),
    mmsi TEXT NOT NULL CHECK (mmsi ~ '^[1-9][0-9]{8}$'),
    vessel_name TEXT NOT NULL CHECK (length(vessel_name) BETWEEN 1 AND 256),
    flag_state TEXT NOT NULL CHECK (flag_state ~ '^[A-Z]{2}$'),
    class_society TEXT NOT NULL CHECK (length(class_society) BETWEEN 1 AND 128),
    gross_tonnage INTEGER NOT NULL CHECK (gross_tonnage > 0),
    build_year INTEGER NOT NULL CHECK (build_year BETWEEN 1800 AND 2100),
    build_country TEXT NOT NULL CHECK (build_country ~ '^[A-Z]{2}$'),
    owner_name TEXT NOT NULL CHECK (length(owner_name) BETWEEN 1 AND 256),
    owner_country TEXT NOT NULL CHECK (owner_country ~ '^[A-Z]{2}$'),
    status TEXT NOT NULL CHECK (status IN ('APPLICATION', 'SURVEY', 'REGISTRATION', 'CERTIFICATE_ISSUED', 'SUSPENDED', 'DEREGISTERED')),
    certificate_number TEXT CHECK (certificate_number IS NULL OR length(certificate_number) BETWEEN 4 AND 64),
    created_by TEXT NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    PRIMARY KEY (tenant_id, vessel_id),
    UNIQUE (idempotency_key),
    -- An IMO number identifies one hull worldwide: at most one live
    -- registration per tenant, and the check digit is enforced by the
    -- application layer (internal/imonumber) before insert.
    CHECK (status <> 'CERTIFICATE_ISSUED' OR certificate_number IS NOT NULL)
);
CREATE UNIQUE INDEX registry_vessels_live_imo_idx
    ON registry_vessels (tenant_id, imo_number)
    WHERE status NOT IN ('DEREGISTERED');
CREATE INDEX registry_vessels_status_idx ON registry_vessels (tenant_id, status);

-- Append-only ownership history with a per-vessel SHA-256 hash chain: each
-- entry commits to the previous entry's hash so silent rewriting or
-- deletion of history rows breaks the chain and is detectable by audit.
CREATE TABLE registry_vessel_ownership (
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    vessel_id TEXT NOT NULL,
    sequence_no INTEGER NOT NULL CHECK (sequence_no > 0),
    owner_name TEXT NOT NULL CHECK (length(owner_name) BETWEEN 1 AND 256),
    owner_country TEXT NOT NULL CHECK (owner_country ~ '^[A-Z]{2}$'),
    effective_from TIMESTAMPTZ NOT NULL,
    recorded_by TEXT NOT NULL CHECK (length(recorded_by) BETWEEN 1 AND 256),
    recorded_at TIMESTAMPTZ NOT NULL,
    previous_hash TEXT NOT NULL CHECK (previous_hash ~ '^sha256:[0-9a-f]{64}$'),
    entry_hash TEXT NOT NULL CHECK (entry_hash ~ '^sha256:[0-9a-f]{64}$'),
    PRIMARY KEY (tenant_id, vessel_id, sequence_no),
    UNIQUE (tenant_id, vessel_id, entry_hash),
    FOREIGN KEY (tenant_id, vessel_id) REFERENCES registry_vessels (tenant_id, vessel_id)
);

-- Tenant isolation matching migration 0008.
ALTER TABLE registry_vessels ENABLE ROW LEVEL SECURITY;
ALTER TABLE registry_vessels FORCE ROW LEVEL SECURITY;
CREATE POLICY registry_vessels_tenant_policy ON registry_vessels
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE registry_vessel_ownership ENABLE ROW LEVEL SECURITY;
ALTER TABLE registry_vessel_ownership FORCE ROW LEVEL SECURITY;
CREATE POLICY registry_vessel_ownership_tenant_policy ON registry_vessel_ownership
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
