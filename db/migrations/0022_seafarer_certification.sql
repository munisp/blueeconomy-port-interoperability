-- Phase 12 seafarer certification. Seafarers hold STCW certificates with
-- issue/expiry windows and flag endorsements; certificates transition
-- ACTIVE → EXPIRED (periodic sweep, matching the call-up sweeper pattern)
-- or ACTIVE → SUSPENDED/REVOKED by the flag administration. Third-party
-- verification is a metered read: every verification call appends a usage
-- row (verification_usage) compatible with the marketplace metering hook.
--
-- Lifecycle events (registry.seafarer.v1) are JWS-signed into the shared
-- platform_outbox in the same transaction as the mutation.

CREATE TABLE registry_seafarers (
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    seafarer_id TEXT NOT NULL CHECK (length(seafarer_id) BETWEEN 1 AND 64),
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 256),
    full_name TEXT NOT NULL CHECK (length(full_name) BETWEEN 1 AND 256),
    date_of_birth DATE NOT NULL,
    nationality TEXT NOT NULL CHECK (nationality ~ '^[A-Z]{2}$'),
    rank TEXT NOT NULL CHECK (length(rank) BETWEEN 1 AND 128),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUSPENDED', 'DECEASED')),
    created_by TEXT NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    PRIMARY KEY (tenant_id, seafarer_id),
    UNIQUE (idempotency_key)
);

CREATE TABLE registry_seafarer_certificates (
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    certificate_number TEXT NOT NULL CHECK (length(certificate_number) BETWEEN 4 AND 64),
    seafarer_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 256),
    -- STCW 1978 (as amended) certificate classes; the closed set is
    -- enforced fail-closed at the application layer too.
    certificate_type TEXT NOT NULL CHECK (certificate_type IN (
        'STCW-II-1',   -- Officer in charge of a navigational watch
        'STCW-II-2',   -- Master/chief mate
        'STCW-III-1',  -- Officer in charge of an engineering watch
        'STCW-III-2',  -- Chief/second engineer
        'STCW-II-3',   -- Master <500 GT near-coastal
        'STCW-III-3',  -- Engineer <750 kW near-coastal
        'STCW-VI-1',   -- Basic safety training
        'STCW-VI-2',   -- Survival craft and rescue boats
        'STCW-VI-3',   -- Advanced fire fighting
        'STCW-VI-6',   -- Security awareness / designated duties
        'STCW-V-1',    -- Tanker cargo operations
        'GMDSS-GOC'    -- GMDSS general operator
    )),
    issuing_authority TEXT NOT NULL CHECK (length(issuing_authority) BETWEEN 1 AND 256),
    flag_endorsement TEXT NOT NULL CHECK (flag_endorsement ~ '^[A-Z]{2}$'),
    issued_at DATE NOT NULL,
    expires_at DATE NOT NULL CHECK (expires_at > issued_at),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'EXPIRED', 'SUSPENDED', 'REVOKED')),
    created_by TEXT NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    PRIMARY KEY (tenant_id, certificate_number),
    UNIQUE (idempotency_key),
    FOREIGN KEY (tenant_id, seafarer_id) REFERENCES registry_seafarers (tenant_id, seafarer_id)
);
CREATE INDEX registry_seafarer_certificates_holder_idx
    ON registry_seafarer_certificates (tenant_id, seafarer_id);
-- The expiry sweep scans ACTIVE certificates whose window has closed.
CREATE INDEX registry_seafarer_certificates_expiry_idx
    ON registry_seafarer_certificates (tenant_id, expires_at) WHERE status = 'ACTIVE';

-- Metered third-party verification usage. Each row is one verification
-- call; the marketplace billing hook aggregates per (tenant, verifier).
CREATE TABLE registry_certificate_verification_usage (
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    usage_id TEXT NOT NULL CHECK (length(usage_id) BETWEEN 1 AND 64),
    certificate_number TEXT NOT NULL,
    verifier_id TEXT NOT NULL CHECK (length(verifier_id) BETWEEN 1 AND 256),
    verified_at TIMESTAMPTZ NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('VALID', 'EXPIRED', 'SUSPENDED', 'REVOKED', 'NOT_FOUND')),
    PRIMARY KEY (tenant_id, usage_id)
);
CREATE INDEX registry_certificate_verification_usage_metering_idx
    ON registry_certificate_verification_usage (tenant_id, verifier_id, verified_at);

-- Tenant isolation matching migration 0008.
ALTER TABLE registry_seafarers ENABLE ROW LEVEL SECURITY;
ALTER TABLE registry_seafarers FORCE ROW LEVEL SECURITY;
CREATE POLICY registry_seafarers_tenant_policy ON registry_seafarers
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE registry_seafarer_certificates ENABLE ROW LEVEL SECURITY;
ALTER TABLE registry_seafarer_certificates FORCE ROW LEVEL SECURITY;
CREATE POLICY registry_seafarer_certificates_tenant_policy ON registry_seafarer_certificates
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE registry_certificate_verification_usage ENABLE ROW LEVEL SECURITY;
ALTER TABLE registry_certificate_verification_usage FORCE ROW LEVEL SECURITY;
CREATE POLICY registry_certificate_verification_usage_tenant_policy ON registry_certificate_verification_usage
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
