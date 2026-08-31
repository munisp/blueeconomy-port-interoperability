-- Customs declaration engine (singlewindow domain import). Additive only:
-- three new tenant-scoped RLS tables and a widened platform outbox topic
-- check for the trade.declarations.v1 topic.

-- The declaration lifecycle state machine:
--   DRAFT -> SUBMITTED -> RISK_ASSESSED -> GREEN_LANE|YELLOW_LANE|RED_LANE
--        -> CLEARED|REJECTED
--   SUBMITTED -> SCORING_UNAVAILABLE (terminal: fail-closed scorer outage)
--   any pre-terminal version -> SUPERSEDED (amendment writes a new DRAFT
--   revision row under the same declaration_ref)
CREATE TABLE customs_declarations (
    declaration_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    request_id TEXT NOT NULL CHECK (length(request_id) BETWEEN 8 AND 128),
    declaration_ref TEXT NOT NULL CHECK (declaration_ref ~ '^[A-Z0-9][A-Z0-9/-]{3,63}$'),
    ucr TEXT CHECK (ucr IS NULL OR length(ucr) BETWEEN 8 AND 64),
    revision INTEGER NOT NULL CHECK (revision >= 1),
    supersedes_id UUID REFERENCES customs_declarations(declaration_id),
    trader_id TEXT NOT NULL CHECK (length(trader_id) BETWEEN 2 AND 256),
    declaration_type TEXT NOT NULL CHECK (declaration_type IN ('IMPORT', 'EXPORT', 'TRANSIT', 'RE_EXPORT', 'TEMPORARY_IMPORT')),
    status TEXT NOT NULL CHECK (status IN (
        'DRAFT', 'SUBMITTED', 'RISK_ASSESSED', 'GREEN_LANE', 'YELLOW_LANE',
        'RED_LANE', 'CLEARED', 'REJECTED', 'SCORING_UNAVAILABLE', 'SUPERSEDED'
    )),
    risk_lane TEXT CHECK (risk_lane IS NULL OR risk_lane IN ('GREEN', 'YELLOW', 'RED')),
    risk_score INTEGER CHECK (risk_score IS NULL OR risk_score BETWEEN 0 AND 100),
    scoring_model TEXT,
    scoring_error TEXT,
    hs_code TEXT NOT NULL CHECK (hs_code ~ '^[0-9]{6,10}$'),
    goods_description TEXT NOT NULL CHECK (length(goods_description) BETWEEN 10 AND 4096),
    country_of_origin CHAR(2) NOT NULL,
    country_of_destination CHAR(2),
    port_of_entry TEXT NOT NULL CHECK (length(port_of_entry) BETWEEN 2 AND 64),
    gross_weight_kg BIGINT NOT NULL CHECK (gross_weight_kg > 0),
    net_weight_kg BIGINT NOT NULL CHECK (net_weight_kg > 0 AND net_weight_kg <= gross_weight_kg),
    number_of_packages INTEGER NOT NULL CHECK (number_of_packages > 0),
    consignee_id TEXT NOT NULL CHECK (length(consignee_id) BETWEEN 2 AND 128),
    operator_id TEXT NOT NULL CHECK (length(operator_id) BETWEEN 2 AND 128),
    is_aeo BOOLEAN NOT NULL DEFAULT false,
    invoice_amount_minor BIGINT NOT NULL CHECK (invoice_amount_minor > 0),
    freight_amount_minor BIGINT NOT NULL DEFAULT 0 CHECK (freight_amount_minor >= 0),
    insurance_amount_minor BIGINT NOT NULL DEFAULT 0 CHECK (insurance_amount_minor >= 0),
    invoice_currency CHAR(3) NOT NULL,
    tariff_bps INTEGER NOT NULL CHECK (tariff_bps BETWEEN 0 AND 10000),
    vat_bps INTEGER NOT NULL CHECK (vat_bps BETWEEN 0 AND 10000),
    levy_bps INTEGER NOT NULL DEFAULT 0 CHECK (levy_bps BETWEEN 0 AND 10000),
    excise_bps INTEGER NOT NULL DEFAULT 0 CHECK (excise_bps BETWEEN 0 AND 10000),
    duty_minor BIGINT CHECK (duty_minor IS NULL OR duty_minor >= 0),
    vat_minor BIGINT CHECK (vat_minor IS NULL OR vat_minor >= 0),
    levy_minor BIGINT CHECK (levy_minor IS NULL OR levy_minor >= 0),
    excise_minor BIGINT CHECK (excise_minor IS NULL OR excise_minor >= 0),
    total_duty_minor BIGINT CHECK (total_duty_minor IS NULL OR total_duty_minor >= 0),
    rejection_reason TEXT,
    submitted_at TIMESTAMPTZ,
    cleared_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL CHECK (version >= 1),
    UNIQUE (tenant_id, request_id),
    UNIQUE (tenant_id, declaration_ref, revision),
    CHECK (status <> 'CLEARED' OR cleared_at IS NOT NULL),
    CHECK (status <> 'SUBMITTED' OR submitted_at IS NOT NULL)
);
-- At most one live (non-superseded) revision per declaration ref.
CREATE UNIQUE INDEX customs_declarations_live_ref_idx
    ON customs_declarations (tenant_id, declaration_ref) WHERE status <> 'SUPERSEDED';
CREATE INDEX customs_declarations_trader_idx ON customs_declarations (tenant_id, trader_id, status);
CREATE INDEX customs_declarations_status_idx ON customs_declarations (tenant_id, status);

ALTER TABLE customs_declarations ENABLE ROW LEVEL SECURITY;
ALTER TABLE customs_declarations FORCE ROW LEVEL SECURITY;
CREATE POLICY customs_declarations_tenant_policy ON customs_declarations
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- OGA (Other Government Agency) permits routed against a declaration. Model
-- port of the singlewindow oga_permits table: multi-agency permit records
-- with SLA deadlines; declaration submission is blocked while a linked
-- permit is not APPROVED or is expired.
CREATE TABLE declaration_permits (
    permit_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    declaration_id UUID NOT NULL REFERENCES customs_declarations(declaration_id),
    agency_code TEXT NOT NULL CHECK (length(agency_code) BETWEEN 2 AND 32),
    agency_name TEXT NOT NULL CHECK (length(agency_name) BETWEEN 2 AND 128),
    permit_type TEXT CHECK (permit_type IS NULL OR length(permit_type) BETWEEN 2 AND 128),
    permit_number TEXT CHECK (permit_number IS NULL OR length(permit_number) BETWEEN 2 AND 64),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'EXPIRED', 'SUSPENDED')),
    sla_deadline TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    responded_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, declaration_id, agency_code, permit_type)
);
CREATE INDEX declaration_permits_declaration_idx ON declaration_permits (declaration_id, status);

ALTER TABLE declaration_permits ENABLE ROW LEVEL SECURITY;
ALTER TABLE declaration_permits FORCE ROW LEVEL SECURITY;
CREATE POLICY declaration_permits_tenant_policy ON declaration_permits
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Clearance certificates, issued atomically with the CLEARED transition.
CREATE TABLE declaration_clearance_certificates (
    certificate_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    declaration_id UUID NOT NULL UNIQUE REFERENCES customs_declarations(declaration_id),
    certificate_number TEXT NOT NULL CHECK (length(certificate_number) BETWEEN 8 AND 96),
    issued_by TEXT NOT NULL CHECK (length(issued_by) BETWEEN 2 AND 256),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^sha256:[0-9a-f]{64}$'),
    issued_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, certificate_number)
);

ALTER TABLE declaration_clearance_certificates ENABLE ROW LEVEL SECURITY;
ALTER TABLE declaration_clearance_certificates FORCE ROW LEVEL SECURITY;
CREATE POLICY declaration_clearance_certificates_tenant_policy ON declaration_clearance_certificates
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- Declaration lifecycle events publish on trade.declarations.v1 through the
-- same transactional outbox as the ports.* topics.
ALTER TABLE platform_outbox DROP CONSTRAINT platform_outbox_topic_check;
ALTER TABLE platform_outbox
    ADD CONSTRAINT platform_outbox_topic_check
    CHECK (topic IN ('ports.booking.v1', 'ports.gate.v1', 'ports.queue.v1', 'trade.declarations.v1'));
