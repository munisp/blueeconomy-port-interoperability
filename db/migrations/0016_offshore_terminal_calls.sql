-- Offshore terminal-call objects (SBM/SPM/FPSO) and the versioned,
-- effective-dated tariff schedule they are assessed against. Tariff-engine
-- lesson: rates are DATA — every charge is a row with a legal anchor and an
-- effectiveness window, never a code constant. Revenue assessments are
-- computed deterministically from a pinned schedule and emitted to the
-- financial-controls contract through the existing platform outbox.

-- Widen the outbox topic contract for the new platform topics.
ALTER TABLE platform_outbox DROP CONSTRAINT platform_outbox_topic_check;
ALTER TABLE platform_outbox
    ADD CONSTRAINT platform_outbox_topic_check
    CHECK (topic IN (
        'ports.booking.v1', 'ports.gate.v1', 'ports.queue.v1',
        'trade.declarations.v1', 'ports.offshore.v1', 'ports.manifests.v1',
        'ports.cruise.v1', 'finance.revenue-assessments.v1'
    ));

-- Versioned tariff schedules. A schedule is immutable once registered: rate
-- changes are a new schedule_id with a new effectiveness window (the
-- effective_from/effective_to pair is the temporal version dimension).
CREATE TABLE tariff_schedules (
    schedule_id TEXT PRIMARY KEY CHECK (schedule_id ~ '^[A-Za-z0-9._:-]{2,64}$'),
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    domain TEXT NOT NULL CHECK (domain IN ('OFFSHORE_TERMINAL', 'CRUISE_DUES')),
    name TEXT NOT NULL CHECK (length(name) BETWEEN 2 AND 256),
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to TIMESTAMPTZ,
    legal_anchor TEXT NOT NULL CHECK (length(legal_anchor) BETWEEN 2 AND 512),
    registered_by TEXT NOT NULL CHECK (length(registered_by) BETWEEN 2 AND 256),
    registered_at TIMESTAMPTZ NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    CHECK (effective_to IS NULL OR effective_to > effective_from)
);
CREATE INDEX tariff_schedules_domain_window_idx ON tariff_schedules (domain, effective_from);

-- Individual rate rows. amount_minor is the charge in the schedule currency's
-- minor units per unit of measure. PER_GT_BAND rows select the per-GT rate by
-- gross-tonnage band (Sea Protection Levy style); band_max NULL is unbounded.
CREATE TABLE tariff_rules (
    rule_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    schedule_id TEXT NOT NULL REFERENCES tariff_schedules(schedule_id),
    component_code TEXT NOT NULL CHECK (component_code ~ '^[A-Z][A-Z0-9_]{1,63}$'),
    unit TEXT NOT NULL CHECK (unit IN ('PER_GRT', 'PER_TON', 'PER_PAX', 'PER_CALL', 'PER_GT_BAND')),
    amount_minor BIGINT NOT NULL CHECK (amount_minor >= 0),
    band_min BIGINT NOT NULL DEFAULT 0 CHECK (band_min >= 0),
    band_max BIGINT,
    legal_anchor TEXT NOT NULL CHECK (length(legal_anchor) BETWEEN 2 AND 512),
    CHECK (unit <> 'PER_GT_BAND' OR (band_max IS NULL OR band_max > band_min)),
    UNIQUE (schedule_id, component_code, band_min)
);
CREATE INDEX tariff_rules_schedule_idx ON tariff_rules (schedule_id, component_code);

-- Offshore terminal calls: tanker calls at SBM/SPM/FPSO terminals that never
-- touch a berth. The mooring-master workflow states run NOMINATED through
-- DEPARTED; CANCELLED is the pre-mooring abort.
CREATE TABLE offshore_terminal_calls (
    call_id TEXT PRIMARY KEY CHECK (call_id ~ '^[A-Za-z0-9._:-]{2,64}$'),
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    idempotency_key TEXT NOT NULL UNIQUE,
    vessel_imo TEXT NOT NULL CHECK (vessel_imo ~ '^[0-9]{7}$'),
    vessel_name TEXT NOT NULL CHECK (length(vessel_name) BETWEEN 2 AND 256),
    terminal_code TEXT NOT NULL CHECK (terminal_code ~ '^[A-Z0-9-]{2,16}$'),
    terminal_kind TEXT NOT NULL CHECK (terminal_kind IN ('SBM', 'SPM', 'FPSO')),
    buoy_id TEXT NOT NULL CHECK (length(buoy_id) BETWEEN 1 AND 64),
    agency_code TEXT NOT NULL CHECK (agency_code ~ '^[A-Z]{2,8}$'),
    gross_tonnage BIGINT NOT NULL CHECK (gross_tonnage > 0),
    mooring_window_start TIMESTAMPTZ NOT NULL,
    mooring_window_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'NOMINATED', 'APPROACH_CLEARED', 'MOORED', 'HOSE_CONNECTED',
        'LOADING', 'CUSTODY_TRANSFERRED', 'DISCONNECTED', 'DEPARTED', 'CANCELLED'
    )),
    mooring_master TEXT CHECK (mooring_master IS NULL OR length(mooring_master) BETWEEN 2 AND 256),
    nominated_by TEXT NOT NULL CHECK (length(nominated_by) BETWEEN 2 AND 256),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    CHECK (mooring_window_end > mooring_window_start)
);
CREATE INDEX offshore_calls_tenant_terminal_idx ON offshore_terminal_calls (tenant_id, terminal_code, mooring_window_start);

-- Append-only operational events: hose connection, loading-arm operations and
-- custody-transfer metering readings. Metering rows carry opening/closing
-- readings; the transferred volume is derived, never client-supplied.
CREATE TABLE offshore_call_events (
    event_seq BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    call_id TEXT NOT NULL REFERENCES offshore_terminal_calls(call_id),
    event_type TEXT NOT NULL CHECK (event_type IN (
        'HOSE_CONNECTION', 'LOADING_ARM_START', 'LOADING_ARM_STOP',
        'CUSTODY_METER_READING', 'MOORING_NOTE'
    )),
    recorded_by TEXT NOT NULL CHECK (length(recorded_by) BETWEEN 2 AND 256),
    recorded_at TIMESTAMPTZ NOT NULL,
    remarks TEXT NOT NULL DEFAULT '' CHECK (length(remarks) <= 1024),
    meter_id TEXT CHECK (meter_id IS NULL OR length(meter_id) BETWEEN 1 AND 64),
    meter_opening_m3 NUMERIC(18,3),
    meter_closing_m3 NUMERIC(18,3),
    CHECK (
        (event_type = 'CUSTODY_METER_READING' AND meter_id IS NOT NULL AND
         meter_opening_m3 IS NOT NULL AND meter_closing_m3 IS NOT NULL AND
         meter_closing_m3 >= meter_opening_m3)
        OR
        (event_type <> 'CUSTODY_METER_READING' AND meter_id IS NULL AND
         meter_opening_m3 IS NULL AND meter_closing_m3 IS NULL)
    )
);
CREATE INDEX offshore_call_events_call_idx ON offshore_call_events (call_id, event_seq);

-- Revenue assessments: the deterministic Compute output pinned to a schedule
-- version. idempotency_key makes every assessment mutation replay-safe; the
-- outbox row is written in the same transaction (exactly-once emission).
CREATE TABLE revenue_assessments (
    assessment_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    idempotency_key TEXT NOT NULL UNIQUE,
    domain TEXT NOT NULL CHECK (domain IN ('OFFSHORE_TERMINAL', 'CRUISE_DUES')),
    call_reference TEXT NOT NULL CHECK (length(call_reference) BETWEEN 2 AND 64),
    schedule_id TEXT NOT NULL REFERENCES tariff_schedules(schedule_id),
    as_of TIMESTAMPTZ NOT NULL,
    facts JSONB NOT NULL,
    line_items JSONB NOT NULL,
    total_minor BIGINT NOT NULL CHECK (total_minor >= 0),
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    assessed_by TEXT NOT NULL CHECK (length(assessed_by) BETWEEN 2 AND 256),
    assessed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX revenue_assessments_call_idx ON revenue_assessments (domain, call_reference, assessed_at);

-- Tenant isolation, matching migrations 0008/0009.
ALTER TABLE tariff_schedules ENABLE ROW LEVEL SECURITY;
ALTER TABLE tariff_schedules FORCE ROW LEVEL SECURITY;
CREATE POLICY tariff_schedules_tenant_policy ON tariff_schedules
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE tariff_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE tariff_rules FORCE ROW LEVEL SECURITY;
CREATE POLICY tariff_rules_tenant_policy ON tariff_rules
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE offshore_terminal_calls ENABLE ROW LEVEL SECURITY;
ALTER TABLE offshore_terminal_calls FORCE ROW LEVEL SECURITY;
CREATE POLICY offshore_terminal_calls_tenant_policy ON offshore_terminal_calls
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE offshore_call_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE offshore_call_events FORCE ROW LEVEL SECURITY;
CREATE POLICY offshore_call_events_tenant_policy ON offshore_call_events
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE revenue_assessments ENABLE ROW LEVEL SECURITY;
ALTER TABLE revenue_assessments FORCE ROW LEVEL SECURITY;
CREATE POLICY revenue_assessments_tenant_policy ON revenue_assessments
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
