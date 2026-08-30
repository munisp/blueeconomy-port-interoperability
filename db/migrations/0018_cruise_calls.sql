-- Cruise call workflows: the cruise vessel port-call object extends the
-- existing port_calls model with passenger count bands, excursion manifests,
-- cruise dues assessment hooks (per-passenger charges computed from the
-- versioned CRUISE_DUES tariff schedule — the NPA US$10/head passenger
-- charge class) and terminal/berth allocation for cruise tenders.

CREATE TABLE cruise_calls (
    call_id TEXT PRIMARY KEY CHECK (call_id ~ '^[A-Za-z0-9._:-]{2,64}$'),
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    idempotency_key TEXT NOT NULL UNIQUE,
    -- The underlying platform port call this cruise call extends.
    port_call_id TEXT NOT NULL REFERENCES port_calls(call_id),
    cruise_line TEXT NOT NULL CHECK (length(cruise_line) BETWEEN 2 AND 256),
    vessel_name TEXT NOT NULL CHECK (length(vessel_name) BETWEEN 2 AND 256),
    pax_count INTEGER NOT NULL CHECK (pax_count > 0),
    pax_band TEXT NOT NULL CHECK (pax_band IN ('SMALL', 'MEDIUM', 'LARGE', 'MEGA')),
    status TEXT NOT NULL CHECK (status IN ('PLANNED', 'CONFIRMED', 'ARRIVED', 'COMPLETED', 'CANCELLED')),
    created_by TEXT NOT NULL CHECK (length(created_by) BETWEEN 2 AND 256),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0)
);
CREATE INDEX cruise_calls_tenant_port_call_idx ON cruise_calls (tenant_id, port_call_id);

-- Excursion manifests attached to a cruise call.
CREATE TABLE cruise_excursions (
    excursion_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    call_id TEXT NOT NULL REFERENCES cruise_calls(call_id),
    idempotency_key TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL CHECK (length(name) BETWEEN 2 AND 256),
    operator TEXT NOT NULL CHECK (length(operator) BETWEEN 2 AND 256),
    pax_count INTEGER NOT NULL CHECK (pax_count > 0),
    status TEXT NOT NULL CHECK (status IN ('SCHEDULED', 'CANCELLED')),
    registered_by TEXT NOT NULL CHECK (length(registered_by) BETWEEN 2 AND 256),
    registered_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX cruise_excursions_call_idx ON cruise_excursions (tenant_id, call_id);

-- Tender terminal/berth allocation windows for cruise calls whose
-- passengers come ashore by tender. btree_gist backs the no-overlap
-- exclusion constraint on text equality members.
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE TABLE cruise_tender_allocations (
    allocation_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    call_id TEXT NOT NULL REFERENCES cruise_calls(call_id),
    idempotency_key TEXT NOT NULL UNIQUE,
    terminal_code TEXT NOT NULL CHECK (terminal_code ~ '^[A-Z0-9-]{2,16}$'),
    berth_code TEXT NOT NULL CHECK (berth_code ~ '^[A-Z0-9-]{1,16}$'),
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    allocated_by TEXT NOT NULL CHECK (length(allocated_by) BETWEEN 2 AND 256),
    allocated_at TIMESTAMPTZ NOT NULL,
    CHECK (window_end > window_start),
    -- One berth serves one call at a time: no overlapping tender windows.
    EXCLUDE USING gist (
        call_id WITH =,
        berth_code WITH =,
        tstzrange(window_start, window_end) WITH &&
    )
);
CREATE INDEX cruise_tender_allocations_call_idx ON cruise_tender_allocations (tenant_id, call_id);

-- Tenant isolation, matching migrations 0008/0009.
ALTER TABLE cruise_calls ENABLE ROW LEVEL SECURITY;
ALTER TABLE cruise_calls FORCE ROW LEVEL SECURITY;
CREATE POLICY cruise_calls_tenant_policy ON cruise_calls
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE cruise_excursions ENABLE ROW LEVEL SECURITY;
ALTER TABLE cruise_excursions FORCE ROW LEVEL SECURITY;
CREATE POLICY cruise_excursions_tenant_policy ON cruise_excursions
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE cruise_tender_allocations ENABLE ROW LEVEL SECURITY;
ALTER TABLE cruise_tender_allocations FORCE ROW LEVEL SECURITY;
CREATE POLICY cruise_tender_allocations_tenant_policy ON cruise_tender_allocations
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
