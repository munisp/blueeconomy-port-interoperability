-- eCallUp 2.0 truck booking: per-truck slot booking, payment, gate control,
-- offline sync, platform outbox and NSW ingress replay protection.
CREATE TABLE port_terminals (
    terminal_id TEXT PRIMARY KEY CHECK (terminal_id ~ '^[A-Z][A-Z0-9-]{1,31}$'),
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    port_code TEXT NOT NULL CHECK (port_code ~ '^[A-Z]{2,8}$'),
    name TEXT NOT NULL CHECK (length(name) BETWEEN 2 AND 256),
    booking_fee_kobo BIGINT NOT NULL CHECK (booking_fee_kobo > 0),
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE terminal_slots (
    slot_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    terminal_id TEXT NOT NULL REFERENCES port_terminals(terminal_id),
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    capacity INTEGER NOT NULL CHECK (capacity > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (ends_at > starts_at),
    UNIQUE (tenant_id, terminal_id, starts_at)
);
CREATE INDEX terminal_slots_window_idx ON terminal_slots (terminal_id, starts_at, ends_at);

CREATE TABLE truck_bookings (
    booking_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    request_id TEXT NOT NULL CHECK (length(request_id) BETWEEN 8 AND 128),
    truck_plate TEXT NOT NULL CHECK (truck_plate ~ '^[A-Z0-9][A-Z0-9-]{2,15}$'),
    trucker_msisdn TEXT NOT NULL CHECK (trucker_msisdn ~ '^\+[0-9]{8,15}$'),
    terminal_id TEXT NOT NULL REFERENCES port_terminals(terminal_id),
    slot_id UUID REFERENCES terminal_slots(slot_id),
    channel TEXT NOT NULL CHECK (channel IN ('WEB', 'USSD', 'OFFLINE')),
    status TEXT NOT NULL CHECK (status IN (
        'DRAFTED', 'PENDING_SYNC', 'SLOT_RESERVED', 'PAID', 'GATE_APPROVED',
        'COMPLETED', 'CANCELLED', 'EXPIRED', 'RECONCILIATION_REQUIRED'
    )),
    amount_kobo BIGINT NOT NULL CHECK (amount_kobo > 0),
    currency TEXT NOT NULL DEFAULT 'NGN' CHECK (currency = 'NGN'),
    payment_receipt_ref TEXT,
    gate_id TEXT,
    ledger_commit_hash TEXT,
    reconciliation_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    UNIQUE (tenant_id, request_id)
);
CREATE INDEX truck_bookings_slot_idx ON truck_bookings (slot_id) WHERE slot_id IS NOT NULL;
CREATE INDEX truck_bookings_plate_idx ON truck_bookings (tenant_id, truck_plate);

-- DB-enforced no-overbooking: a booking may only hold a slot while it occupies
-- an active state; the trigger re-counts under the slot row lock taken by the
-- reserving transaction, so concurrent reservations cannot exceed capacity.
CREATE FUNCTION enforce_slot_capacity() RETURNS trigger AS $$
DECLARE
    slot_capacity INTEGER;
    active_count INTEGER;
BEGIN
    IF NEW.slot_id IS NULL OR NEW.status NOT IN ('SLOT_RESERVED', 'PAID', 'GATE_APPROVED') THEN
        RETURN NEW;
    END IF;
    SELECT capacity INTO slot_capacity FROM terminal_slots WHERE slot_id = NEW.slot_id FOR UPDATE;
    IF slot_capacity IS NULL THEN
        RAISE EXCEPTION 'slot % does not exist', NEW.slot_id;
    END IF;
    SELECT count(*) INTO active_count FROM truck_bookings
    WHERE slot_id = NEW.slot_id
      AND status IN ('SLOT_RESERVED', 'PAID', 'GATE_APPROVED')
      AND booking_id <> NEW.booking_id;
    IF active_count >= slot_capacity THEN
        RAISE EXCEPTION 'slot % is at capacity %', NEW.slot_id, slot_capacity
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER truck_bookings_capacity_guard
    BEFORE INSERT OR UPDATE OF slot_id, status ON truck_bookings
    FOR EACH ROW EXECUTE FUNCTION enforce_slot_capacity();

CREATE TABLE booking_payment_intents (
    intent_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    booking_id UUID NOT NULL REFERENCES truck_bookings(booking_id),
    request_id TEXT NOT NULL CHECK (length(request_id) BETWEEN 8 AND 128),
    amount_kobo BIGINT NOT NULL CHECK (amount_kobo > 0),
    currency TEXT NOT NULL DEFAULT 'NGN' CHECK (currency = 'NGN'),
    mojaloop_tx_ref TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('REQUESTED', 'COMPLETED', 'FAILED')),
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, request_id)
);

CREATE TABLE gate_scans (
    scan_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    booking_id UUID NOT NULL REFERENCES truck_bookings(booking_id),
    gate_id TEXT NOT NULL CHECK (length(gate_id) BETWEEN 2 AND 64),
    scanned_by TEXT NOT NULL CHECK (length(scanned_by) BETWEEN 2 AND 256),
    decision TEXT NOT NULL CHECK (decision IN ('APPROVED', 'DENIED')),
    denial_reason TEXT,
    scanned_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX gate_scans_booking_idx ON gate_scans (booking_id, scanned_at);

-- Platform outbox for ports.booking.v1 / ports.gate.v1 envelopes. Published
-- at-least-once by the outbox publisher; event_id is the Kafka idempotence key.
CREATE TABLE platform_outbox (
    event_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    topic TEXT NOT NULL CHECK (topic IN ('ports.booking.v1', 'ports.gate.v1')),
    event_type TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    UNIQUE (topic, idempotency_key)
);
CREATE INDEX platform_outbox_unpublished_idx ON platform_outbox (created_at) WHERE published_at IS NULL;

-- NSW authority ingress replay protection (see internal/nswsecurity).
CREATE TABLE nsw_ingress_replay (
    replay_hash TEXT PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX nsw_ingress_replay_expiry_idx ON nsw_ingress_replay (expires_at);

-- Tenant isolation for the new business tables, matching migration 0008.
ALTER TABLE port_terminals ENABLE ROW LEVEL SECURITY;
ALTER TABLE port_terminals FORCE ROW LEVEL SECURITY;
CREATE POLICY port_terminals_tenant_policy ON port_terminals
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE terminal_slots ENABLE ROW LEVEL SECURITY;
ALTER TABLE terminal_slots FORCE ROW LEVEL SECURITY;
CREATE POLICY terminal_slots_tenant_policy ON terminal_slots
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE truck_bookings ENABLE ROW LEVEL SECURITY;
ALTER TABLE truck_bookings FORCE ROW LEVEL SECURITY;
CREATE POLICY truck_bookings_tenant_policy ON truck_bookings
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE booking_payment_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE booking_payment_intents FORCE ROW LEVEL SECURITY;
CREATE POLICY booking_payment_intents_tenant_policy ON booking_payment_intents
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE gate_scans ENABLE ROW LEVEL SECURITY;
ALTER TABLE gate_scans FORCE ROW LEVEL SECURITY;
CREATE POLICY gate_scans_tenant_policy ON gate_scans
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
