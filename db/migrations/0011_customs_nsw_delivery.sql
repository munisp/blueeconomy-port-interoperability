-- Nigeria Customs cross-validation for eCallUp bookings and the NSW outbound
-- delivery ledger. Additive only: new nullable booking columns, two new
-- booking states, two new tables, and a widened capacity trigger.

-- A booking may reference a Nigeria Customs cargo declaration; when it does,
-- the declared weight, consignee and operator identities are mandatory so the
-- customs validator can cross-check them.
ALTER TABLE truck_bookings ADD COLUMN cargo_declaration_ref TEXT;
ALTER TABLE truck_bookings ADD COLUMN declared_weight_kg BIGINT;
ALTER TABLE truck_bookings ADD COLUMN consignee_id TEXT;
ALTER TABLE truck_bookings ADD COLUMN operator_id TEXT;
ALTER TABLE truck_bookings
    ADD CONSTRAINT truck_bookings_declaration_ref_check
    CHECK (cargo_declaration_ref IS NULL OR cargo_declaration_ref ~ '^[A-Z0-9][A-Z0-9/-]{3,63}$');
ALTER TABLE truck_bookings
    ADD CONSTRAINT truck_bookings_declaration_complete_check
    CHECK (cargo_declaration_ref IS NULL OR (
        declared_weight_kg IS NOT NULL AND declared_weight_kg > 0 AND
        consignee_id IS NOT NULL AND length(consignee_id) BETWEEN 2 AND 128 AND
        operator_id IS NOT NULL AND length(operator_id) BETWEEN 2 AND 128
    ));

-- Customs gate states: VALIDATION_PENDING holds the booking (and its slot
-- capacity) while the declaration cross-check runs; REJECTED is the
-- fail-closed mismatch outcome.
ALTER TABLE truck_bookings DROP CONSTRAINT truck_bookings_status_check;
ALTER TABLE truck_bookings
    ADD CONSTRAINT truck_bookings_status_check
    CHECK (status IN (
        'DRAFTED', 'PENDING_SYNC', 'SLOT_RESERVED', 'PAID', 'VALIDATION_PENDING',
        'GATE_APPROVED', 'COMPLETED', 'CANCELLED', 'EXPIRED', 'REJECTED',
        'RECONCILIATION_REQUIRED'
    ));

-- VALIDATION_PENDING still occupies terminal slot capacity: the booking is
-- paused, not released. Replace the guard to keep no-overbooking airtight.
CREATE OR REPLACE FUNCTION enforce_slot_capacity() RETURNS trigger AS $$
DECLARE
    slot_capacity INTEGER;
    active_count INTEGER;
BEGIN
    IF NEW.slot_id IS NULL OR NEW.status NOT IN ('SLOT_RESERVED', 'PAID', 'VALIDATION_PENDING', 'GATE_APPROVED') THEN
        RETURN NEW;
    END IF;
    SELECT capacity INTO slot_capacity FROM terminal_slots WHERE slot_id = NEW.slot_id FOR UPDATE;
    IF slot_capacity IS NULL THEN
        RAISE EXCEPTION 'slot % does not exist', NEW.slot_id;
    END IF;
    SELECT count(*) INTO active_count FROM truck_bookings
    WHERE slot_id = NEW.slot_id
      AND status IN ('SLOT_RESERVED', 'PAID', 'VALIDATION_PENDING', 'GATE_APPROVED')
      AND booking_id <> NEW.booking_id;
    IF active_count >= slot_capacity THEN
        RAISE EXCEPTION 'slot % is at capacity %', NEW.slot_id, slot_capacity
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Append-only record of every Nigeria Customs cross-validation decision. A
-- MISMATCH row is the evidence trail behind a REJECTED booking; a MATCH row
-- is the gate-approval prerequisite for declaration-carrying bookings.
CREATE TABLE customs_validations (
    validation_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    booking_id UUID NOT NULL REFERENCES truck_bookings(booking_id),
    declaration_ref TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('MATCH', 'MISMATCH')),
    reason_code TEXT NOT NULL,
    customs_status TEXT,
    customs_weight_kg BIGINT,
    booking_weight_kg BIGINT,
    consignee_id TEXT,
    operator_id TEXT,
    validated_by TEXT NOT NULL CHECK (length(validated_by) BETWEEN 2 AND 256),
    validated_at TIMESTAMPTZ NOT NULL,
    CHECK ((decision = 'MATCH' AND reason_code = '') OR (decision = 'MISMATCH' AND reason_code <> ''))
);
CREATE INDEX customs_validations_booking_idx ON customs_validations (booking_id, validated_at);

ALTER TABLE customs_validations ENABLE ROW LEVEL SECURITY;
ALTER TABLE customs_validations FORCE ROW LEVEL SECURITY;
CREATE POLICY customs_validations_tenant_policy ON customs_validations
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- NSW outbound delivery ledger: every NSW-relevant outbox event is handed to
-- the NSW operator endpoint at-least-once. Rows move PENDING -> DELIVERED or
-- PENDING -> FAILED_PERMANENT after max_attempts; nothing is silently dropped.
CREATE TABLE nsw_delivery (
    delivery_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    source TEXT NOT NULL CHECK (source IN ('platform_outbox', 'port_call_outbox')),
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    call_reference TEXT NOT NULL,
    content_type TEXT NOT NULL CHECK (content_type IN ('application/json', 'application/xml')),
    payload TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^sha256:[0-9a-f]{64}$'),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'DELIVERED', 'FAILED_PERMANENT')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    next_attempt_at TIMESTAMPTZ NOT NULL,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    delivered_at TIMESTAMPTZ,
    UNIQUE (source, event_id),
    CHECK (status <> 'DELIVERED' OR delivered_at IS NOT NULL)
);
CREATE INDEX nsw_delivery_due_idx ON nsw_delivery (next_attempt_at) WHERE status = 'PENDING';

ALTER TABLE nsw_delivery ENABLE ROW LEVEL SECURITY;
ALTER TABLE nsw_delivery FORCE ROW LEVEL SECURITY;
CREATE POLICY nsw_delivery_tenant_policy ON nsw_delivery
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
