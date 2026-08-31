-- eCallUp 2.0 truck call-up queue: per-terminal FIFO with priority classes,
-- DB-enforced call-up capacity, grace-window forfeiture and tenant RLS.
-- The platform outbox additionally accepts the ports.queue.v1 topic.

-- Per-terminal call-up capacity: at most this many queue requests may hold
-- CALLED_UP/EN_ROUTE at once. Enforced by trigger below.
ALTER TABLE port_terminals
    ADD COLUMN queue_capacity INTEGER NOT NULL DEFAULT 1 CHECK (queue_capacity > 0);

CREATE TABLE truck_queue_requests (
    queue_request_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 8 AND 128),
    booking_id UUID REFERENCES truck_bookings(booking_id),
    truck_plate TEXT NOT NULL CHECK (truck_plate ~ '^[A-Z0-9][A-Z0-9-]{2,15}$'),
    trucker_msisdn TEXT NOT NULL CHECK (trucker_msisdn ~ '^\+[0-9]{8,15}$'),
    terminal_id TEXT NOT NULL REFERENCES port_terminals(terminal_id),
    priority_class TEXT NOT NULL CHECK (priority_class IN ('STANDARD', 'PERISHABLE', 'PRIORITY')),
    status TEXT NOT NULL CHECK (status IN (
        'REQUESTED', 'QUEUED', 'CALLED_UP', 'EN_ROUTE', 'ARRIVED',
        'EXPIRED', 'FORFEITED', 'CANCELLED', 'RECONCILIATION_REQUIRED'
    )),
    position BIGINT CHECK (position > 0),
    called_up_at TIMESTAMPTZ,
    grace_deadline TIMESTAMPTZ,
    arrived_at TIMESTAMPTZ,
    gate_id TEXT,
    forfeit_reason TEXT,
    reconciliation_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    UNIQUE (tenant_id, idempotency_key),
    CHECK (status NOT IN ('QUEUED', 'CALLED_UP', 'EN_ROUTE', 'ARRIVED') OR position IS NOT NULL),
    CHECK (status NOT IN ('CALLED_UP', 'EN_ROUTE') OR grace_deadline IS NOT NULL),
    CHECK (status <> 'ARRIVED' OR arrived_at IS NOT NULL)
);
-- One winner per terminal position: the position sequence is assigned under
-- the terminal row lock and this index rejects any duplicate that survives.
CREATE UNIQUE INDEX truck_queue_position_idx ON truck_queue_requests (terminal_id, position) WHERE position IS NOT NULL;
CREATE INDEX truck_queue_terminal_idx ON truck_queue_requests (terminal_id, status, position) WHERE status IN ('QUEUED', 'CALLED_UP', 'EN_ROUTE');
CREATE INDEX truck_queue_booking_idx ON truck_queue_requests (booking_id) WHERE booking_id IS NOT NULL;
CREATE INDEX truck_queue_grace_idx ON truck_queue_requests (grace_deadline) WHERE status IN ('CALLED_UP', 'EN_ROUTE');

-- DB-enforced call-up capacity: a queue request may only hold CALLED_UP or
-- EN_ROUTE while the terminal has remaining call-up capacity. The trigger
-- re-counts under the terminal row lock taken by the promoting transaction,
-- so concurrent promotions can never exceed queue_capacity.
CREATE FUNCTION enforce_terminal_callup_capacity() RETURNS trigger AS $$
DECLARE
    terminal_capacity INTEGER;
    active_count INTEGER;
BEGIN
    IF NEW.status NOT IN ('CALLED_UP', 'EN_ROUTE') THEN
        RETURN NEW;
    END IF;
    SELECT queue_capacity INTO terminal_capacity FROM port_terminals WHERE terminal_id = NEW.terminal_id FOR UPDATE;
    IF terminal_capacity IS NULL THEN
        RAISE EXCEPTION 'terminal % does not exist', NEW.terminal_id;
    END IF;
    SELECT count(*) INTO active_count FROM truck_queue_requests
    WHERE terminal_id = NEW.terminal_id
      AND status IN ('CALLED_UP', 'EN_ROUTE')
      AND queue_request_id <> NEW.queue_request_id;
    IF active_count >= terminal_capacity THEN
        RAISE EXCEPTION 'terminal % call-up capacity % exhausted', NEW.terminal_id, terminal_capacity
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER truck_queue_callup_capacity_guard
    BEFORE INSERT OR UPDATE OF status ON truck_queue_requests
    FOR EACH ROW EXECUTE FUNCTION enforce_terminal_callup_capacity();

-- The queue lifecycle publishes ports.queue.v1 envelopes through the same
-- transactional outbox as booking and gate events.
ALTER TABLE platform_outbox DROP CONSTRAINT platform_outbox_topic_check;
ALTER TABLE platform_outbox
    ADD CONSTRAINT platform_outbox_topic_check CHECK (topic IN ('ports.booking.v1', 'ports.gate.v1', 'ports.queue.v1'));

-- Tenant isolation matching migration 0008/0009.
ALTER TABLE truck_queue_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE truck_queue_requests FORCE ROW LEVEL SECURITY;
CREATE POLICY truck_queue_requests_tenant_policy ON truck_queue_requests
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
