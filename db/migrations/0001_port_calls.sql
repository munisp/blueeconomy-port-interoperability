CREATE TABLE port_calls (
    call_id TEXT PRIMARY KEY,
    vessel_imo TEXT NOT NULL CHECK (vessel_imo ~ '^[0-9]{7}$'),
    port_code TEXT NOT NULL CHECK (port_code ~ '^[A-Z]{2,8}$'),
    declaration_reference TEXT NOT NULL,
    submitted_by TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('DRAFT', 'SUBMITTED', 'ACCEPTED', 'REJECTED')),
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0)
);

CREATE TABLE port_call_outbox (
    event_id UUID PRIMARY KEY,
    call_id TEXT NOT NULL REFERENCES port_calls(call_id),
    event_type TEXT NOT NULL CHECK (event_type IN ('port_call.created', 'port_call.status_changed')),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ
);

CREATE INDEX port_call_outbox_unpublished_idx ON port_call_outbox (created_at) WHERE published_at IS NULL;
