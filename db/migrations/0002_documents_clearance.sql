CREATE TABLE port_call_documents (
    document_id UUID PRIMARY KEY,
    call_id TEXT NOT NULL REFERENCES port_calls(call_id),
    document_type TEXT NOT NULL CHECK (document_type ~ '^[A-Za-z][A-Za-z0-9._:-]{0,63}$'),
    media_type TEXT NOT NULL CHECK (media_type ~ '^[A-Za-z0-9.+-]+/[A-Za-z0-9.+-]+$'),
    size_bytes BIGINT NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 104857600),
    sha256 TEXT NOT NULL CHECK (sha256 ~ '^sha256:[0-9a-f]{64}$'),
    declared_by TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('DECLARED', 'VERIFIED', 'REJECTED')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (call_id, document_type, sha256)
);

CREATE INDEX port_call_documents_call_idx ON port_call_documents (call_id, created_at);

CREATE TABLE port_call_clearance_decisions (
    decision_id UUID PRIMARY KEY,
    call_id TEXT NOT NULL UNIQUE REFERENCES port_calls(call_id),
    decision TEXT NOT NULL CHECK (decision IN ('APPROVED', 'REJECTED')),
    reason TEXT NOT NULL,
    decided_by TEXT NOT NULL,
    call_version BIGINT NOT NULL CHECK (call_version > 0),
    decided_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE port_call_outbox
    DROP CONSTRAINT IF EXISTS port_call_outbox_event_type_check;
ALTER TABLE port_call_outbox
    ADD CONSTRAINT port_call_outbox_event_type_check
    CHECK (event_type IN ('port_call.created', 'port_call.status_changed', 'port_call.document_declared', 'port_call.clearance_decided'));
