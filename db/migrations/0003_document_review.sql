ALTER TABLE port_call_documents
    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS reviewed_by TEXT,
    ADD COLUMN IF NOT EXISTS reviewed_reason TEXT,
    ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ;

ALTER TABLE port_call_documents
    DROP CONSTRAINT IF EXISTS port_call_documents_version_check;
ALTER TABLE port_call_documents
    ADD CONSTRAINT port_call_documents_version_check CHECK (version > 0);

ALTER TABLE port_call_documents
    DROP CONSTRAINT IF EXISTS port_call_documents_review_fields_check;
ALTER TABLE port_call_documents
    ADD CONSTRAINT port_call_documents_review_fields_check
    CHECK ((status = 'DECLARED' AND reviewed_by IS NULL AND reviewed_at IS NULL) OR (status IN ('VERIFIED', 'REJECTED') AND reviewed_by IS NOT NULL AND reviewed_at IS NOT NULL));

ALTER TABLE port_call_outbox
    DROP CONSTRAINT IF EXISTS port_call_outbox_event_type_check;
ALTER TABLE port_call_outbox
    ADD CONSTRAINT port_call_outbox_event_type_check
    CHECK (event_type IN ('port_call.created', 'port_call.status_changed', 'port_call.document_declared', 'port_call.document_reviewed', 'port_call.clearance_decided'));
