CREATE TABLE port_call_document_supersessions (
    supersession_id UUID PRIMARY KEY,
    call_id TEXT NOT NULL REFERENCES port_calls(call_id),
    original_document_id UUID NOT NULL REFERENCES port_call_documents(document_id),
    replacement_document_id UUID NOT NULL REFERENCES port_call_documents(document_id),
    reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 1024),
    superseded_by TEXT NOT NULL,
    superseded_at TIMESTAMPTZ NOT NULL,
    UNIQUE (original_document_id),
    UNIQUE (replacement_document_id),
    CHECK (original_document_id <> replacement_document_id)
);

CREATE TABLE port_call_clearance_amendments (
    amendment_id UUID PRIMARY KEY,
    call_id TEXT NOT NULL REFERENCES port_calls(call_id),
    prior_decision_id UUID NOT NULL REFERENCES port_call_clearance_decisions(decision_id),
    prior_decision TEXT NOT NULL CHECK (prior_decision IN ('APPROVED','REJECTED')),
    amended_decision TEXT NOT NULL CHECK (amended_decision IN ('APPROVED','REJECTED')),
    reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 1024),
    amended_by TEXT NOT NULL,
    call_version BIGINT NOT NULL CHECK (call_version > 0),
    amended_at TIMESTAMPTZ NOT NULL,
    CHECK (prior_decision <> amended_decision)
);

ALTER TABLE port_call_outbox DROP CONSTRAINT IF EXISTS port_call_outbox_event_type_check;
ALTER TABLE port_call_outbox ADD CONSTRAINT port_call_outbox_event_type_check CHECK (event_type IN (
 'port_call.created','port_call.status_changed','port_call.document_declared','port_call.document_reviewed','port_call.clearance_decided','port_call.document_superseded','port_call.clearance_amended'
));
