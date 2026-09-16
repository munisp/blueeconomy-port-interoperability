-- Phase 19 port reception facility waste receipts (B15 of the scenario
-- matrix): MARPOL Annex I-VI waste delivery records linked to port calls.
-- The receipt documents a delivery of ship-generated waste to a named
-- reception facility; the receipt document itself is referenced by
-- id/URI only (storage lives in the evidence service or the facility's
-- own system — nothing is fabricated here). Lifecycle:
-- DRAFT → DELIVERED → VERIFIED, with maker-checker on verification
-- (verifier differs from the recorder). Lifecycle events are JWS-signed
-- into the platform outbox (ports.waste-receipts.v1) in the same
-- transaction as the mutation.

CREATE TABLE portcall_waste_receipts (
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    receipt_id TEXT NOT NULL CHECK (length(receipt_id) BETWEEN 1 AND 64),
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 256),
    port_call_id TEXT NOT NULL,
    -- MARPOL 73/78 annex the waste category falls under.
    marpol_annex TEXT NOT NULL CHECK (marpol_annex IN ('I', 'II', 'III', 'IV', 'V', 'VI')),
    -- Free-text waste description within the annex (e.g. 'oil residues
    -- (bilge)', 'food waste'); never an enumerated fabrication.
    waste_type TEXT NOT NULL CHECK (length(waste_type) BETWEEN 1 AND 256),
    quantity NUMERIC(14,3) NOT NULL CHECK (quantity > 0),
    unit TEXT NOT NULL CHECK (unit IN ('M3', 'TONNES', 'KG')),
    -- Reference identifying the reception facility (registry id or name
    -- as supplied); no facility master data is fabricated.
    facility_reference TEXT NOT NULL CHECK (length(facility_reference) BETWEEN 1 AND 256),
    -- Receipt document references only: an evidence-package id and/or a
    -- URI. Document storage is out of scope for this service.
    receipt_document_id TEXT CHECK (receipt_document_id IS NULL OR length(receipt_document_id) BETWEEN 1 AND 128),
    receipt_document_uri TEXT CHECK (receipt_document_uri IS NULL OR length(receipt_document_uri) BETWEEN 1 AND 1024),
    status TEXT NOT NULL CHECK (status IN ('DRAFT', 'DELIVERED', 'VERIFIED')),
    created_by TEXT NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
    delivered_by TEXT CHECK (delivered_by IS NULL OR length(delivered_by) BETWEEN 1 AND 256),
    verified_by TEXT CHECK (verified_by IS NULL OR length(verified_by) BETWEEN 1 AND 256),
    delivered_at TIMESTAMPTZ,
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    PRIMARY KEY (tenant_id, receipt_id),
    UNIQUE (idempotency_key),
    -- port_calls.call_id is globally unique (migration 0001); RLS on this
    -- table plus the tenant-scoped insert check in the store keep the
    -- linkage tenant-isolated.
    FOREIGN KEY (port_call_id) REFERENCES port_calls (call_id),
    -- Status/timestamp coherence.
    CHECK ((status = 'DRAFT' AND delivered_at IS NULL AND verified_at IS NULL)
           OR (status = 'DELIVERED' AND delivered_at IS NOT NULL AND verified_at IS NULL)
           OR (status = 'VERIFIED' AND delivered_at IS NOT NULL AND verified_at IS NOT NULL)),
    -- Maker-checker: the verifying officer is never the recorder.
    CHECK (verified_by IS NULL OR verified_by <> created_by)
);
CREATE INDEX portcall_waste_receipts_call_idx
    ON portcall_waste_receipts (tenant_id, port_call_id);

-- Tenant isolation matching migration 0008.
ALTER TABLE portcall_waste_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE portcall_waste_receipts FORCE ROW LEVEL SECURITY;
CREATE POLICY portcall_waste_receipts_tenant_policy ON portcall_waste_receipts
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
