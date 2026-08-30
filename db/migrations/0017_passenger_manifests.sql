-- API/BRI passenger manifest ingest for cruise and ferry international calls
-- (WCO/IATA/ICAO 2023 Guidelines on API and BRI for Cruise Ship Travel).
-- Manifests arrive as signed envelope v1.0 artifacts (FHIR R4 message Bundle
-- wrap, JWS-EdDSA over RFC 8785 JCS — the platform envelope scheme). Every
-- record lands either in passenger_manifest_records or in
-- passenger_manifest_rejections with a machine-readable reason: no silent
-- drops. Envelope-level failures (bad signature, malformed artifact) are
-- quarantined as envelope rejections without trusting the payload.

CREATE TABLE passenger_manifests (
    manifest_id UUID PRIMARY KEY,           -- the envelope eventId
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    authority_kid TEXT NOT NULL CHECK (length(authority_kid) BETWEEN 2 AND 128),
    principal_id TEXT NOT NULL CHECK (length(principal_id) BETWEEN 2 AND 256),
    manifest_reference TEXT NOT NULL CHECK (length(manifest_reference) BETWEEN 2 AND 128),
    voyage_reference TEXT NOT NULL CHECK (length(voyage_reference) BETWEEN 2 AND 128),
    call_reference TEXT NOT NULL CHECK (length(call_reference) BETWEEN 2 AND 64),
    manifest_kind TEXT NOT NULL CHECK (manifest_kind IN ('CRUISE', 'FERRY')),
    vessel_imo TEXT NOT NULL CHECK (vessel_imo ~ '^[0-9]{7}$'),
    status TEXT NOT NULL CHECK (status IN ('ACCEPTED', 'ACCEPTED_WITH_REJECTIONS', 'REJECTED')),
    records_total INTEGER NOT NULL CHECK (records_total >= 0),
    records_accepted INTEGER NOT NULL CHECK (records_accepted >= 0),
    records_rejected INTEGER NOT NULL CHECK (records_rejected >= 0),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^sha256:[0-9a-f]{64}$'),
    payload JSONB NOT NULL,
    received_by TEXT NOT NULL CHECK (length(received_by) BETWEEN 2 AND 256),
    received_at TIMESTAMPTZ NOT NULL,
    CHECK (records_total = records_accepted + records_rejected)
);
CREATE INDEX passenger_manifests_call_idx ON passenger_manifests (tenant_id, call_reference, received_at);

-- Accepted records, one row per manifest line.
CREATE TABLE passenger_manifest_records (
    record_seq BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    manifest_id UUID NOT NULL REFERENCES passenger_manifests(manifest_id),
    record_index INTEGER NOT NULL CHECK (record_index >= 0),
    record_type TEXT NOT NULL CHECK (record_type IN ('PAX', 'CREW')),
    family_name TEXT NOT NULL CHECK (length(family_name) BETWEEN 1 AND 128),
    given_name TEXT NOT NULL CHECK (length(given_name) BETWEEN 1 AND 128),
    date_of_birth DATE NOT NULL,
    nationality TEXT NOT NULL CHECK (nationality ~ '^[A-Z]{2,3}$'),
    document_number TEXT NOT NULL CHECK (document_number ~ '^[A-Z0-9-]{4,24}$'),
    sex TEXT CHECK (sex IS NULL OR sex IN ('M', 'F', 'X')),
    UNIQUE (manifest_id, record_index)
);
CREATE INDEX passenger_manifest_records_document_idx ON passenger_manifest_records (tenant_id, document_number);

-- The rejection queue: per-record rejections with reasons, plus envelope-
-- level quarantine entries (record_index NULL, no trusted payload). Nothing
-- is silently dropped — every unaccepted record is explained here.
CREATE TABLE passenger_manifest_rejections (
    rejection_seq BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    manifest_id UUID REFERENCES passenger_manifests(manifest_id),
    envelope_event_id UUID,
    record_index INTEGER,
    reason_code TEXT NOT NULL CHECK (reason_code ~ '^[A-Z][A-Z0-9_]{2,63}$'),
    reason_detail TEXT NOT NULL CHECK (length(reason_detail) BETWEEN 1 AND 512),
    raw_record JSONB,
    rejected_at TIMESTAMPTZ NOT NULL,
    CHECK (record_index IS NULL OR record_index >= 0)
);
CREATE INDEX passenger_manifest_rejections_manifest_idx ON passenger_manifest_rejections (manifest_id, record_index);
CREATE INDEX passenger_manifest_rejections_envelope_idx ON passenger_manifest_rejections (envelope_event_id);

-- Tenant isolation, matching migrations 0008/0009.
ALTER TABLE passenger_manifests ENABLE ROW LEVEL SECURITY;
ALTER TABLE passenger_manifests FORCE ROW LEVEL SECURITY;
CREATE POLICY passenger_manifests_tenant_policy ON passenger_manifests
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE passenger_manifest_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE passenger_manifest_records FORCE ROW LEVEL SECURITY;
CREATE POLICY passenger_manifest_records_tenant_policy ON passenger_manifest_records
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE passenger_manifest_rejections ENABLE ROW LEVEL SECURITY;
ALTER TABLE passenger_manifest_rejections FORCE ROW LEVEL SECURITY;
CREATE POLICY passenger_manifest_rejections_tenant_policy ON passenger_manifest_rejections
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
