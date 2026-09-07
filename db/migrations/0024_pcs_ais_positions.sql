-- Phase 16 PCS integrations (GAP-PCS-AIS): validated AIS position reports
-- ingested from the env-gated feed. The (mmsi, message_ts) primary key
-- makes ingestion idempotent under feed replays. Positions are
-- platform-level reference data (not tenant-scoped): MMSI is enforced as
-- a nine-digit ship-station identity, coordinates are bounded to the
-- WGS-84 envelope and speed/course to the AIS domain envelope at the
-- database layer, mirroring the application-layer geo data-integrity
-- validation in internal/pcs/ais.

CREATE TABLE pcs_ais_positions (
    mmsi TEXT NOT NULL CHECK (mmsi ~ '^[1-9][0-9]{8}$'),
    imo TEXT CHECK (imo IS NULL OR imo ~ '^[0-9]{7}$'),
    latitude DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    speed_knots DOUBLE PRECISION NOT NULL CHECK (speed_knots >= 0 AND speed_knots <= 102.2),
    course_degrees DOUBLE PRECISION NOT NULL CHECK (course_degrees >= 0 AND course_degrees < 360),
    heading INTEGER CHECK (heading IS NULL OR heading BETWEEN 0 AND 359),
    message_ts TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (mmsi, message_ts)
);

CREATE INDEX pcs_ais_positions_message_ts ON pcs_ais_positions (message_ts DESC);
CREATE INDEX pcs_ais_positions_imo ON pcs_ais_positions (imo) WHERE imo IS NOT NULL;
