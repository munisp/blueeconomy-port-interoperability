CREATE TABLE port_agency_profiles (
    profile_id TEXT NOT NULL CHECK (profile_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    version TEXT NOT NULL CHECK (version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    agency_code TEXT NOT NULL CHECK (agency_code ~ '^[A-Z]{2,16}$'),
    active BOOLEAN NOT NULL,
    profile_sha256 TEXT NOT NULL CHECK (profile_sha256 ~ '^sha256:[0-9a-f]{64}$'),
    registered_by TEXT NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (profile_id, version)
);

ALTER TABLE port_calls
    ADD COLUMN agency_profile_id TEXT,
    ADD COLUMN agency_profile_version TEXT,
    ADD CONSTRAINT port_calls_agency_profile_fk
        FOREIGN KEY (agency_profile_id, agency_profile_version)
        REFERENCES port_agency_profiles(profile_id, version);
