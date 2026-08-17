-- RELEASE COUPLING: deploy only with the S1 store revision that writes profile events
-- and reads effective activation state from port_agency_profile_events. Do not apply
-- this migration while an older Store.RegisterAgencyProfile implementation is running.
BEGIN;

LOCK TABLE port_calls IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE port_agency_profiles IN ACCESS EXCLUSIVE MODE;

-- A legacy call without a profile binding cannot be made compliant automatically.
-- Fail rather than inventing authority data or selecting an arbitrary profile version.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM port_calls
        WHERE agency_profile_id IS NULL OR agency_profile_version IS NULL
    ) THEN
        RAISE EXCEPTION 'cannot harden profile binding: port_calls contains null agency profile references';
    END IF;
END $$;

ALTER TABLE port_calls
    ALTER COLUMN agency_profile_id SET NOT NULL,
    ALTER COLUMN agency_profile_version SET NOT NULL;

-- The retained version table is immutable profile content. Its original composite
-- key remains the referential target for port_calls and preserves historical binding.
ALTER TABLE port_agency_profiles RENAME TO port_agency_profile_versions;

-- Each write to this table is an immutable activation-state event. There are no
-- UPDATE/DELETE privileges in the application role; triggers add database defense.
CREATE TABLE port_agency_profile_events (
    event_sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    profile_id TEXT NOT NULL,
    version TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('REGISTERED', 'ACTIVATED', 'DEACTIVATED')),
    active BOOLEAN NOT NULL,
    profile_sha256 TEXT NOT NULL CHECK (profile_sha256 ~ '^sha256:[0-9a-f]{64}$'),
    actor TEXT NOT NULL CHECK (actor = btrim(actor) AND actor <> ''),
    occurred_at TIMESTAMPTZ NOT NULL,
    reason TEXT,
    FOREIGN KEY (profile_id, version)
        REFERENCES port_agency_profile_versions(profile_id, version),
    CHECK (
        (event_type IN ('REGISTERED', 'ACTIVATED') AND active)
        OR (event_type = 'DEACTIVATED' AND NOT active)
    ),
    CHECK (reason IS NULL OR (reason = btrim(reason) AND reason <> ''))
);

-- Copy each present version as its first event. The statement does not fabricate
-- time, actor or digest: it uses the registry evidence already recorded in 0005.
INSERT INTO port_agency_profile_events (
    profile_id, version, event_type, active, profile_sha256, actor, occurred_at, reason
)
SELECT
    profile_id,
    version,
    'REGISTERED',
    active,
    profile_sha256,
    registered_by,
    registered_at,
    'migrated from 0005_agency_profiles'
FROM port_agency_profile_versions;

CREATE INDEX port_agency_profile_events_latest_idx
    ON port_agency_profile_events (profile_id, version, event_sequence DESC);

CREATE OR REPLACE FUNCTION reject_port_agency_profile_event_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'port_agency_profile_events is append-only; % is forbidden', TG_OP;
END;
$$;

CREATE TRIGGER port_agency_profile_events_no_update
    BEFORE UPDATE ON port_agency_profile_events
    FOR EACH ROW EXECUTE FUNCTION reject_port_agency_profile_event_mutation();

CREATE TRIGGER port_agency_profile_events_no_delete
    BEFORE DELETE ON port_agency_profile_events
    FOR EACH ROW EXECUTE FUNCTION reject_port_agency_profile_event_mutation();

CREATE OR REPLACE FUNCTION reject_port_agency_profile_version_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'port_agency_profile_versions is immutable; create a new version instead of %', TG_OP;
END;
$$;

CREATE TRIGGER port_agency_profile_versions_no_update
    BEFORE UPDATE ON port_agency_profile_versions
    FOR EACH ROW EXECUTE FUNCTION reject_port_agency_profile_version_mutation();

CREATE TRIGGER port_agency_profile_versions_no_delete
    BEFORE DELETE ON port_agency_profile_versions
    FOR EACH ROW EXECUTE FUNCTION reject_port_agency_profile_version_mutation();

-- Effective activation must be queried as:
-- SELECT active FROM port_agency_profile_events
-- WHERE profile_id=$1 AND version=$2
-- ORDER BY event_sequence DESC LIMIT 1 FOR SHARE;
COMMIT;
