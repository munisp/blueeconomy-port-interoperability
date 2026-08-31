-- Mobile push-notification device tokens. A verified gateway subject
-- (user) registers the current push token for each of its devices via
-- POST /v1/push-tokens; tokens are revoked explicitly (device logout /
-- token rollover) via POST /v1/push-tokens/revoke. There is no platform
-- event surface here: device registration is operational plumbing, not a
-- ledger fact, so no outbox/envelope emission is involved.
--
-- Scope: (tenant, user, device). actor identity is the verified
-- OIDC/gateway subject — never a request-body user id.

CREATE TABLE push_tokens (
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    user_id TEXT NOT NULL CHECK (length(user_id) BETWEEN 1 AND 256),
    device_id TEXT NOT NULL CHECK (length(device_id) BETWEEN 1 AND 256),
    -- Provider-issued push token (FCM/APNs registration token).
    token TEXT NOT NULL CHECK (length(token) BETWEEN 8 AND 4096),
    platform TEXT NOT NULL CHECK (platform IN ('android', 'ios', 'web')),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'REVOKED')),
    registered_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, user_id, device_id),
    CHECK (status = 'ACTIVE' OR revoked_at IS NOT NULL)
);
-- A provider token identifies one active installation: at most one ACTIVE
-- row per (tenant, token). Re-registration moves the token, revoking the
-- previous holder's row in the same transaction.
CREATE UNIQUE INDEX push_tokens_active_token_idx
    ON push_tokens (tenant_id, token) WHERE status = 'ACTIVE';
CREATE INDEX push_tokens_user_idx
    ON push_tokens (tenant_id, user_id) WHERE status = 'ACTIVE';

-- Tenant isolation matching migration 0008.
ALTER TABLE push_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE push_tokens FORCE ROW LEVEL SECURITY;
CREATE POLICY push_tokens_tenant_policy ON push_tokens
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
