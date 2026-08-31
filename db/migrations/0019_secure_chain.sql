-- Secure Chain (WP-7): Portbase-style PIN-free, verified-chain digital
-- container release. A shipping line registers release authority over a
-- bill-of-lading digest, opens a release chain for a container and each link
-- holder explicitly nominates the next organisation (line -> forwarder ->
-- transporter). The terminal releases the container only to the verified
-- chain tail, presenting a short-TTL single-use signed authorization token.
-- No PINs or shared secrets exist anywhere in the design: actor identity is
-- the verified OIDC/gateway subject, links are hash-chained and append-only,
-- and the single-active-tail invariant is DB-enforced.

-- B/L release-authority registry: only the shipping line recorded here may
-- open a chain for the container/B-L pair. The digest is the SHA-256 (hex)
-- of the carrier manifest record; the plaintext B/L never leaves the line.
CREATE TABLE secure_chain_bl_registry (
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    bl_digest TEXT NOT NULL CHECK (bl_digest ~ '^[0-9a-f]{64}$'),
    container_id TEXT NOT NULL CHECK (container_id ~ '^[A-Z]{4}[0-9]{7}$'),
    shipping_line_org TEXT NOT NULL CHECK (length(shipping_line_org) BETWEEN 2 AND 128),
    registered_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, bl_digest)
);

CREATE TABLE secure_chains (
    chain_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 8 AND 128),
    container_id TEXT NOT NULL CHECK (container_id ~ '^[A-Z]{4}[0-9]{7}$'),
    bl_digest TEXT NOT NULL CHECK (bl_digest ~ '^[0-9a-f]{64}$'),
    issuer_org TEXT NOT NULL CHECK (length(issuer_org) BETWEEN 2 AND 128),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'COMPLETED', 'REVOKED', 'EXPIRED')),
    -- velocity_hold is the fail-closed anti-fraud hold: while set, no
    -- nomination, acceptance or release authorization is possible.
    velocity_hold BOOLEAN NOT NULL DEFAULT FALSE,
    hold_reason TEXT,
    revoke_reason TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    UNIQUE (tenant_id, idempotency_key),
    CHECK (status = 'ACTIVE' OR velocity_hold = FALSE OR hold_reason IS NOT NULL),
    CHECK (status <> 'REVOKED' OR revoke_reason IS NOT NULL)
);
-- One ACTIVE chain per container per tenant: a container can never have two
-- competing release chains.
CREATE UNIQUE INDEX secure_chains_active_container_idx
    ON secure_chains (tenant_id, container_id) WHERE status = 'ACTIVE';
CREATE INDEX secure_chains_expiry_idx ON secure_chains (expires_at) WHERE status = 'ACTIVE';

-- Chain links are append-only and hash-chained:
-- link_hash = sha256(prev_link_hash || JCS(link identity fields)). seq and
-- prev_hash are checked by trigger against the previous link, so a forked
-- or rewritten history is rejected by the database itself.
CREATE TABLE secure_chain_links (
    chain_id UUID NOT NULL REFERENCES secure_chains(chain_id),
    seq BIGINT NOT NULL CHECK (seq > 0),
    from_org TEXT NOT NULL CHECK (length(from_org) BETWEEN 2 AND 128),
    to_org TEXT NOT NULL CHECK (length(to_org) BETWEEN 2 AND 128),
    nominated_by TEXT NOT NULL,
    nominated_at TIMESTAMPTZ NOT NULL,
    accepted_at TIMESTAMPTZ,
    declined_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    decline_reason TEXT,
    prev_hash TEXT NOT NULL CHECK (prev_hash ~ '^[0-9a-f]{64}$'),
    link_hash TEXT NOT NULL CHECK (link_hash ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (chain_id, seq),
    -- A link resolves exactly once.
    CHECK ((accepted_at IS NOT NULL)::int + (declined_at IS NOT NULL)::int + (revoked_at IS NOT NULL)::int <= 1),
    CHECK (declined_at IS NULL OR decline_reason IS NOT NULL),
    CHECK (from_org <> to_org)
);
-- Single-active-tail invariant, part 1: at most one unresolved (PENDING)
-- link per chain. A new nomination is impossible until the open nomination
-- is accepted, declined or revoked.
CREATE UNIQUE INDEX secure_chain_links_pending_idx ON secure_chain_links (chain_id)
    WHERE accepted_at IS NULL AND declined_at IS NULL AND revoked_at IS NULL;

-- Single-active-tail invariant, part 2: a link may only be appended by the
-- current tail holder. The tail is the issuer for seq 1; afterwards it is
-- the accepted nominee, or the nominator when the open nomination was
-- declined. The trigger also pins seq/prev_hash to the hash chain.
CREATE FUNCTION secure_chain_link_guard() RETURNS trigger AS $$
DECLARE
    chain_record secure_chains%ROWTYPE;
    previous secure_chain_links%ROWTYPE;
BEGIN
    IF TG_OP = 'INSERT' THEN
        SELECT * INTO chain_record FROM secure_chains WHERE chain_id = NEW.chain_id FOR UPDATE;
        IF chain_record.chain_id IS NULL THEN
            RAISE EXCEPTION 'secure chain % does not exist', NEW.chain_id USING ERRCODE = 'foreign_key_violation';
        END IF;
        IF chain_record.status <> 'ACTIVE' THEN
            RAISE EXCEPTION 'secure chain % is %; links are appendable only while ACTIVE', NEW.chain_id, chain_record.status
                USING ERRCODE = 'check_violation';
        END IF;
        SELECT * INTO previous FROM secure_chain_links
            WHERE chain_id = NEW.chain_id ORDER BY seq DESC LIMIT 1;
        IF NOT FOUND THEN
            IF NEW.seq <> 1 THEN
                RAISE EXCEPTION 'first secure-chain link must have seq 1' USING ERRCODE = 'check_violation';
            END IF;
            IF NEW.from_org <> chain_record.issuer_org THEN
                RAISE EXCEPTION 'first secure-chain link must be nominated by the issuer %', chain_record.issuer_org
                    USING ERRCODE = 'check_violation';
            END IF;
            IF NEW.prev_hash <> repeat('0', 64) THEN
                RAISE EXCEPTION 'first secure-chain link must anchor on the zero hash' USING ERRCODE = 'check_violation';
            END IF;
        ELSE
            IF NEW.seq <> previous.seq + 1 THEN
                RAISE EXCEPTION 'secure-chain link seq % does not follow %', NEW.seq, previous.seq
                    USING ERRCODE = 'check_violation';
            END IF;
            IF NEW.prev_hash <> previous.link_hash THEN
                RAISE EXCEPTION 'secure-chain link % breaks the hash chain', NEW.seq USING ERRCODE = 'check_violation';
            END IF;
            IF previous.accepted_at IS NULL AND previous.declined_at IS NULL AND previous.revoked_at IS NULL THEN
                RAISE EXCEPTION 'secure chain % already has a pending nomination', NEW.chain_id
                    USING ERRCODE = 'check_violation';
            END IF;
            IF previous.accepted_at IS NOT NULL AND NEW.from_org <> previous.to_org THEN
                RAISE EXCEPTION 'secure-chain tail is %; % cannot nominate', previous.to_org, NEW.from_org
                    USING ERRCODE = 'check_violation';
            END IF;
            IF previous.declined_at IS NOT NULL AND NEW.from_org <> previous.from_org THEN
                RAISE EXCEPTION 'declined secure-chain nomination returns the tail to %; % cannot nominate', previous.from_org, NEW.from_org
                    USING ERRCODE = 'check_violation';
            END IF;
            IF previous.revoked_at IS NOT NULL THEN
                RAISE EXCEPTION 'secure chain % is revoked at the tail; no further links', NEW.chain_id
                    USING ERRCODE = 'check_violation';
            END IF;
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        -- Identity and hash-chain fields are immutable: history cannot be
        -- rewritten, only resolved (accept/decline/revoke timestamps).
        IF NEW.seq <> OLD.seq OR NEW.from_org <> OLD.from_org OR NEW.to_org <> OLD.to_org
           OR NEW.nominated_by <> OLD.nominated_by OR NEW.nominated_at <> OLD.nominated_at
           OR NEW.prev_hash <> OLD.prev_hash OR NEW.link_hash <> OLD.link_hash
           OR NEW.chain_id <> OLD.chain_id THEN
            RAISE EXCEPTION 'secure-chain link identity and hash-chain fields are immutable'
                USING ERRCODE = 'raise_exception';
        END IF;
        -- Accept/decline are only valid from PENDING; revoke may also close
        -- an accepted link (cascade). Each resolves exactly once (CHECK).
        IF NEW.accepted_at IS NOT NULL AND OLD.accepted_at IS NULL
           AND (OLD.declined_at IS NOT NULL OR OLD.revoked_at IS NOT NULL) THEN
            RAISE EXCEPTION 'secure-chain link already resolved; cannot accept' USING ERRCODE = 'check_violation';
        END IF;
        IF NEW.declined_at IS NOT NULL AND OLD.declined_at IS NULL
           AND (OLD.accepted_at IS NOT NULL OR OLD.revoked_at IS NOT NULL) THEN
            RAISE EXCEPTION 'secure-chain link already resolved; cannot decline' USING ERRCODE = 'check_violation';
        END IF;
        IF NEW.revoked_at IS NOT NULL AND OLD.revoked_at IS NOT NULL THEN
            RAISE EXCEPTION 'secure-chain link already revoked' USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    ELSE
        RAISE EXCEPTION 'secure-chain links are append-only' USING ERRCODE = 'raise_exception';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER secure_chain_links_guard
    BEFORE INSERT OR UPDATE OR DELETE ON secure_chain_links
    FOR EACH ROW EXECUTE FUNCTION secure_chain_link_guard();

-- Short-TTL single-use release authorization tokens. The nonce is a random
-- 256-bit hex value; consumption is an atomic UPDATE guarded by
-- consumed_at IS NULL, so replay is rejected by the database.
CREATE TABLE secure_chain_tokens (
    nonce TEXT PRIMARY KEY CHECK (nonce ~ '^[0-9a-f]{64}$'),
    tenant_id TEXT NOT NULL REFERENCES platform_tenants(tenant_id),
    chain_id UUID NOT NULL REFERENCES secure_chains(chain_id),
    container_id TEXT NOT NULL,
    holder_org TEXT NOT NULL,
    token_jws TEXT NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    consumed_gate TEXT,
    CHECK (expires_at > issued_at),
    CHECK (consumed_at IS NULL OR consumed_gate IS NOT NULL)
);
CREATE INDEX secure_chain_tokens_chain_idx ON secure_chain_tokens (chain_id);

-- Hash-chained append-only audit ledger for every secure-chain event:
-- entry_hash = sha256(prev_entry_hash || canonical event payload).
CREATE TABLE secure_chain_audit (
    audit_seq BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    chain_id UUID NOT NULL REFERENCES secure_chains(chain_id),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    prev_hash TEXT NOT NULL CHECK (prev_hash ~ '^[0-9a-f]{64}$'),
    entry_hash TEXT NOT NULL CHECK (entry_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX secure_chain_audit_chain_idx ON secure_chain_audit (chain_id, audit_seq);

CREATE FUNCTION secure_chain_audit_guard() RETURNS trigger AS $$
DECLARE
    previous_hash TEXT;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'secure-chain audit ledger is append-only' USING ERRCODE = 'raise_exception';
    END IF;
    SELECT entry_hash INTO previous_hash FROM secure_chain_audit
        WHERE chain_id = NEW.chain_id ORDER BY audit_seq DESC LIMIT 1;
    IF previous_hash IS NULL THEN
        previous_hash := repeat('0', 64);
    END IF;
    IF NEW.prev_hash <> previous_hash THEN
        RAISE EXCEPTION 'secure-chain audit entry breaks the hash chain' USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER secure_chain_audit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON secure_chain_audit
    FOR EACH ROW EXECUTE FUNCTION secure_chain_audit_guard();

-- eCallUp integration: a truck booking may bind an import container; such
-- bookings are gated on the secure-chain tail-holder check in the booking
-- store (fail-closed when the verifier is unwired).
ALTER TABLE truck_bookings
    ADD COLUMN container_id TEXT CHECK (container_id IS NULL OR container_id ~ '^[A-Z]{4}[0-9]{7}$');
CREATE INDEX truck_bookings_container_idx ON truck_bookings (container_id) WHERE container_id IS NOT NULL;

-- The secure-chain lifecycle publishes ports.securechain.v1 envelopes
-- through the same transactional outbox as booking, gate and queue events.
ALTER TABLE platform_outbox DROP CONSTRAINT platform_outbox_topic_check;
ALTER TABLE platform_outbox
    ADD CONSTRAINT platform_outbox_topic_check
    CHECK (topic IN (
        'ports.booking.v1', 'ports.gate.v1', 'ports.queue.v1',
        'trade.declarations.v1', 'ports.offshore.v1', 'ports.manifests.v1',
        'ports.cruise.v1', 'finance.revenue-assessments.v1',
        'ports.securechain.v1'
    ));

-- Tenant isolation matching migration 0008.
ALTER TABLE secure_chain_bl_registry ENABLE ROW LEVEL SECURITY;
ALTER TABLE secure_chain_bl_registry FORCE ROW LEVEL SECURITY;
CREATE POLICY secure_chain_bl_registry_tenant_policy ON secure_chain_bl_registry
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE secure_chains ENABLE ROW LEVEL SECURITY;
ALTER TABLE secure_chains FORCE ROW LEVEL SECURITY;
CREATE POLICY secure_chains_tenant_policy ON secure_chains
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE secure_chain_links ENABLE ROW LEVEL SECURITY;
ALTER TABLE secure_chain_links FORCE ROW LEVEL SECURITY;
CREATE POLICY secure_chain_links_tenant_policy ON secure_chain_links
    USING (chain_id IN (SELECT chain_id FROM secure_chains));

ALTER TABLE secure_chain_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE secure_chain_tokens FORCE ROW LEVEL SECURITY;
CREATE POLICY secure_chain_tokens_tenant_policy ON secure_chain_tokens
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE secure_chain_audit ENABLE ROW LEVEL SECURITY;
ALTER TABLE secure_chain_audit FORCE ROW LEVEL SECURITY;
CREATE POLICY secure_chain_audit_tenant_policy ON secure_chain_audit
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
