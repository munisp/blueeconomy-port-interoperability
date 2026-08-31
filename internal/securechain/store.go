package securechain

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/munisp/blueeconomy-port-interoperability/internal/telemetry"
	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantdb"
)

// Principal is the verified actor behind a secure-chain mutation; it becomes
// the provenance block of every emitted platform event.
type Principal = booking.Principal

const (
	// DefaultTokenTTL bounds the terminal-release authorization: a token the
	// gate never sees within the window dies instead of floating as a
	// replayable credential.
	DefaultTokenTTL = 15 * time.Minute
	// DefaultChainTTL bounds an open release chain.
	DefaultChainTTL = 30 * 24 * time.Hour
	// DefaultVelocityThreshold is the number of nominations inside 24h that
	// flags a chain for anomaly review (rapid re-nomination is the classic
	// chain-hijack / drug-extraction pattern).
	DefaultVelocityThreshold = 5
	// MaxDeclineReason / MaxRevokeReason bound free-text fields.
	MaxDeclineReason = 500
	MaxRevokeReason  = 500
)

// Config wires the fail-closed secure-chain policy knobs.
type Config struct {
	// TokenTTL is the release-token lifetime; 0 selects DefaultTokenTTL.
	TokenTTL time.Duration
	// VelocityThreshold is nominations-per-24h before flagging; 0 selects
	// DefaultVelocityThreshold, negative disables the velocity check.
	VelocityThreshold int
	// VelocityHold, when true, turns a velocity breach into a fail-closed
	// chain hold (no further nominations or releases until lifted).
	VelocityHold bool
}

type Store struct {
	pool   *pgxpool.Pool
	signer *events.Signer
	config Config
}

// NewStore fails closed without an envelope signer; the pool may only be nil
// in fail-closed constructor tests that never touch the database.
func NewStore(pool *pgxpool.Pool, signer *events.Signer, config Config) (*Store, error) {
	if signer == nil {
		return nil, errors.New("secure-chain store requires an envelope signer")
	}
	if config.TokenTTL < 0 {
		return nil, errors.New("release token TTL must not be negative")
	}
	if config.TokenTTL == 0 {
		config.TokenTTL = DefaultTokenTTL
	}
	if config.VelocityThreshold == 0 {
		config.VelocityThreshold = DefaultVelocityThreshold
	}
	return &Store{pool: pool, signer: signer, config: config}, nil
}

func Open(ctx context.Context, databaseURL string, signer *events.Signer, config Config) (*Store, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	if err := telemetry.ApplyPoolEnv(poolConfig); err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return NewStore(pool, signer, config)
}

func (store *Store) Close() { store.pool.Close() }

func (store *Store) Pool() *pgxpool.Pool { return store.pool }

func (store *Store) Exec(ctx context.Context, statement string) (int64, error) {
	result, err := store.pool.Exec(ctx, statement)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (store *Store) withTx(ctx context.Context, work func(pgx.Tx, tenantctx.Claims) error) error {
	return tenantdb.WithTx(ctx, store.pool, work)
}

// emit writes a FHIR-enveloped ports.securechain.v1 event into the
// transactional platform outbox inside the caller's transaction.
func (store *Store) emit(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, eventType, correlationID, subjectID string, payload any, extensions map[string]string, principal Principal, occurredAt time.Time) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s payload: %w", eventType, err)
	}
	envelope, err := events.Message(eventType, events.TopicSecureChain, correlationID, subjectID, payloadJSON, extensions, events.Provenance{
		PrincipalID:   principal.ID,
		PrincipalRole: principal.Role,
	}, occurredAt, store.signer)
	if err != nil {
		return fmt.Errorf("build %s envelope: %w", eventType, err)
	}
	envelopeJSON, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode %s envelope: %w", eventType, err)
	}
	eventID, err := uuid.Parse(envelope.EventID)
	if err != nil {
		return fmt.Errorf("parse event id: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO platform_outbox (event_id, tenant_id, topic, event_type, idempotency_key, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		eventID, claims.TenantID, events.TopicSecureChain, eventType, envelope.EventID, envelopeJSON, occurredAt); err != nil {
		return fmt.Errorf("write %s outbox event: %w", eventType, err)
	}
	return nil
}

// audit appends a hash-chained entry to the secure-chain audit ledger. The
// caller must hold the chain row lock so prev-hash selection serializes.
func audit(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, chainID, eventType string, payload any, occurredAt time.Time) error {
	payloadJSON, err := json.Marshal(map[string]any{
		"eventType":  eventType,
		"occurredAt": occurredAt.UTC().Format(time.RFC3339Nano),
		"detail":     payload,
	})
	if err != nil {
		return fmt.Errorf("encode audit payload: %w", err)
	}
	var prevHash string
	err = tx.QueryRow(ctx, `
		SELECT entry_hash FROM secure_chain_audit
		WHERE chain_id = $1 ORDER BY audit_seq DESC LIMIT 1`, chainID).Scan(&prevHash)
	if errors.Is(err, pgx.ErrNoRows) {
		prevHash = ZeroHash
	} else if err != nil {
		return fmt.Errorf("read audit tail: %w", err)
	}
	entryHash, err := AuditHash(prevHash, payloadJSON)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO secure_chain_audit (tenant_id, chain_id, event_type, payload, prev_hash, entry_hash, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		claims.TenantID, chainID, eventType, payloadJSON, prevHash, entryHash, occurredAt); err != nil {
		return fmt.Errorf("append %s audit entry: %w", eventType, err)
	}
	return nil
}

const chainColumns = `chain_id, tenant_id, container_id, bl_digest, issuer_org, status,
	velocity_hold, hold_reason, revoke_reason, expires_at, created_at, updated_at, version`

func scanChain(row pgx.Row) (Chain, error) {
	var chain Chain
	err := row.Scan(&chain.ChainID, &chain.TenantID, &chain.ContainerID, &chain.BLDigest,
		&chain.IssuerOrg, &chain.Status, &chain.VelocityHold, &chain.HoldReason, &chain.RevokeReason,
		&chain.ExpiresAt, &chain.CreatedAt, &chain.UpdatedAt, &chain.Version)
	return chain, err
}

// tail is the current verified holder of the chain: the issuer before any
// link, the accepted nominee afterwards, or the nominator again after a
// decline. pendingLink, when non-nil, is the open nomination blocking any
// further nomination or release.
type tail struct {
	holderOrg   string
	pendingLink *Link
	lastSeq     int64
	lastHash    string
}

// tailOf resolves the chain tail; the caller must hold the chain row lock.
func tailOf(ctx context.Context, tx pgx.Tx, chain Chain) (tail, error) {
	rows, err := tx.Query(ctx, `
		SELECT seq, from_org, to_org, nominated_by, nominated_at, accepted_at, declined_at, revoked_at, prev_hash, link_hash
		FROM secure_chain_links WHERE chain_id = $1 ORDER BY seq`, chain.ChainID)
	if err != nil {
		return tail{}, fmt.Errorf("load chain links: %w", err)
	}
	defer rows.Close()
	holder := chain.IssuerOrg
	result := tail{holderOrg: holder, lastSeq: 0, lastHash: ZeroHash}
	for rows.Next() {
		var link Link
		link.ChainID = chain.ChainID
		if err := rows.Scan(&link.Seq, &link.FromOrg, &link.ToOrg, &link.NominatedBy, &link.NominatedAt,
			&link.AcceptedAt, &link.DeclinedAt, &link.RevokedAt, &link.PrevHash, &link.LinkHash); err != nil {
			return tail{}, fmt.Errorf("scan chain link: %w", err)
		}
		result.lastSeq = link.Seq
		result.lastHash = link.LinkHash
		switch link.State() {
		case LinkPending:
			pending := link
			result.pendingLink = &pending
			// Tail stays with the nominator while the nomination is open.
			result.holderOrg = link.FromOrg
		case LinkAccepted:
			result.pendingLink = nil
			result.holderOrg = link.ToOrg
		case LinkDeclined:
			result.pendingLink = nil
			result.holderOrg = link.FromOrg
		case LinkRevoked:
			result.pendingLink = nil
			result.holderOrg = ""
		}
	}
	return result, rows.Err()
}

// lockChain loads the chain row FOR UPDATE, serializing every mutation.
func lockChain(ctx context.Context, tx pgx.Tx, chainID string) (Chain, error) {
	chain, err := scanChain(tx.QueryRow(ctx, `
		SELECT `+chainColumns+` FROM secure_chains WHERE chain_id = $1 FOR UPDATE`, chainID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Chain{}, ErrNotFound
	}
	if err != nil {
		return Chain{}, fmt.Errorf("lock secure chain: %w", err)
	}
	return chain, nil
}

// RegisterBLAuthority records the shipping line's release authority over a
// bill-of-lading digest (SHA-256 of the carrier manifest record). The
// calling organisation is the verified token subject — never a body field.
func (store *Store) RegisterBLAuthority(ctx context.Context, containerID, blDigest string, principal Principal) error {
	containerID = strings.ToUpper(strings.TrimSpace(containerID))
	if !ValidContainerID(containerID) {
		return errors.New("container id must be an ISO 6346 number")
	}
	if !ValidDigest(blDigest) {
		return errors.New("bill-of-lading digest must be a SHA-256 hex digest")
	}
	return store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO secure_chain_bl_registry (tenant_id, bl_digest, container_id, shipping_line_org, registered_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (tenant_id, bl_digest) DO UPDATE
			SET container_id = EXCLUDED.container_id, registered_at = EXCLUDED.registered_at
			WHERE secure_chain_bl_registry.shipping_line_org = EXCLUDED.shipping_line_org`,
			claims.TenantID, blDigest, containerID, claims.Subject, time.Now().UTC()); err != nil {
			return fmt.Errorf("register B/L release authority: %w", err)
		}
		_ = principal
		return nil
	})
}

// CreateChain opens a release chain. The caller must hold release authority
// for the B/L digest (registered by the same verified organisation).
func (store *Store) CreateChain(ctx context.Context, idempotencyKey, containerID, blDigest string, expiresAt time.Time, principal Principal) (Chain, error) {
	containerID = strings.ToUpper(strings.TrimSpace(containerID))
	if !ValidContainerID(containerID) {
		return Chain{}, errors.New("container id must be an ISO 6346 number")
	}
	if !ValidDigest(blDigest) {
		return Chain{}, errors.New("bill-of-lading digest must be a SHA-256 hex digest")
	}
	if strings.TrimSpace(idempotencyKey) == "" || len(idempotencyKey) > 128 {
		return Chain{}, errors.New("idempotency key is required (8-128 chars)")
	}
	now := time.Now().UTC()
	if !expiresAt.After(now) {
		return Chain{}, errors.New("chain expiry must be in the future")
	}
	if expiresAt.After(now.Add(DefaultChainTTL)) {
		return Chain{}, fmt.Errorf("chain expiry exceeds the %s maximum", DefaultChainTTL)
	}
	var created Chain
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		// Release-authority check: the verified subject must be the shipping
		// line recorded for this B/L digest and container.
		var authority string
		err := tx.QueryRow(ctx, `
			SELECT shipping_line_org FROM secure_chain_bl_registry
			WHERE bl_digest = $1 AND container_id = $2`, blDigest, containerID).Scan(&authority)
		if errors.Is(err, pgx.ErrNoRows) || authority != claims.Subject {
			return ErrNoReleaseAuthority
		}
		if err != nil {
			return fmt.Errorf("check B/L release authority: %w", err)
		}
		// Idempotent replay: same key and same intent returns the retained chain.
		existing, err := scanChain(tx.QueryRow(ctx, `
			SELECT `+chainColumns+` FROM secure_chains WHERE tenant_id = $1 AND idempotency_key = $2`,
			claims.TenantID, idempotencyKey))
		if err == nil {
			if existing.ContainerID != containerID || existing.BLDigest != blDigest {
				return ErrIdempotencyConflict
			}
			created = existing
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("check idempotency key: %w", err)
		}
		chainID := uuid.NewString()
		created, err = scanChain(tx.QueryRow(ctx, `
			INSERT INTO secure_chains (chain_id, tenant_id, idempotency_key, container_id, bl_digest, issuer_org,
				status, expires_at, created_at, updated_at, version)
			VALUES ($1, $2, $3, $4, $5, $6, 'ACTIVE', $7, $8, $8, 1)
			RETURNING `+chainColumns,
			chainID, claims.TenantID, idempotencyKey, containerID, blDigest, claims.Subject, expiresAt, now))
		if err != nil {
			if strings.Contains(err.Error(), "secure_chains_active_container_idx") {
				return fmt.Errorf("%w: an ACTIVE secure chain already exists for container %s", ErrInvalidTransition, containerID)
			}
			return fmt.Errorf("create secure chain: %w", err)
		}
		if err := audit(ctx, tx, claims, chainID, EventChainCreated, map[string]any{
			"containerId": containerID, "blDigest": blDigest, "issuerOrg": claims.Subject,
		}, now); err != nil {
			return err
		}
		return store.emit(ctx, tx, claims, EventChainCreated, chainID, containerID, map[string]string{
			"chain_id": chainID, "container_id": containerID, "bl_digest": blDigest, "issuer_org": claims.Subject,
		}, map[string]string{"container": containerID, "issuer": claims.Subject}, principal, now)
	})
	return created, err
}

// chainByContainer loads an ACTIVE chain with its links (read path).
func (store *Store) chainByContainer(ctx context.Context, containerID string) (Chain, error) {
	containerID = strings.ToUpper(strings.TrimSpace(containerID))
	var chain Chain
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		var err error
		chain, err = scanChain(tx.QueryRow(ctx, `
			SELECT `+chainColumns+` FROM secure_chains WHERE container_id = $1
			ORDER BY created_at DESC LIMIT 1`, containerID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("load secure chain: %w", err)
		}
		links, err := listLinks(ctx, tx, chain.ChainID)
		if err != nil {
			return err
		}
		chain.Links = links
		return nil
	})
	return chain, err
}

// GetByContainer returns the most recent chain for a container, with links.
func (store *Store) GetByContainer(ctx context.Context, containerID string) (Chain, error) {
	return store.chainByContainer(ctx, containerID)
}

// AuditTrail returns the hash-chained audit ledger of a chain.
func (store *Store) AuditTrail(ctx context.Context, chainID string) ([]AuditEntry, error) {
	var entries []AuditEntry
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		rows, err := tx.Query(ctx, `
			SELECT audit_seq, chain_id, event_type, payload, prev_hash, entry_hash, created_at
			FROM secure_chain_audit WHERE chain_id = $1 ORDER BY audit_seq`, chainID)
		if err != nil {
			return fmt.Errorf("load audit trail: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var entry AuditEntry
			if err := rows.Scan(&entry.AuditSeq, &entry.ChainID, &entry.EventType, &entry.Payload,
				&entry.PrevHash, &entry.EntryHash, &entry.CreatedAt); err != nil {
				return fmt.Errorf("scan audit entry: %w", err)
			}
			entries = append(entries, entry)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, ErrNotFound
	}
	return entries, nil
}

func listLinks(ctx context.Context, tx pgx.Tx, chainID string) ([]Link, error) {
	rows, err := tx.Query(ctx, `
		SELECT seq, from_org, to_org, nominated_by, nominated_at, accepted_at, declined_at, revoked_at, prev_hash, link_hash
		FROM secure_chain_links WHERE chain_id = $1 ORDER BY seq`, chainID)
	if err != nil {
		return nil, fmt.Errorf("load chain links: %w", err)
	}
	defer rows.Close()
	var links []Link
	for rows.Next() {
		var link Link
		link.ChainID = chainID
		if err := rows.Scan(&link.Seq, &link.FromOrg, &link.ToOrg, &link.NominatedBy, &link.NominatedAt,
			&link.AcceptedAt, &link.DeclinedAt, &link.RevokedAt, &link.PrevHash, &link.LinkHash); err != nil {
			return nil, fmt.Errorf("scan chain link: %w", err)
		}
		links = append(links, link)
	}
	return links, rows.Err()
}

// Nominate appends a link from the current tail holder to the next
// organisation. The nominator is the verified token subject; a body-supplied
// from_org is never trusted.
func (store *Store) Nominate(ctx context.Context, chainID, toOrg string, principal Principal) (Link, error) {
	if !ValidOrg(toOrg) {
		return Link{}, errors.New("nominated organisation is not a valid org identifier")
	}
	var nominated Link
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		chain, err := lockChain(ctx, tx, chainID)
		if err != nil {
			return err
		}
		if chain.Status != StatusActive {
			return fmt.Errorf("%w: chain is %s", ErrInvalidTransition, chain.Status)
		}
		if chain.VelocityHold {
			return ErrVelocityHold
		}
		if chain.ExpiresAt.Before(time.Now().UTC()) {
			return fmt.Errorf("%w: chain is past its expiry", ErrInvalidTransition)
		}
		chainTail, err := tailOf(ctx, tx, chain)
		if err != nil {
			return err
		}
		if chainTail.pendingLink != nil {
			return fmt.Errorf("%w: a nomination is already pending", ErrInvalidTransition)
		}
		if chainTail.holderOrg == "" || chainTail.holderOrg != claims.Subject {
			return ErrNotTailHolder
		}
		if toOrg == claims.Subject {
			return fmt.Errorf("%w: self-nomination is not a handoff", ErrInvalidTransition)
		}
		// timestamptz stores microseconds; truncate so the link hash is
		// recomputable from the persisted row byte-for-byte.
		now := time.Now().UTC().Truncate(time.Microsecond)
		// Velocity anomaly hook: too many nominations in 24h flags the chain
		// (and, when configured, clamps a fail-closed hold on it). The
		// flag/hold commit MUST survive the refused nomination, so the
		// breach escapes this transaction and is applied separately below.
		if store.config.VelocityThreshold > 0 {
			var recent int
			if err := tx.QueryRow(ctx, `
				SELECT count(*) FROM secure_chain_links
				WHERE chain_id = $1 AND nominated_at > $2`, chainID, now.Add(-24*time.Hour)).Scan(&recent); err != nil {
				return fmt.Errorf("count recent nominations: %w", err)
			}
			if recent+1 > store.config.VelocityThreshold {
				if store.config.VelocityHold {
					return errVelocityBreach
				}
				if err := store.flagVelocity(ctx, tx, claims, chain, recent+1, principal, now); err != nil {
					return err
				}
			}
		}
		seq := chainTail.lastSeq + 1
		linkHash, err := LinkHash(chainTail.lastHash, chainID, seq, claims.Subject, toOrg, principal.ID, now)
		if err != nil {
			return err
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO secure_chain_links (chain_id, seq, from_org, to_org, nominated_by, nominated_at, prev_hash, link_hash)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			RETURNING seq, from_org, to_org, nominated_by, nominated_at, prev_hash, link_hash`,
			chainID, seq, claims.Subject, toOrg, principal.ID, now, chainTail.lastHash, linkHash).
			Scan(&nominated.Seq, &nominated.FromOrg, &nominated.ToOrg, &nominated.NominatedBy,
				&nominated.NominatedAt, &nominated.PrevHash, &nominated.LinkHash)
		if err != nil {
			return fmt.Errorf("append chain link: %w", err)
		}
		nominated.ChainID = chainID
		if err := audit(ctx, tx, claims, chainID, EventLinkNominated, map[string]any{
			"seq": seq, "fromOrg": claims.Subject, "toOrg": toOrg, "linkHash": linkHash,
		}, now); err != nil {
			return err
		}
		return store.emit(ctx, tx, claims, EventLinkNominated, chainID, chain.ContainerID, map[string]string{
			"chain_id": chainID, "seq": fmt.Sprintf("%d", seq), "from_org": claims.Subject, "to_org": toOrg,
		}, map[string]string{"container": chain.ContainerID, "toOrg": toOrg}, principal, now)
	})
	if errors.Is(err, errVelocityBreach) {
		// The refused nomination rolled back; the velocity flag and the
		// fail-closed hold must still commit, in their own transaction.
		if holdErr := store.applyVelocityHold(ctx, chainID, principal); holdErr != nil {
			return Link{}, fmt.Errorf("apply velocity hold: %w", holdErr)
		}
		return Link{}, ErrVelocityHold
	}
	return nominated, err
}

// errVelocityBreach escapes the nomination transaction so the fail-closed
// hold commits independently of the refused write.
var errVelocityBreach = errors.New("secure-chain velocity threshold breached")

// applyVelocityHold flags the velocity anomaly and clamps the hold in a
// dedicated transaction (the nomination that triggered it was refused and
// rolled back).
func (store *Store) applyVelocityHold(ctx context.Context, chainID string, principal Principal) error {
	return store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		chain, err := lockChain(ctx, tx, chainID)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		var recent int
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM secure_chain_links
			WHERE chain_id = $1 AND nominated_at > $2`, chainID, now.Add(-24*time.Hour)).Scan(&recent); err != nil {
			return fmt.Errorf("count recent nominations: %w", err)
		}
		return store.flagVelocity(ctx, tx, claims, chain, recent, principal, now)
	})
}

// flagVelocity records the velocity anomaly: audit entry, outbox event and,
// when fail-closed hold is configured, the chain hold.
func (store *Store) flagVelocity(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, chain Chain, nominations int, principal Principal, now time.Time) error {
	reason := fmt.Sprintf("velocity anomaly: %d nominations within 24h (threshold %d)", nominations, store.config.VelocityThreshold)
	if store.config.VelocityHold {
		if _, err := tx.Exec(ctx, `
			UPDATE secure_chains SET velocity_hold = TRUE, hold_reason = $2, updated_at = $3, version = version + 1
			WHERE chain_id = $1 AND velocity_hold = FALSE`, chain.ChainID, reason, now); err != nil {
			return fmt.Errorf("apply velocity hold: %w", err)
		}
	}
	if err := audit(ctx, tx, claims, chain.ChainID, EventVelocityFlagRaised, map[string]any{
		"nominations24h": nominations, "threshold": store.config.VelocityThreshold, "hold": store.config.VelocityHold,
	}, now); err != nil {
		return err
	}
	return store.emit(ctx, tx, claims, EventVelocityFlagRaised, chain.ChainID, chain.ContainerID, map[string]string{
		"chain_id": chain.ChainID, "reason": reason, "hold": fmt.Sprintf("%t", store.config.VelocityHold),
	}, map[string]string{"container": chain.ContainerID}, principal, now)
}

// resolveLink applies accept/decline to the pending link of a chain.
func (store *Store) resolveLink(ctx context.Context, chainID string, seq int64, accept bool, reason string, principal Principal) (Link, error) {
	var resolved Link
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		chain, err := lockChain(ctx, tx, chainID)
		if err != nil {
			return err
		}
		if chain.Status != StatusActive {
			return fmt.Errorf("%w: chain is %s", ErrInvalidTransition, chain.Status)
		}
		if chain.VelocityHold {
			return ErrVelocityHold
		}
		chainTail, err := tailOf(ctx, tx, chain)
		if err != nil {
			return err
		}
		pending := chainTail.pendingLink
		if pending == nil || pending.Seq != seq {
			return fmt.Errorf("%w: no pending nomination at seq %d", ErrInvalidTransition, seq)
		}
		// Only the nominated organisation may accept or decline.
		if pending.ToOrg != claims.Subject {
			return ErrNotAuthorizedParty
		}
		now := time.Now().UTC()
		eventType := EventLinkAccepted
		if accept {
			err = tx.QueryRow(ctx, `
				UPDATE secure_chain_links SET accepted_at = $3
				WHERE chain_id = $1 AND seq = $2
				RETURNING seq, from_org, to_org, nominated_by, nominated_at, accepted_at, prev_hash, link_hash`,
				chainID, seq, now).
				Scan(&resolved.Seq, &resolved.FromOrg, &resolved.ToOrg, &resolved.NominatedBy,
					&resolved.NominatedAt, &resolved.AcceptedAt, &resolved.PrevHash, &resolved.LinkHash)
		} else {
			if strings.TrimSpace(reason) == "" || len(reason) > MaxDeclineReason {
				return fmt.Errorf("a decline reason (1-%d chars) is required", MaxDeclineReason)
			}
			eventType = EventLinkDeclined
			err = tx.QueryRow(ctx, `
				UPDATE secure_chain_links SET declined_at = $3, decline_reason = $4
				WHERE chain_id = $1 AND seq = $2
				RETURNING seq, from_org, to_org, nominated_by, nominated_at, declined_at, prev_hash, link_hash`,
				chainID, seq, now, reason).
				Scan(&resolved.Seq, &resolved.FromOrg, &resolved.ToOrg, &resolved.NominatedBy,
					&resolved.NominatedAt, &resolved.DeclinedAt, &resolved.PrevHash, &resolved.LinkHash)
		}
		if err != nil {
			return fmt.Errorf("resolve chain link: %w", err)
		}
		resolved.ChainID = chainID
		if err := audit(ctx, tx, claims, chainID, eventType, map[string]any{
			"seq": seq, "fromOrg": resolved.FromOrg, "toOrg": resolved.ToOrg, "linkHash": resolved.LinkHash,
		}, now); err != nil {
			return err
		}
		return store.emit(ctx, tx, claims, eventType, chainID, chain.ContainerID, map[string]string{
			"chain_id": chainID, "seq": fmt.Sprintf("%d", seq), "from_org": resolved.FromOrg, "to_org": resolved.ToOrg,
		}, map[string]string{"container": chain.ContainerID}, principal, now)
	})
	return resolved, err
}

// Accept lets the nominated organisation accept the pending link.
func (store *Store) Accept(ctx context.Context, chainID string, seq int64, principal Principal) (Link, error) {
	return store.resolveLink(ctx, chainID, seq, true, "", principal)
}

// Decline lets the nominated organisation decline; the tail returns to the
// nominator, who may nominate someone else.
func (store *Store) Decline(ctx context.Context, chainID string, seq int64, reason string, principal Principal) (Link, error) {
	return store.resolveLink(ctx, chainID, seq, false, reason, principal)
}

// Revoke cascades: the chain dies, every unresolved or accepted link is
// revoked, outstanding release tokens die with it (they validate against
// chain status at the gate). Only the issuer may revoke the chain.
func (store *Store) Revoke(ctx context.Context, chainID, reason string, principal Principal) (Chain, error) {
	if strings.TrimSpace(reason) == "" || len(reason) > MaxRevokeReason {
		return Chain{}, fmt.Errorf("a revoke reason (1-%d chars) is required", MaxRevokeReason)
	}
	var revoked Chain
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		chain, err := lockChain(ctx, tx, chainID)
		if err != nil {
			return err
		}
		if chain.IssuerOrg != claims.Subject {
			return fmt.Errorf("%w: only the issuing shipping line may revoke", ErrNotAuthorizedParty)
		}
		if chain.Status != StatusActive {
			return fmt.Errorf("%w: chain is %s", ErrInvalidTransition, chain.Status)
		}
		now := time.Now().UTC()
		// Cascade: unresolved (pending) links are revoked; accepted links
		// remain as hash-chained history of a now-dead chain (a link
		// resolves exactly once, DB-enforced).
		if _, err := tx.Exec(ctx, `
			UPDATE secure_chain_links SET revoked_at = $2
			WHERE chain_id = $1 AND revoked_at IS NULL AND accepted_at IS NULL AND declined_at IS NULL`, chainID, now); err != nil {
			return fmt.Errorf("revoke chain links: %w", err)
		}
		revoked, err = scanChain(tx.QueryRow(ctx, `
			UPDATE secure_chains SET status = 'REVOKED', revoke_reason = $2, updated_at = $3, version = version + 1
			WHERE chain_id = $1 RETURNING `+chainColumns, chainID, reason, now))
		if err != nil {
			return fmt.Errorf("revoke secure chain: %w", err)
		}
		if err := audit(ctx, tx, claims, chainID, EventChainRevoked, map[string]any{
			"reason": reason, "revokedBy": claims.Subject,
		}, now); err != nil {
			return err
		}
		return store.emit(ctx, tx, claims, EventChainRevoked, chainID, chain.ContainerID, map[string]string{
			"chain_id": chainID, "reason": reason,
		}, map[string]string{"container": chain.ContainerID}, principal, now)
	})
	return revoked, err
}

// ReleaseAuthorization issues the terminal-release token. It succeeds only
// for the verified tail holder of an ACTIVE chain with no pending
// nomination — this is the check the terminal gate depends on. The token is
// an envelope-signed JWS carrying a random single-use nonce and short TTL.
func (store *Store) ReleaseAuthorization(ctx context.Context, containerID string, principal Principal) (Token, error) {
	containerID = strings.ToUpper(strings.TrimSpace(containerID))
	if !ValidContainerID(containerID) {
		return Token{}, errors.New("container id must be an ISO 6346 number")
	}
	var token Token
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		var chain Chain
		var err error
		chain, err = scanChain(tx.QueryRow(ctx, `
			SELECT `+chainColumns+` FROM secure_chains WHERE container_id = $1 AND status = 'ACTIVE'
			FOR UPDATE`, containerID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("lock secure chain: %w", err)
		}
		if chain.VelocityHold {
			return ErrVelocityHold
		}
		if chain.ExpiresAt.Before(time.Now().UTC()) {
			return fmt.Errorf("%w: chain is past its expiry", ErrInvalidTransition)
		}
		chainTail, err := tailOf(ctx, tx, chain)
		if err != nil {
			return err
		}
		if chainTail.pendingLink != nil {
			return fmt.Errorf("%w: a nomination is still pending", ErrInvalidTransition)
		}
		if chainTail.holderOrg == "" || chainTail.holderOrg != claims.Subject {
			return ErrNotTailHolder
		}
		now := time.Now().UTC()
		nonceBytes := make([]byte, 32)
		if _, err := rand.Read(nonceBytes); err != nil {
			return fmt.Errorf("draw release nonce: %w", err)
		}
		nonce := hex.EncodeToString(nonceBytes)
		expiresAt := now.Add(store.config.TokenTTL)
		// The token is itself the signed release_authorized envelope: the
		// gate can verify the JWS offline and the nonce makes it single-use.
		payload, _ := json.Marshal(map[string]string{
			"nonce": nonce, "chain_id": chain.ChainID, "container_id": containerID,
			"holder_org": claims.Subject, "expires_at": expiresAt.UTC().Format(time.RFC3339Nano),
		})
		envelope, err := events.Message(EventReleaseAuthorized, events.TopicSecureChain, chain.ChainID, containerID,
			payload, map[string]string{"container": containerID, "holder": claims.Subject}, events.Provenance{
				PrincipalID:   principal.ID,
				PrincipalRole: principal.Role,
			}, now, store.signer)
		if err != nil {
			return fmt.Errorf("sign release authorization: %w", err)
		}
		envelopeJSON, err := json.Marshal(envelope)
		if err != nil {
			return fmt.Errorf("encode release authorization: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO secure_chain_tokens (nonce, tenant_id, chain_id, container_id, holder_org, token_jws, issued_at, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			nonce, claims.TenantID, chain.ChainID, containerID, claims.Subject, string(envelopeJSON), now, expiresAt); err != nil {
			return fmt.Errorf("persist release token: %w", err)
		}
		eventID, err := uuid.Parse(envelope.EventID)
		if err != nil {
			return fmt.Errorf("parse event id: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO platform_outbox (event_id, tenant_id, topic, event_type, idempotency_key, payload, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			eventID, claims.TenantID, events.TopicSecureChain, EventReleaseAuthorized, envelope.EventID, envelopeJSON, now); err != nil {
			return fmt.Errorf("write release_authorized outbox event: %w", err)
		}
		if err := audit(ctx, tx, claims, chain.ChainID, EventReleaseAuthorized, map[string]any{
			"nonce": nonce, "holderOrg": claims.Subject, "expiresAt": expiresAt.UTC().Format(time.RFC3339Nano),
		}, now); err != nil {
			return err
		}
		token = Token{
			Nonce: nonce, ChainID: chain.ChainID, ContainerID: containerID, HolderOrg: claims.Subject,
			TokenJWS: string(envelopeJSON), IssuedAt: now, ExpiresAt: expiresAt,
		}
		return nil
	})
	return token, err
}

// Consume redeems a release token at the gate. The nonce is consumed
// atomically (UPDATE ... WHERE consumed_at IS NULL), the embedded JWS is
// verified against the service signing key, and the chain transitions to
// COMPLETED. Any replay, expiry, wrong-chain or forged-token attempt is
// rejected fail-closed.
func (store *Store) Consume(ctx context.Context, nonce, gateID string, principal Principal) (Token, error) {
	if !digestPattern.MatchString(nonce) {
		return Token{}, ErrTokenInvalid
	}
	if strings.TrimSpace(gateID) == "" {
		return Token{}, errors.New("gate id is required")
	}
	var token Token
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		now := time.Now().UTC()
		// Atomic single-use claim: exactly one transaction wins this UPDATE.
		var chainID, containerID, holderOrg, tokenJWS string
		var issuedAt, expiresAt time.Time
		err := tx.QueryRow(ctx, `
			UPDATE secure_chain_tokens SET consumed_at = $2, consumed_gate = $3
			WHERE nonce = $1 AND consumed_at IS NULL AND expires_at > $2
			RETURNING chain_id, container_id, holder_org, token_jws, issued_at, expires_at`,
			nonce, now, gateID).
			Scan(&chainID, &containerID, &holderOrg, &tokenJWS, &issuedAt, &expiresAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTokenInvalid
		}
		if err != nil {
			return fmt.Errorf("consume release token: %w", err)
		}
		// Verify the signed artifact: alg, kid, canonical payload and the
		// Ed25519 signature must all check out against the service key.
		var envelope events.Envelope
		if err := json.Unmarshal([]byte(tokenJWS), &envelope); err != nil {
			return fmt.Errorf("%w: token is not a signed envelope", ErrTokenInvalid)
		}
		if err := events.Verify(envelope, store.signer.PublicKey()); err != nil {
			return fmt.Errorf("%w: %v", ErrTokenInvalid, err)
		}
		if envelope.EventType != EventReleaseAuthorized || envelope.CorrelationID != chainID {
			return fmt.Errorf("%w: token envelope does not match its chain", ErrTokenInvalid)
		}
		chain, err := lockChain(ctx, tx, chainID)
		if err != nil {
			return err
		}
		if chain.Status != StatusActive {
			return fmt.Errorf("%w: chain is %s", ErrInvalidTransition, chain.Status)
		}
		completed, err := scanChain(tx.QueryRow(ctx, `
			UPDATE secure_chains SET status = 'COMPLETED', updated_at = $2, version = version + 1
			WHERE chain_id = $1 AND status = 'ACTIVE' RETURNING `+chainColumns, chainID, now))
		if err != nil {
			return fmt.Errorf("complete secure chain: %w", err)
		}
		token = Token{
			Nonce: nonce, ChainID: chainID, ContainerID: containerID, HolderOrg: holderOrg,
			TokenJWS: tokenJWS, IssuedAt: issuedAt, ExpiresAt: expiresAt,
			ConsumedAt: &now, ConsumedGate: &gateID,
		}
		if err := audit(ctx, tx, claims, chainID, EventReleaseConsumed, map[string]any{
			"nonce": nonce, "holderOrg": holderOrg, "gateId": gateID,
		}, now); err != nil {
			return err
		}
		return store.emit(ctx, tx, claims, EventReleaseConsumed, chainID, containerID, map[string]string{
			"chain_id": chainID, "nonce": nonce, "holder_org": holderOrg, "gate_id": gateID,
		}, map[string]string{"container": completed.ContainerID, "gate": gateID}, principal, now)
	})
	return token, err
}

// ExpireDue retires every ACTIVE chain of the bound tenant whose expiry has
// passed; it returns the expired chain ids. The sweeper/workflow calls this
// once per tenant.
func (store *Store) ExpireDue(ctx context.Context, principal Principal) ([]string, error) {
	var expired []string
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		rows, err := tx.Query(ctx, `
			UPDATE secure_chains SET status = 'EXPIRED', updated_at = $1, version = version + 1
			WHERE status = 'ACTIVE' AND expires_at <= $1 RETURNING chain_id, container_id`, time.Now().UTC())
		if err != nil {
			return fmt.Errorf("expire due secure chains: %w", err)
		}
		defer rows.Close()
		type expiredChain struct{ id, container string }
		var due []expiredChain
		for rows.Next() {
			var entry expiredChain
			if err := rows.Scan(&entry.id, &entry.container); err != nil {
				return fmt.Errorf("scan expired chain: %w", err)
			}
			due = append(due, entry)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		now := time.Now().UTC()
		for _, entry := range due {
			if _, err := tx.Exec(ctx, `
				UPDATE secure_chain_links SET revoked_at = $2
				WHERE chain_id = $1 AND revoked_at IS NULL AND accepted_at IS NULL AND declined_at IS NULL`, entry.id, now); err != nil {
				return fmt.Errorf("expire chain links: %w", err)
			}
			if err := audit(ctx, tx, claims, entry.id, EventChainExpired, map[string]any{
				"containerId": entry.container,
			}, now); err != nil {
				return err
			}
			if err := store.emit(ctx, tx, claims, EventChainExpired, entry.id, entry.container, map[string]string{
				"chain_id": entry.id, "container_id": entry.container,
			}, map[string]string{"container": entry.container}, principal, now); err != nil {
				return err
			}
			expired = append(expired, entry.id)
		}
		return nil
	})
	return expired, err
}

// VerifyAuditTrail recomputes the hash chain of the audit ledger; any
// tampering (row edited or deleted out-of-band) breaks the recomputation.
func (store *Store) VerifyAuditTrail(ctx context.Context, chainID string) (bool, error) {
	entries, err := store.AuditTrail(ctx, chainID)
	if err != nil {
		return false, err
	}
	prev := ZeroHash
	for _, entry := range entries {
		if entry.PrevHash != prev {
			return false, nil
		}
		recomputed, err := AuditHash(entry.PrevHash, entry.Payload)
		if err != nil {
			return false, err
		}
		if recomputed != entry.EntryHash {
			return false, nil
		}
		prev = entry.EntryHash
	}
	return true, nil
}
