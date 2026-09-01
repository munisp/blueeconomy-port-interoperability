// Package securechain implements the WP-7 Secure Chain: a Portbase-style
// PIN-free, verified-chain digital container release. A shipping line opens
// a release chain over a bill-of-lading digest; each link holder explicitly
// nominates the next organisation and the terminal releases the container
// only to the verified chain tail presenting a short-TTL single-use signed
// authorization token. There are no PIN codes or shared secrets anywhere:
// actor identity is the verified gateway/OIDC subject, links are hash-chained
// and append-only, and every state transition emits a signed envelope event
// plus a hash-chained audit entry.
package securechain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	jsoncanonicalizer "github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"

	"github.com/munisp/blueeconomy-port-interoperability/internal/containerid"
)

// Chain lifecycle. The DB CHECK constraint enforces the same set.
type Status string

const (
	StatusActive    Status = "ACTIVE"
	StatusCompleted Status = "COMPLETED"
	StatusRevoked   Status = "REVOKED"
	StatusExpired   Status = "EXPIRED"
)

// LinkState is derived from the resolution timestamps (append-only links
// carry no mutable status column).
type LinkState string

const (
	LinkPending  LinkState = "PENDING"
	LinkAccepted LinkState = "ACCEPTED"
	LinkDeclined LinkState = "DECLINED"
	LinkRevoked  LinkState = "REVOKED"
)

// Event types published on ports.securechain.v1 and recorded in the
// hash-chained audit ledger.
const (
	EventChainCreated       = "chain_created"
	EventLinkNominated      = "link_nominated"
	EventLinkAccepted       = "link_accepted"
	EventLinkDeclined       = "link_declined"
	EventChainRevoked       = "chain_revoked"
	EventChainExpired       = "chain_expired"
	EventReleaseAuthorized  = "release_authorized"
	EventReleaseConsumed    = "release_consumed"
	EventVelocityFlagRaised = "velocity_flagged"
)

var (
	ErrNotFound              = errors.New("secure chain resource not found")
	ErrInvalidTransition     = errors.New("invalid secure chain state transition")
	ErrNotTailHolder         = errors.New("caller does not hold the verified secure-chain tail")
	ErrNotAuthorizedParty    = errors.New("caller is not a party of the pending secure-chain link")
	ErrNoReleaseAuthority    = errors.New("caller holds no B/L release authority for this container")
	ErrIdempotencyConflict   = errors.New("request id conflicts with a retained secure chain")
	ErrVelocityHold          = errors.New("secure chain is on a fail-closed velocity hold")
	ErrTokenInvalid          = errors.New("release authorization token is invalid, expired or already consumed")
	ErrChainCompletedRelease = errors.New("secure chain already produced a consumed release")
)

var (
	digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	orgPattern    = regexp.MustCompile(`^[^|]{2,128}$`)
)

// ValidContainerID enforces ISO 6346 container numbers, recomputing the
// mandatory check digit (weighted 2^n mod 11) — a well-formed number with
// a wrong check digit is rejected fail-closed.
func ValidContainerID(value string) bool { return containerid.Valid(value) }

// ValidDigest enforces lower-case hex SHA-256 digests.
func ValidDigest(value string) bool { return digestPattern.MatchString(value) }

// ValidOrg bounds organisation identifiers; the pipe is excluded because it
// would corrupt the hash-chain preimage separator.
func ValidOrg(value string) bool { return orgPattern.MatchString(value) }

// Chain is the release-chain aggregate root.
type Chain struct {
	ChainID      string     `json:"chain_id"`
	TenantID     string     `json:"tenant_id"`
	ContainerID  string     `json:"container_id"`
	BLDigest     string     `json:"bl_digest"`
	IssuerOrg    string     `json:"issuer_org"`
	Status       Status     `json:"status"`
	VelocityHold bool       `json:"velocity_hold"`
	HoldReason   *string    `json:"hold_reason,omitempty"`
	RevokeReason *string    `json:"revoke_reason,omitempty"`
	ExpiresAt    time.Time  `json:"expires_at"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	Version      int64      `json:"version"`
	Links        []Link     `json:"links,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
}

// Link is one nomination edge in the chain (line -> forwarder ->
// transporter). Identity fields are immutable; resolution timestamps are
// write-once (DB-enforced).
type Link struct {
	ChainID       string     `json:"chain_id"`
	Seq           int64      `json:"seq"`
	FromOrg       string     `json:"from_org"`
	ToOrg         string     `json:"to_org"`
	NominatedBy   string     `json:"nominated_by"`
	NominatedAt   time.Time  `json:"nominated_at"`
	AcceptedAt    *time.Time `json:"accepted_at,omitempty"`
	DeclinedAt    *time.Time `json:"declined_at,omitempty"`
	RevokedAt     *time.Time `json:"revoked_at,omitempty"`
	DeclineReason *string    `json:"decline_reason,omitempty"`
	PrevHash      string     `json:"prev_hash"`
	LinkHash      string     `json:"link_hash"`
}

// State derives the link state from its resolution timestamps.
func (link Link) State() LinkState {
	switch {
	case link.AcceptedAt != nil:
		return LinkAccepted
	case link.DeclinedAt != nil:
		return LinkDeclined
	case link.RevokedAt != nil:
		return LinkRevoked
	default:
		return LinkPending
	}
}

// Token is the single-use terminal-release authorization.
type Token struct {
	Nonce        string     `json:"nonce"`
	ChainID      string     `json:"chain_id"`
	ContainerID  string     `json:"container_id"`
	HolderOrg    string     `json:"holder_org"`
	TokenJWS     string     `json:"token_jws"`
	IssuedAt     time.Time  `json:"issued_at"`
	ExpiresAt    time.Time  `json:"expires_at"`
	ConsumedAt   *time.Time `json:"consumed_at,omitempty"`
	ConsumedGate *string    `json:"consumed_gate,omitempty"`
}

// AuditEntry is one hash-chained append-only audit record.
type AuditEntry struct {
	AuditSeq  int64           `json:"audit_seq"`
	ChainID   string          `json:"chain_id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
	PrevHash  string          `json:"prev_hash"`
	EntryHash string          `json:"entry_hash"`
	CreatedAt time.Time       `json:"created_at"`
}

// ZeroHash anchors the first link and the first audit entry of a chain.
const ZeroHash = "0000000000000000000000000000000000000000000000000000000000000000"

// linkPreimage is the canonical identity content of a link. The link hash is
// sha256(prev_hash || "|" || JCS(linkPreimage)) — the JCS (RFC 8785) form is
// deterministic, so every party can recompute and verify the chain.
type linkPreimage struct {
	ChainID     string `json:"chainId"`
	Seq         int64  `json:"seq"`
	FromOrg     string `json:"fromOrg"`
	ToOrg       string `json:"toOrg"`
	NominatedBy string `json:"nominatedBy"`
	NominatedAt string `json:"nominatedAt"`
}

// LinkHash computes the hash-chain digest of a link identity.
func LinkHash(prevHash, chainID string, seq int64, fromOrg, toOrg, nominatedBy string, nominatedAt time.Time) (string, error) {
	if !digestPattern.MatchString(prevHash) {
		return "", fmt.Errorf("previous link hash %q is not a SHA-256 hex digest", prevHash)
	}
	preimage, err := json.Marshal(linkPreimage{
		ChainID:     chainID,
		Seq:         seq,
		FromOrg:     fromOrg,
		ToOrg:       toOrg,
		NominatedBy: nominatedBy,
		NominatedAt: nominatedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return "", fmt.Errorf("encode link preimage: %w", err)
	}
	canonical, err := jsoncanonicalizer.Transform(preimage)
	if err != nil {
		return "", fmt.Errorf("JCS-canonicalize link preimage: %w", err)
	}
	sum := sha256.Sum256(append([]byte(prevHash+"|"), canonical...))
	return hex.EncodeToString(sum[:]), nil
}

// AuditHash computes the audit ledger digest: sha256(prev || "|" ||
// JCS(payload)). eventType and occurredAt are bound into the payload by the
// caller before canonicalization.
func AuditHash(prevHash string, payload json.RawMessage) (string, error) {
	if !digestPattern.MatchString(prevHash) {
		return "", fmt.Errorf("previous audit hash %q is not a SHA-256 hex digest", prevHash)
	}
	if len(payload) == 0 || !json.Valid(payload) {
		return "", errors.New("audit payload must be valid JSON")
	}
	canonical, err := jsoncanonicalizer.Transform(payload)
	if err != nil {
		return "", fmt.Errorf("JCS-canonicalize audit payload: %w", err)
	}
	sum := sha256.Sum256(append([]byte(prevHash+"|"), canonical...))
	return hex.EncodeToString(sum[:]), nil
}
