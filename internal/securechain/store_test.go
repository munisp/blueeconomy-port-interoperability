package securechain

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// testSigner builds a throwaway envelope signer for store tests.
func testSigner(t *testing.T) *events.Signer {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}
	signer, err := events.NewSigner(key, "1")
	if err != nil {
		t.Fatalf("build test signer: %v", err)
	}
	return signer
}

// These tests run against a real PostgreSQL when SECURECHAIN_TEST_DATABASE_URL
// is set — see scripts/verify-local.sh and docker-compose.integration.yml.
// They are skipped otherwise; there is no in-memory substitute for the
// hash-chain triggers, the single-active-tail invariants and the atomic
// single-use token claim under test. A dedicated database keeps this package
// race-safe against other packages' schema resets.
type testEnv struct {
	store    *Store
	tenantID string
	ctx      context.Context
	cleanup  func()
}

func newTestEnv(t *testing.T, config Config) testEnv {
	t.Helper()
	databaseURL := os.Getenv("SECURECHAIN_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SECURECHAIN_TEST_DATABASE_URL is not set; skipping PostgreSQL-backed secure-chain tests")
	}
	ctx := context.Background()
	store, err := Open(ctx, databaseURL, testSigner(t), config)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if _, err := store.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset test schema: %v", err)
	}
	migrationDir := filepath.Join("..", "..", "db", "migrations")
	entries, err := filepath.Glob(filepath.Join(migrationDir, "*.sql"))
	if err != nil || len(entries) == 0 {
		t.Fatalf("find migrations: %v", err)
	}
	sort.Strings(entries)
	for _, entry := range entries {
		migration, err := os.ReadFile(entry)
		if err != nil {
			t.Fatalf("read migration %s: %v", entry, err)
		}
		if _, err := store.Exec(ctx, string(migration)); err != nil {
			t.Fatalf("apply migration %s: %v", entry, err)
		}
	}
	tenantID := fmt.Sprintf("tenant-sc-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := store.Pool().Exec(ctx, `INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ($1, $2)`, tenantID, "securechain-test-authority"); err != nil {
		t.Fatalf("insert test tenant: %v", err)
	}
	return testEnv{store: store, tenantID: tenantID, ctx: ctx, cleanup: store.Close}
}

// orgCtx binds a verified identity for an organisation; in production this
// comes from the Keycloak/OIDC gateway token — never from request bodies.
func (env testEnv) orgCtx(t *testing.T, org string) context.Context {
	t.Helper()
	bound, err := tenantctx.WithClaims(env.ctx, tenantctx.Claims{
		Issuer:   "securechain-test",
		Audience: "s1-port-interoperability",
		TenantID: env.tenantID,
		Subject:  org,
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("bind claims for %s: %v", org, err)
	}
	return bound
}

func blDigest(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])
}

func principal(org, role string) Principal {
	return Principal{ID: org, Role: role}
}

// makeChain registers B/L authority for the shipping line and opens a chain.
func (env testEnv) makeChain(t *testing.T, line, container, seed string, expiresAt time.Time) Chain {
	t.Helper()
	lineCtx := env.orgCtx(t, line)
	if err := env.store.RegisterBLAuthority(lineCtx, container, blDigest(seed), principal(line, "shipping-line")); err != nil {
		t.Fatalf("register B/L authority: %v", err)
	}
	chain, err := env.store.CreateChain(lineCtx, "idem-"+seed, container, blDigest(seed), expiresAt, principal(line, "shipping-line"))
	if err != nil {
		t.Fatalf("create chain: %v", err)
	}
	if chain.Status != StatusActive || chain.IssuerOrg != line {
		t.Fatalf("chain status=%s issuer=%s, want ACTIVE %s", chain.Status, chain.IssuerOrg, line)
	}
	return chain
}

func (env testEnv) outboxEvents(t *testing.T, eventType string) int {
	t.Helper()
	var count int
	if err := env.store.Pool().QueryRow(env.ctx, `
		SELECT count(*) FROM platform_outbox WHERE topic = 'ports.securechain.v1' AND event_type = $1`, eventType).Scan(&count); err != nil {
		t.Fatalf("count outbox events: %v", err)
	}
	return count
}

func TestSecureChainFullLifecycle(t *testing.T) {
	env := newTestEnv(t, Config{})
	defer env.cleanup()
	container := "MSCU1234567"
	chain := env.makeChain(t, "line-ng", container, "lifecycle", time.Now().Add(72*time.Hour))

	lineCtx := env.orgCtx(t, "line-ng")
	fwdCtx := env.orgCtx(t, "forwarder-ng")
	trkCtx := env.orgCtx(t, "transporter-ng")

	// line -> forwarder
	link1, err := env.store.Nominate(lineCtx, chain.ChainID, "forwarder-ng", principal("line-ng", "shipping-line"))
	if err != nil {
		t.Fatalf("nominate forwarder: %v", err)
	}
	if link1.Seq != 1 || link1.State() != LinkPending {
		t.Fatalf("link1 seq=%d state=%s, want 1 PENDING", link1.Seq, link1.State())
	}
	// The issuer cannot accept its own nomination; a third org cannot either.
	if _, err := env.store.Accept(lineCtx, chain.ChainID, 1, principal("line-ng", "shipping-line")); !errors.Is(err, ErrNotAuthorizedParty) {
		t.Fatalf("issuer accept err=%v, want ErrNotAuthorizedParty", err)
	}
	if _, err := env.store.Accept(trkCtx, chain.ChainID, 1, principal("transporter-ng", "transporter")); !errors.Is(err, ErrNotAuthorizedParty) {
		t.Fatalf("third-party accept err=%v, want ErrNotAuthorizedParty", err)
	}
	if _, err := env.store.Accept(fwdCtx, chain.ChainID, 1, principal("forwarder-ng", "forwarder")); err != nil {
		t.Fatalf("forwarder accept: %v", err)
	}
	// forwarder -> transporter
	if _, err := env.store.Nominate(fwdCtx, chain.ChainID, "transporter-ng", principal("forwarder-ng", "forwarder")); err != nil {
		t.Fatalf("nominate transporter: %v", err)
	}
	if _, err := env.store.Accept(trkCtx, chain.ChainID, 2, principal("transporter-ng", "transporter")); err != nil {
		t.Fatalf("transporter accept: %v", err)
	}

	// Release authorization goes only to the verified tail (transporter).
	if _, err := env.store.ReleaseAuthorization(lineCtx, container, principal("line-ng", "shipping-line")); !errors.Is(err, ErrNotTailHolder) {
		t.Fatalf("issuer release err=%v, want ErrNotTailHolder", err)
	}
	token, err := env.store.ReleaseAuthorization(trkCtx, container, principal("transporter-ng", "transporter"))
	if err != nil {
		t.Fatalf("tail release authorization: %v", err)
	}
	if token.HolderOrg != "transporter-ng" || token.ExpiresAt.Before(time.Now()) {
		t.Fatalf("token holder=%s expires=%s", token.HolderOrg, token.ExpiresAt)
	}
	// The token JWS verifies against the service public key.
	var envelope events.Envelope
	if err := json.Unmarshal([]byte(token.TokenJWS), &envelope); err != nil {
		t.Fatalf("token is not an envelope: %v", err)
	}
	if !envelope.VerifySignature(env.store.signer.PublicKey()) {
		t.Fatal("release token JWS does not verify")
	}

	// Gate consumes the token; the chain completes.
	consumed, err := env.store.Consume(env.orgCtx(t, "gate-officer-1"), token.Nonce, "GATE-1", principal("gate-officer-1", "gate-officer"))
	if err != nil {
		t.Fatalf("consume release token: %v", err)
	}
	if consumed.ConsumedAt == nil || *consumed.ConsumedGate != "GATE-1" {
		t.Fatalf("consumed token missing gate evidence: %+v", consumed)
	}
	loaded, err := env.store.GetByContainer(trkCtx, container)
	if err != nil {
		t.Fatalf("load completed chain: %v", err)
	}
	if loaded.Status != StatusCompleted {
		t.Fatalf("chain status=%s, want COMPLETED", loaded.Status)
	}
	if len(loaded.Links) != 2 {
		t.Fatalf("chain has %d links, want 2", len(loaded.Links))
	}

	// Hash chain of links verifies.
	prev := ZeroHash
	for _, link := range loaded.Links {
		recomputed, err := LinkHash(prev, chain.ChainID, link.Seq, link.FromOrg, link.ToOrg, link.NominatedBy, link.NominatedAt)
		if err != nil {
			t.Fatalf("recompute link hash: %v", err)
		}
		if recomputed != link.LinkHash {
			t.Fatalf("link %d hash mismatch: stored %s recomputed %s", link.Seq, link.LinkHash, recomputed)
		}
		prev = link.LinkHash
	}

	// Hash-chained audit trail verifies and covers the lifecycle.
	ok, err := env.store.VerifyAuditTrail(trkCtx, chain.ChainID)
	if err != nil || !ok {
		t.Fatalf("audit trail verify ok=%v err=%v", ok, err)
	}
	for _, eventType := range []string{EventChainCreated, EventLinkNominated, EventLinkAccepted, EventReleaseAuthorized, EventReleaseConsumed} {
		if env.outboxEvents(t, eventType) == 0 {
			t.Fatalf("no outbox event of type %s", eventType)
		}
	}
}

func TestSecureChainDoubleAcceptRace(t *testing.T) {
	env := newTestEnv(t, Config{})
	defer env.cleanup()
	chain := env.makeChain(t, "line-ng", "TCLU7654321", "race", time.Now().Add(72*time.Hour))
	lineCtx := env.orgCtx(t, "line-ng")
	fwdCtx := env.orgCtx(t, "forwarder-ng")
	if _, err := env.store.Nominate(lineCtx, chain.ChainID, "forwarder-ng", principal("line-ng", "shipping-line")); err != nil {
		t.Fatalf("nominate: %v", err)
	}
	var wait sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			_, errs[index] = env.store.Accept(fwdCtx, chain.ChainID, 1, principal("forwarder-ng", "forwarder"))
		}(i)
	}
	wait.Wait()
	succeeded := 0
	for _, err := range errs {
		if err == nil {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("%d concurrent accepts succeeded, want exactly 1 (errs=%v)", succeeded, errs)
	}
	loaded, err := env.store.GetByContainer(fwdCtx, "TCLU7654321")
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	if len(loaded.Links) != 1 || loaded.Links[0].State() != LinkAccepted {
		t.Fatalf("links=%v, want one ACCEPTED link", loaded.Links)
	}
}

func TestSecureChainDeclineReturnsTailToNominator(t *testing.T) {
	env := newTestEnv(t, Config{})
	defer env.cleanup()
	chain := env.makeChain(t, "line-ng", "GESU2468101", "decline", time.Now().Add(72*time.Hour))
	lineCtx := env.orgCtx(t, "line-ng")
	fwdCtx := env.orgCtx(t, "forwarder-ng")
	if _, err := env.store.Nominate(lineCtx, chain.ChainID, "forwarder-ng", principal("line-ng", "shipping-line")); err != nil {
		t.Fatalf("nominate: %v", err)
	}
	if _, err := env.store.Decline(fwdCtx, chain.ChainID, 1, "cannot service this corridor", principal("forwarder-ng", "forwarder")); err != nil {
		t.Fatalf("decline: %v", err)
	}
	// The forwarder is no longer the tail; the line re-nominates.
	if _, err := env.store.Nominate(fwdCtx, chain.ChainID, "transporter-ng", principal("forwarder-ng", "forwarder")); !errors.Is(err, ErrNotTailHolder) {
		t.Fatalf("declined org nominate err=%v, want ErrNotTailHolder", err)
	}
	link2, err := env.store.Nominate(lineCtx, chain.ChainID, "transporter-ng", principal("line-ng", "shipping-line"))
	if err != nil {
		t.Fatalf("re-nominate after decline: %v", err)
	}
	if link2.Seq != 2 || link2.FromOrg != "line-ng" {
		t.Fatalf("link2 seq=%d from=%s, want seq 2 from line-ng", link2.Seq, link2.FromOrg)
	}
	if env.outboxEvents(t, EventLinkDeclined) != 1 {
		t.Fatal("link_declined outbox event missing")
	}
}

func TestSecureChainRevokeCascade(t *testing.T) {
	env := newTestEnv(t, Config{})
	defer env.cleanup()
	container := "CMAU1357913"
	chain := env.makeChain(t, "line-ng", container, "revoke", time.Now().Add(72*time.Hour))
	lineCtx := env.orgCtx(t, "line-ng")
	fwdCtx := env.orgCtx(t, "forwarder-ng")
	if _, err := env.store.Nominate(lineCtx, chain.ChainID, "forwarder-ng", principal("line-ng", "shipping-line")); err != nil {
		t.Fatalf("nominate: %v", err)
	}
	if _, err := env.store.Accept(fwdCtx, chain.ChainID, 1, principal("forwarder-ng", "forwarder")); err != nil {
		t.Fatalf("accept: %v", err)
	}
	// Only the issuer may revoke.
	if _, err := env.store.Revoke(fwdCtx, chain.ChainID, "fraud attempt", principal("forwarder-ng", "forwarder")); !errors.Is(err, ErrNotAuthorizedParty) {
		t.Fatalf("non-issuer revoke err=%v, want ErrNotAuthorizedParty", err)
	}
	revoked, err := env.store.Revoke(lineCtx, chain.ChainID, "suspected chain hijack", principal("line-ng", "shipping-line"))
	if err != nil {
		t.Fatalf("issuer revoke: %v", err)
	}
	if revoked.Status != StatusRevoked {
		t.Fatalf("status=%s, want REVOKED", revoked.Status)
	}
	// Cascade: the chain is dead; the accepted link remains as hash-chained
	// history (links resolve exactly once) and release is dead.
	loaded, err := env.store.GetByContainer(fwdCtx, container)
	if err != nil {
		t.Fatalf("load revoked chain: %v", err)
	}
	if len(loaded.Links) != 1 || loaded.Links[0].State() != LinkAccepted {
		t.Fatalf("links=%v, want the accepted link retained as history", loaded.Links)
	}
	if _, err := env.store.ReleaseAuthorization(fwdCtx, container, principal("forwarder-ng", "forwarder")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("release after revoke err=%v, want ErrNotFound (no ACTIVE chain)", err)
	}
	if _, err := env.store.Nominate(fwdCtx, chain.ChainID, "transporter-ng", principal("forwarder-ng", "forwarder")); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("nominate after revoke err=%v, want ErrInvalidTransition", err)
	}
	if env.outboxEvents(t, EventChainRevoked) != 1 {
		t.Fatal("chain_revoked outbox event missing")
	}
}

func TestSecureChainSingleUseTokenReplayRejected(t *testing.T) {
	env := newTestEnv(t, Config{})
	defer env.cleanup()
	container := "HLBU1122334"
	chain := env.makeChain(t, "line-ng", container, "replay", time.Now().Add(72*time.Hour))
	lineCtx := env.orgCtx(t, "line-ng")
	token, err := env.store.ReleaseAuthorization(lineCtx, container, principal("line-ng", "shipping-line"))
	if err != nil {
		t.Fatalf("release authorization: %v", err)
	}
	gateCtx := env.orgCtx(t, "gate-officer-1")
	if _, err := env.store.Consume(gateCtx, token.Nonce, "GATE-1", principal("gate-officer-1", "gate-officer")); err != nil {
		t.Fatalf("first consume: %v", err)
	}
	// Replay of the same nonce is rejected by the atomic single-use claim.
	if _, err := env.store.Consume(gateCtx, token.Nonce, "GATE-2", principal("gate-officer-1", "gate-officer")); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("replay err=%v, want ErrTokenInvalid", err)
	}
	// A random nonce (forged token) is rejected.
	forged := blDigest("forged-nonce")
	if _, err := env.store.Consume(gateCtx, forged, "GATE-1", principal("gate-officer-1", "gate-officer")); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("forged nonce err=%v, want ErrTokenInvalid", err)
	}
	// A second token for the completed chain is impossible.
	if _, err := env.store.ReleaseAuthorization(lineCtx, container, principal("line-ng", "shipping-line")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("release after completion err=%v, want ErrNotFound", err)
	}
	_ = chain
}

func TestSecureChainTokenExpiryRejected(t *testing.T) {
	env := newTestEnv(t, Config{TokenTTL: time.Millisecond})
	defer env.cleanup()
	container := "MSKU9988776"
	env.makeChain(t, "line-ng", container, "expired-token", time.Now().Add(72*time.Hour))
	lineCtx := env.orgCtx(t, "line-ng")
	token, err := env.store.ReleaseAuthorization(lineCtx, container, principal("line-ng", "shipping-line"))
	if err != nil {
		t.Fatalf("release authorization: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := env.store.Consume(env.orgCtx(t, "gate-officer-1"), token.Nonce, "GATE-1", principal("gate-officer-1", "gate-officer")); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("expired token err=%v, want ErrTokenInvalid", err)
	}
}

func TestSecureChainCreateGuards(t *testing.T) {
	env := newTestEnv(t, Config{})
	defer env.cleanup()
	lineCtx := env.orgCtx(t, "line-ng")
	otherCtx := env.orgCtx(t, "other-line")
	container := "UETU5544332"
	digest := blDigest("guards")
	// No release authority: another org cannot open a chain over the digest.
	if err := env.store.RegisterBLAuthority(lineCtx, container, digest, principal("line-ng", "shipping-line")); err != nil {
		t.Fatalf("register authority: %v", err)
	}
	if _, err := env.store.CreateChain(otherCtx, "idem-steal", container, digest, time.Now().Add(time.Hour), principal("other-line", "shipping-line")); !errors.Is(err, ErrNoReleaseAuthority) {
		t.Fatalf("unauthorised create err=%v, want ErrNoReleaseAuthority", err)
	}
	chain := env.makeChain(t, "line-ng", container, "guards", time.Now().Add(72*time.Hour))
	// Idempotent replay returns the retained chain.
	replay, err := env.store.CreateChain(lineCtx, "idem-guards", container, digest, time.Now().Add(72*time.Hour), principal("line-ng", "shipping-line"))
	if err != nil || replay.ChainID != chain.ChainID {
		t.Fatalf("idempotent replay chain=%v err=%v", replay.ChainID, err)
	}
	// A second ACTIVE chain for the same container is DB-rejected.
	if err := env.store.RegisterBLAuthority(lineCtx, container, blDigest("second"), principal("line-ng", "shipping-line")); err != nil {
		t.Fatalf("register second authority: %v", err)
	}
	if _, err := env.store.CreateChain(lineCtx, "idem-second", container, blDigest("second"), time.Now().Add(time.Hour), principal("line-ng", "shipping-line")); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("second active chain err=%v, want ErrInvalidTransition", err)
	}
}

func TestSecureChainVelocityHold(t *testing.T) {
	env := newTestEnv(t, Config{VelocityThreshold: 2, VelocityHold: true})
	defer env.cleanup()
	container := "FCIU6677889"
	chain := env.makeChain(t, "line-ng", container, "velocity", time.Now().Add(72*time.Hour))
	lineCtx := env.orgCtx(t, "line-ng")
	fwdCtx := env.orgCtx(t, "forwarder-ng")
	// Two full nomination/decline cycles are within threshold; the third
	// nomination in 24h breaches it and clamps the fail-closed hold.
	for i := 0; i < 2; i++ {
		if _, err := env.store.Nominate(lineCtx, chain.ChainID, "forwarder-ng", principal("line-ng", "shipping-line")); err != nil {
			t.Fatalf("nominate %d: %v", i+1, err)
		}
		if _, err := env.store.Decline(fwdCtx, chain.ChainID, int64(i+1), "retry", principal("forwarder-ng", "forwarder")); err != nil {
			t.Fatalf("decline %d: %v", i+1, err)
		}
	}
	if _, err := env.store.Nominate(lineCtx, chain.ChainID, "forwarder-ng", principal("line-ng", "shipping-line")); !errors.Is(err, ErrVelocityHold) {
		t.Fatalf("velocity breach err=%v, want ErrVelocityHold", err)
	}
	loaded, err := env.store.GetByContainer(lineCtx, container)
	if err != nil {
		t.Fatalf("load held chain: %v", err)
	}
	if !loaded.VelocityHold {
		t.Fatal("chain is not on velocity hold after breach")
	}
	// The hold blocks release authorization too.
	if _, err := env.store.ReleaseAuthorization(lineCtx, container, principal("line-ng", "shipping-line")); !errors.Is(err, ErrVelocityHold) {
		t.Fatalf("release on hold err=%v, want ErrVelocityHold", err)
	}
	if env.outboxEvents(t, EventVelocityFlagRaised) == 0 {
		t.Fatal("velocity_flagged outbox event missing")
	}
}

func TestSecureChainExpirySweep(t *testing.T) {
	env := newTestEnv(t, Config{})
	defer env.cleanup()
	container := "TGHU3322110"
	chain := env.makeChain(t, "line-ng", container, "sweep", time.Now().Add(time.Hour))
	// Force the expiry into the past (the API refuses past expiries; the
	// sweeper deals with chains that outlived their window).
	if _, err := env.store.Pool().Exec(env.ctx, `UPDATE secure_chains SET expires_at = now() - interval '1 minute' WHERE chain_id = $1`, chain.ChainID); err != nil {
		t.Fatalf("force expiry: %v", err)
	}
	lineCtx := env.orgCtx(t, "line-ng")
	// An expired chain can no longer mutate or release.
	if _, err := env.store.Nominate(lineCtx, chain.ChainID, "forwarder-ng", principal("line-ng", "shipping-line")); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("nominate expired err=%v, want ErrInvalidTransition", err)
	}
	expired, err := env.store.ExpireDue(lineCtx, principal("booking-worker", "securechain-expiry"))
	if err != nil {
		t.Fatalf("expiry sweep: %v", err)
	}
	if len(expired) != 1 || expired[0] != chain.ChainID {
		t.Fatalf("expired=%v, want [%s]", expired, chain.ChainID)
	}
	loaded, err := env.store.GetByContainer(lineCtx, container)
	if err != nil {
		t.Fatalf("load expired chain: %v", err)
	}
	if loaded.Status != StatusExpired {
		t.Fatalf("status=%s, want EXPIRED", loaded.Status)
	}
	if env.outboxEvents(t, EventChainExpired) != 1 {
		t.Fatal("chain_expired outbox event missing")
	}
	// The sweep is idempotent.
	again, err := env.store.ExpireDue(lineCtx, principal("booking-worker", "securechain-expiry"))
	if err != nil || len(again) != 0 {
		t.Fatalf("second sweep expired=%v err=%v, want empty", again, err)
	}
}

func TestSecureChainBookingGateIntegration(t *testing.T) {
	env := newTestEnv(t, Config{})
	defer env.cleanup()
	container := "BEAU4455667"
	chain := env.makeChain(t, "line-ng", container, "booking-gate", time.Now().Add(72*time.Hour))
	lineCtx := env.orgCtx(t, "line-ng")
	fwdCtx := env.orgCtx(t, "forwarder-ng")
	// Booking gate: the tail check runs inside the caller's transaction via
	// VerifyReleaseHolder (wired into booking.Store.SetReleaseVerifier).
	err := env.store.withTx(fwdCtx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		return env.store.VerifyReleaseHolder(fwdCtx, tx, container, "forwarder-ng")
	})
	if !errors.Is(err, ErrNotTailHolder) {
		t.Fatalf("non-tail verify err=%v, want ErrNotTailHolder", err)
	}
	if _, err := env.store.Nominate(lineCtx, chain.ChainID, "forwarder-ng", principal("line-ng", "shipping-line")); err != nil {
		t.Fatalf("nominate: %v", err)
	}
	if _, err := env.store.Accept(fwdCtx, chain.ChainID, 1, principal("forwarder-ng", "forwarder")); err != nil {
		t.Fatalf("accept: %v", err)
	}
	err = env.store.withTx(fwdCtx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		return env.store.VerifyReleaseHolder(fwdCtx, tx, container, "forwarder-ng")
	})
	if err != nil {
		t.Fatalf("tail verify: %v", err)
	}
}
