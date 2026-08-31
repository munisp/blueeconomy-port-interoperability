package pushtokens

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// These tests run against a real PostgreSQL when PUSH_TOKENS_TEST_DATABASE_URL
// is set — see scripts/verify-local.sh and docker-compose.integration.yml.
// They are skipped otherwise; there is no in-memory substitute for the
// upsert semantics, the single-active-token index and the tenant RLS
// enforcement under test.
type testEnv struct {
	store    *Store
	tenantID string
	ctx      context.Context
	cleanup  func()
}

func newTestEnv(t *testing.T) testEnv {
	t.Helper()
	databaseURL := os.Getenv("PUSH_TOKENS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("PUSH_TOKENS_TEST_DATABASE_URL is not set; skipping PostgreSQL-backed push-token tests")
	}
	ctx := context.Background()
	store, err := Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if _, err := store.Pool().Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
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
		if _, err := store.Pool().Exec(ctx, string(migration)); err != nil {
			t.Fatalf("apply migration %s: %v", entry, err)
		}
	}
	tenantID := fmt.Sprintf("tenant-pt-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := store.Pool().Exec(ctx, `INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ($1, $2)`, tenantID, "pushtokens-test-authority"); err != nil {
		t.Fatalf("insert test tenant: %v", err)
	}
	return testEnv{store: store, tenantID: tenantID, ctx: ctx, cleanup: store.Close}
}

// userCtx binds a verified subject; in production this comes from the
// Keycloak/gateway token — never from request bodies.
func (env testEnv) userCtx(t *testing.T, subject string) context.Context {
	t.Helper()
	bound, err := tenantctx.WithClaims(env.ctx, tenantctx.Claims{
		Issuer:   "pushtokens-test",
		Audience: "s1-port-interoperability",
		TenantID: env.tenantID,
		Subject:  subject,
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("bind claims for %s: %v", subject, err)
	}
	return bound
}

func registration(device, token, platform string) RegisterRequest {
	return RegisterRequest{DeviceID: device, Token: token, Platform: platform}
}

func TestNewStoreFailsClosedWithoutPool(t *testing.T) {
	if _, err := NewStore(nil); err == nil {
		t.Fatal("nil pool must fail closed")
	}
}

func TestRegisterValidation(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	for name, request := range map[string]RegisterRequest{
		"missing device":   {Token: "token-12345678", Platform: "android"},
		"short token":      {DeviceID: "d", Token: "short", Platform: "android"},
		"bad platform":     {DeviceID: "d", Token: "token-12345678", Platform: "smarttv"},
		"missing platform": {DeviceID: "d", Token: "token-12345678"},
	} {
		if _, err := env.store.Register(env.userCtx(t, "user-1"), request); err == nil {
			t.Fatalf("%s: must be rejected", name)
		}
	}
}

func TestRegisterPersistsActiveToken(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	stored, err := env.store.Register(env.userCtx(t, "user-1"), registration("device-1", "fcm-token-aaaaaaaa", "android"))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if stored.UserID != "user-1" || stored.DeviceID != "device-1" || stored.Status != "ACTIVE" {
		t.Fatalf("stored = %+v", stored)
	}
	if stored.RevokedAt != nil {
		t.Fatal("active registration must not carry revoked_at")
	}
}

func TestRegisterSameDeviceUpserts(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	ctx := env.userCtx(t, "user-1")
	if _, err := env.store.Register(ctx, registration("device-1", "fcm-token-aaaaaaaa", "android")); err != nil {
		t.Fatalf("first register: %v", err)
	}
	stored, err := env.store.Register(ctx, registration("device-1", "fcm-token-bbbbbbbb", "android"))
	if err != nil {
		t.Fatalf("second register: %v", err)
	}
	if stored.Token != "fcm-token-bbbbbbbb" || stored.Status != "ACTIVE" {
		t.Fatalf("upsert = %+v", stored)
	}
	var count int
	if err := env.store.Pool().QueryRow(env.ctx, `SELECT count(*) FROM push_tokens`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("rows = %d, want 1 (upsert, not insert)", count)
	}
}

func TestTokenMoveRevokesPreviousHolder(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	ctx := env.userCtx(t, "user-1")
	if _, err := env.store.Register(ctx, registration("device-1", "fcm-token-aaaaaaaa", "android")); err != nil {
		t.Fatalf("register device-1: %v", err)
	}
	if _, err := env.store.Register(ctx, registration("device-2", "fcm-token-aaaaaaaa", "android")); err != nil {
		t.Fatalf("register device-2 with same provider token: %v", err)
	}
	// device-1's row must now be REVOKED; device-2 holds the only ACTIVE token.
	var device1Status, device2Status string
	if err := env.store.Pool().QueryRow(env.ctx,
		`SELECT status FROM push_tokens WHERE device_id = 'device-1'`).Scan(&device1Status); err != nil {
		t.Fatalf("read device-1: %v", err)
	}
	if err := env.store.Pool().QueryRow(env.ctx,
		`SELECT status FROM push_tokens WHERE device_id = 'device-2'`).Scan(&device2Status); err != nil {
		t.Fatalf("read device-2: %v", err)
	}
	if device1Status != "REVOKED" || device2Status != "ACTIVE" {
		t.Fatalf("device-1=%s device-2=%s, want REVOKED/ACTIVE", device1Status, device2Status)
	}
}

func TestRevoke(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	ctx := env.userCtx(t, "user-1")
	if _, err := env.store.Register(ctx, registration("device-1", "fcm-token-aaaaaaaa", "ios")); err != nil {
		t.Fatalf("register: %v", err)
	}
	stored, err := env.store.Revoke(ctx, "device-1")
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if stored.Status != "REVOKED" || stored.RevokedAt == nil {
		t.Fatalf("revoked = %+v", stored)
	}
}

func TestRevokeUnknownOrAlreadyRevokedFailsClosed(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	ctx := env.userCtx(t, "user-1")
	if _, err := env.store.Revoke(ctx, "absent"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revoke absent = %v, want ErrNotFound", err)
	}
	if _, err := env.store.Register(ctx, registration("device-1", "fcm-token-aaaaaaaa", "web")); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := env.store.Revoke(ctx, "device-1"); err != nil {
		t.Fatalf("first revoke: %v", err)
	}
	if _, err := env.store.Revoke(ctx, "device-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second revoke = %v, want ErrNotFound", err)
	}
}

func TestUsersCannotTouchEachOthersDevices(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	if _, err := env.store.Register(env.userCtx(t, "user-1"), registration("device-1", "fcm-token-aaaaaaaa", "android")); err != nil {
		t.Fatalf("register: %v", err)
	}
	// user-2 cannot revoke user-1's device.
	if _, err := env.store.Revoke(env.userCtx(t, "user-2"), "device-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-user revoke = %v, want ErrNotFound", err)
	}
}

func TestTenantIsolationEnforcedByRLS(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	if _, err := env.store.Register(env.userCtx(t, "user-1"), registration("device-1", "fcm-token-aaaaaaaa", "android")); err != nil {
		t.Fatalf("register: %v", err)
	}
	otherTenant := env.tenantID + "-other"
	if _, err := env.store.Pool().Exec(env.ctx, `INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ($1, $2)`, otherTenant, "pushtokens-test-authority"); err != nil {
		t.Fatalf("insert other tenant: %v", err)
	}
	otherCtx, err := tenantctx.WithClaims(env.ctx, tenantctx.Claims{
		Issuer: "pushtokens-test", Audience: "s1-port-interoperability",
		TenantID: otherTenant, Subject: "user-1", Expires: time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("bind other-tenant claims: %v", err)
	}
	if _, err := env.store.Revoke(otherCtx, "device-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant revoke = %v, want ErrNotFound", err)
	}
}
