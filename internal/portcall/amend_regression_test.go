package portcall

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// These tests run against a real PostgreSQL when PORTCALL_TEST_DATABASE_URL
// (or BOOKING_TEST_DATABASE_URL) is set — the same gating as the booking and
// declarations store tests. They are skipped otherwise.

type amendEnv struct {
	store   *Store
	ctx     context.Context
	cleanup func()
}

func newAmendEnv(t *testing.T) amendEnv {
	t.Helper()
	databaseURL := os.Getenv("PORTCALL_TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("BOOKING_TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("PORTCALL_TEST_DATABASE_URL is not set; skipping PostgreSQL-backed port-call tests")
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
	tenantID := fmt.Sprintf("tenant-portcall-%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := store.Pool().Exec(ctx, `INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ($1, $2)`, tenantID, "portcall-test-authority"); err != nil {
		t.Fatalf("insert test tenant: %v", err)
	}
	bound, err := tenantctx.WithClaims(ctx, tenantctx.Claims{
		Issuer:   "portcall-test",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  "portcall-test-officer",
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("bind test tenant claims: %v", err)
	}
	return amendEnv{store: store, ctx: bound, cleanup: store.Close}
}

// clearedCall drives a port call to an APPROVED clearance decision and
// returns the call id and current version.
func (env amendEnv) clearedCall(t *testing.T, callID string) (string, int64) {
	t.Helper()
	// The profile digest must satisfy the sha256:<64 hex> contract enforced
	// by both RegisterAgencyProfile and the port_agency_profile_versions
	// CHECK constraint (migration 0005); "abc123" was never a storable
	// digest and the registration rightly failed closed.
	if err := env.store.RegisterAgencyProfile(env.ctx, AgencyProfileRegistration{
		ProfileID: "profile-npa-1", Version: "v1", AgencyCode: "NPA",
		ProfileSHA256: "sha256:9b8c7d6e5f4a3b2c1d0e9f8a7b6c5d4e3f2a1b0c9d8e7f6a5b4c3d2e1f0a9b8c",
		RegisteredBy:  "portcall-test-officer", Active: true,
	}); err != nil {
		t.Fatalf("register agency profile: %v", err)
	}
	call, err := env.store.Create(env.ctx, "idem-"+callID, CreateRequest{
		CallID: callID, VesselIMO: "9074729", PortCode: "NGAPP",
		DeclarationRef: "NCS-2026-XYZ789", SubmittedBy: "agent-1",
		AgencyProfileID: "profile-npa-1", AgencyProfileVersion: "v1",
	})
	if err != nil {
		t.Fatalf("create port call: %v", err)
	}
	if _, err := env.store.Transition(env.ctx, callID, call.Version, StatusSubmitted); err != nil {
		t.Fatalf("submit port call: %v", err)
	}
	if _, err := env.store.Transition(env.ctx, callID, call.Version+1, StatusAccepted); err != nil {
		t.Fatalf("accept port call: %v", err)
	}
	document, err := env.store.DeclareDocument(env.ctx, callID, DocumentDeclarationRequest{
		DocumentType: "bill-of-lading", MediaType: "application/pdf",
		SizeBytes: 1024, SHA256: "sha256:3b8c7d6e5f4a3b2c1d0e9f8a7b6c5d4e3f2a1b0c9d8e7f6a5b4c3d2e1f0a9b8c",
		DeclaredBy: "agent-1",
	})
	if err != nil {
		t.Fatalf("declare document: %v", err)
	}
	if _, err := env.store.ReviewDocument(env.ctx, callID, document.DocumentID, DocumentReviewRequest{
		ExpectedVersion: document.Version, Status: DocumentVerified,
		ReviewedBy: "officer-1", Reason: "legible and complete",
	}); err != nil {
		t.Fatalf("verify document: %v", err)
	}
	clearance, err := env.store.DecideClearance(env.ctx, callID, call.Version+2, ClearanceApproved, "all documents verified", "officer-1")
	if err != nil {
		t.Fatalf("decide clearance: %v", err)
	}
	return clearance.CallID, clearance.CallVersion
}

// PI-9 regression: an amendment bumps port_calls.version and aligns
// port_calls.status with the amended decision in the same transaction.
func TestAmendClearanceBumpsVersionAndAlignsStatus(t *testing.T) {
	env := newAmendEnv(t)
	defer env.cleanup()
	callID, version := env.clearedCall(t, "call-amend-1")

	amended, err := env.store.AmendClearance(env.ctx, callID, ClearanceAmendmentRequest{
		ExpectedVersion: version, Decision: ClearanceRejected,
		Reason: "post-audit discrepancy", AmendedBy: "officer-2",
	})
	if err != nil {
		t.Fatalf("amend clearance: %v", err)
	}
	if amended.CallVersion != version+1 {
		t.Fatalf("amended call version = %d, want %d", amended.CallVersion, version+1)
	}
	call, err := env.store.Get(env.ctx, callID)
	if err != nil {
		t.Fatalf("get port call: %v", err)
	}
	if call.Version != version+1 {
		t.Fatalf("port call version = %d, want %d", call.Version, version+1)
	}
	if call.Status != StatusRejected {
		t.Fatalf("port call status = %s, want REJECTED (aligned with amended decision)", call.Status)
	}

	// Amending back to APPROVED re-aligns the status to ACCEPTED.
	again, err := env.store.AmendClearance(env.ctx, callID, ClearanceAmendmentRequest{
		ExpectedVersion: call.Version, Decision: ClearanceApproved,
		Reason: "discrepancy resolved", AmendedBy: "officer-1",
	})
	if err != nil {
		t.Fatalf("re-amend clearance: %v", err)
	}
	call, err = env.store.Get(env.ctx, callID)
	if err != nil {
		t.Fatalf("get port call: %v", err)
	}
	if call.Status != StatusAccepted || call.Version != again.CallVersion {
		t.Fatalf("re-amended call = %s v%d, want ACCEPTED v%d", call.Status, call.Version, again.CallVersion)
	}
}

// PI-9 regression: two concurrent amendments on the same expected version
// conflict — exactly one commits, the other gets ErrOptimisticConflict.
func TestConcurrentAmendmentsConflict(t *testing.T) {
	env := newAmendEnv(t)
	defer env.cleanup()
	callID, version := env.clearedCall(t, "call-amend-2")

	var wait sync.WaitGroup
	errs := make([]error, 2)
	for index, amender := range []string{"officer-2", "officer-3"} {
		wait.Add(1)
		go func(index int, amender string) {
			defer wait.Done()
			_, err := env.store.AmendClearance(env.ctx, callID, ClearanceAmendmentRequest{
				ExpectedVersion: version, Decision: ClearanceRejected,
				Reason: "concurrent amendment by " + amender, AmendedBy: amender,
			})
			errs[index] = err
		}(index, amender)
	}
	wait.Wait()

	succeeded, conflicts := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrOptimisticConflict):
			conflicts++
		default:
			t.Fatalf("unexpected amendment error: %v", err)
		}
	}
	if succeeded != 1 || conflicts != 1 {
		t.Fatalf("concurrent amendments: succeeded = %d, conflicts = %d, want exactly one of each", succeeded, conflicts)
	}
	call, err := env.store.Get(env.ctx, callID)
	if err != nil {
		t.Fatalf("get port call: %v", err)
	}
	if call.Version != version+1 || call.Status != StatusRejected {
		t.Fatalf("call after race = %s v%d, want REJECTED v%d", call.Status, call.Version, version+1)
	}
}
