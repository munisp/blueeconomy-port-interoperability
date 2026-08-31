package declarations

import (
	"testing"
	"time"
)

// DB-gated Phase-7 integration (PRA-066..068): the real GRPCScorer — not a
// stub — drives store.AssessRisk against an in-process gRPC fixture, proving
// the resilience client preserves the fail-closed lifecycle semantics at the
// database boundary. Runs against real PostgreSQL per the package
// conventions (DECLARATIONS_TEST_DATABASE_URL); skipped otherwise.

func TestGRPCScorerAssessRiskEndToEndAgainstPostgres(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	fixture := &scriptedScorer{} // deterministic valid verdict, score 42
	scorer := newTestScorer(t, startFixtureScorer(t, fixture), func(config *GRPCScorerConfig) {
		config.MaxRetries = 2
		config.BackoffBase = time.Millisecond
		config.BackoffMax = 4 * time.Millisecond
	})

	created, err := env.store.Create(env.ctx, createRequest("req-decl-grpc-0001"), principal())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	submitted, err := env.store.Submit(env.ctx, created.DeclarationID, created.Version, principal())
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	assessed, err := env.store.AssessRisk(env.ctx, created.DeclarationID, submitted.Version, scorer, 0, principal())
	if err != nil {
		t.Fatalf("assess risk via gRPC scorer: %v", err)
	}
	if assessed.RiskScore == nil || *assessed.RiskScore != 42 {
		t.Fatalf("risk score = %+v, want 42 from the gRPC verdict", assessed.RiskScore)
	}
	if assessed.RiskLane == nil {
		t.Fatalf("a scored declaration must be laned, got %+v", assessed)
	}
	if fixture.callCount() != 1 {
		t.Fatalf("scorer calls = %d, want exactly 1", fixture.callCount())
	}
}

func TestGRPCScorerOutageParksDeclarationAgainstPostgres(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	// Scorer down: nothing listens on the reserved-and-released address.
	deadAddress := reserveDeadAddress(t)
	scorer := newTestScorer(t, deadAddress, func(config *GRPCScorerConfig) {
		config.MaxRetries = 1
		config.BackoffBase = time.Millisecond
		config.BackoffMax = 4 * time.Millisecond
		config.Timeout = 300 * time.Millisecond
	})

	created, err := env.store.Create(env.ctx, createRequest("req-decl-grpc-0002"), principal())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	submitted, err := env.store.Submit(env.ctx, created.DeclarationID, created.Version, principal())
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	assessed, err := env.store.AssessRisk(env.ctx, created.DeclarationID, submitted.Version, scorer, 0, principal())
	if err != nil {
		t.Fatalf("assess risk: %v", err)
	}
	if assessed.Status != StatusScoringUnavailable || assessed.RiskLane != nil {
		t.Fatalf("gRPC scorer outage must park the declaration, got %+v", assessed)
	}
	if assessed.ScoringError == nil || *assessed.ScoringError == "" {
		t.Fatal("the scoring error must be recorded for audit")
	}
}
