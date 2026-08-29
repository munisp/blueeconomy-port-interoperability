package declarations

import (
	"context"
	"net"
	"testing"
	"time"

	riskscorev1 "github.com/munisp/blueeconomy-contracts/gen/go/blueeconomy/riskscore/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryPolicyConfig isolates the retry policy from the breaker: the failure
// threshold is set far above the retry budget.
func retryPolicyConfig(config *GRPCScorerConfig) {
	config.MaxRetries = 3
	config.BackoffBase = time.Millisecond
	config.BackoffMax = 4 * time.Millisecond
	config.BreakerFailureThreshold = 1000
}

func TestRetryPolicyMatrix(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "scorer down")
	deadline := status.Error(codes.DeadlineExceeded, "slow")
	invalid := status.Error(codes.InvalidArgument, "bad payload")
	unauthenticated := status.Error(codes.Unauthenticated, "bad token")

	cases := map[string]struct {
		script    []error
		wantCalls int
		wantErr   bool
	}{
		"success first attempt":              {script: nil, wantCalls: 1},
		"unavailable then success":           {script: []error{unavailable}, wantCalls: 2},
		"deadline exceeded then success":     {script: []error{deadline, unavailable}, wantCalls: 3},
		"unavailable exhausts the budget":    {script: []error{unavailable, unavailable, unavailable, unavailable}, wantCalls: 4, wantErr: true},
		"invalid argument never retried":     {script: []error{invalid}, wantCalls: 1, wantErr: true},
		"unauthenticated never retried":      {script: []error{unauthenticated}, wantCalls: 1, wantErr: true},
		"caller fault mid-sequence stops":    {script: []error{unavailable, invalid, unavailable}, wantCalls: 2, wantErr: true},
		"unauthenticated mid-sequence stops": {script: []error{unavailable, unauthenticated, unavailable}, wantCalls: 2, wantErr: true},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			fixture := &scriptedScorer{script: testCase.script}
			scorer := newTestScorer(t, startFixtureScorer(t, fixture), retryPolicyConfig)
			_, err := scorer.Score(context.Background(), scoreRequest())
			if testCase.wantErr && err == nil {
				t.Fatalf("%s must fail closed", name)
			}
			if !testCase.wantErr && err != nil {
				t.Fatalf("%s must succeed: %v", name, err)
			}
			if fixture.callCount() != testCase.wantCalls {
				t.Fatalf("%s: calls = %d, want %d", name, fixture.callCount(), testCase.wantCalls)
			}
		})
	}
}

func TestCircuitBreakerOpenHalfOpenClosed(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "scorer down")
	fixture := &scriptedScorer{fallback: func() (*riskscorev1.ScoreDeclarationResponse, error) {
		return nil, unavailable
	}}
	scorer := newTestScorer(t, startFixtureScorer(t, fixture), func(config *GRPCScorerConfig) {
		config.MaxRetries = 0 // one attempt per Score call: isolate the breaker
		config.BreakerFailureThreshold = 3
		config.BreakerTimeout = 150 * time.Millisecond
	})
	// Closed: three consecutive retriable failures reach the scorer.
	for i := 0; i < 3; i++ {
		if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
			t.Fatalf("attempt %d must fail", i+1)
		}
	}
	if fixture.callCount() != 3 {
		t.Fatalf("closed breaker passed %d calls, want 3", fixture.callCount())
	}
	// Open: the breaker now fails fast without touching the scorer.
	if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
		t.Fatal("an open breaker must fail closed")
	}
	if fixture.callCount() != 3 {
		t.Fatalf("open breaker leaked a call to the scorer: calls = %d", fixture.callCount())
	}
	// Half-open after the cooldown: one probe is allowed; it succeeds and
	// the breaker closes again.
	time.Sleep(200 * time.Millisecond)
	fixture.fallback = func() (*riskscorev1.ScoreDeclarationResponse, error) {
		return validVerdict(), nil
	}
	verdict, err := scorer.Score(context.Background(), scoreRequest())
	if err != nil {
		t.Fatalf("half-open probe must be allowed and succeed: %v", err)
	}
	if verdict.Score != 42 {
		t.Fatalf("verdict = %+v", verdict)
	}
	if fixture.callCount() != 4 {
		t.Fatalf("calls = %d, want 4", fixture.callCount())
	}
	// Closed again: traffic flows.
	if _, err := scorer.Score(context.Background(), scoreRequest()); err != nil {
		t.Fatalf("breaker must be closed after a successful probe: %v", err)
	}
	if fixture.callCount() != 5 {
		t.Fatalf("calls = %d, want 5", fixture.callCount())
	}
}

func TestCircuitBreakerIgnoresCallerFaults(t *testing.T) {
	// UNAUTHENTICATED and INVALID_ARGUMENT are caller faults: they fail the
	// call but must never count toward opening the breaker.
	fixture := &scriptedScorer{fallback: func() (*riskscorev1.ScoreDeclarationResponse, error) {
		return nil, status.Error(codes.Unauthenticated, "bad token")
	}}
	scorer := newTestScorer(t, startFixtureScorer(t, fixture), func(config *GRPCScorerConfig) {
		config.MaxRetries = 0
		config.BreakerFailureThreshold = 2
	})
	for i := 0; i < 4; i++ {
		if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
			t.Fatal("unauthenticated must fail")
		}
	}
	if fixture.callCount() != 4 {
		t.Fatalf("caller faults must not trip the breaker: calls = %d, want 4", fixture.callCount())
	}
}

func TestBreakerOpenFailsClosedWithoutSilentPass(t *testing.T) {
	// Scorer down hard (connection refused) with a low threshold: the
	// breaker opens and subsequent scores fail fast. No declaration ever
	// passes silently — every path returns an error.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	scorer := newTestScorer(t, address, func(config *GRPCScorerConfig) {
		config.MaxRetries = 0
		config.Timeout = 200 * time.Millisecond
		config.BreakerFailureThreshold = 2
		config.BreakerTimeout = time.Minute
	})
	for i := 0; i < 4; i++ {
		if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
			t.Fatalf("call %d must fail closed", i+1)
		}
	}
}
