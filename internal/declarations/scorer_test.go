package declarations

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	riskscorev1 "github.com/munisp/blueeconomy-contracts/gen/go/blueeconomy/riskscore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// staticTokenSource is a test fixture supplying a fixed token.
type staticTokenSource struct {
	token string
	err   error
}

func (source staticTokenSource) Token(context.Context) (string, error) {
	if source.err != nil {
		return "", source.err
	}
	return source.token, nil
}

// scriptedScorer is a test fixture gRPC server: it returns the scripted
// errors in order, then the fallback (or a fixed valid verdict). It records
// every call so retry and breaker tests can count attempts.
type scriptedScorer struct {
	riskscorev1.UnimplementedRiskScoreServiceServer
	mu       sync.Mutex
	calls    int
	script   []error
	fallback func() (*riskscorev1.ScoreDeclarationResponse, error)
}

func validVerdict() *riskscorev1.ScoreDeclarationResponse {
	return &riskscorev1.ScoreDeclarationResponse{Score: 42, ModelVersion: "scorer-v3", RuleBased: true}
}

func (scorer *scriptedScorer) ScoreDeclaration(_ context.Context, _ *riskscorev1.ScoreDeclarationRequest) (*riskscorev1.ScoreDeclarationResponse, error) {
	scorer.mu.Lock()
	scorer.calls++
	attempt := scorer.calls
	scorer.mu.Unlock()
	if attempt <= len(scorer.script) {
		return nil, scorer.script[attempt-1]
	}
	if scorer.fallback != nil {
		return scorer.fallback()
	}
	return validVerdict(), nil
}

func (scorer *scriptedScorer) callCount() int {
	scorer.mu.Lock()
	defer scorer.mu.Unlock()
	return scorer.calls
}

// startFixtureScorer serves the fixture on loopback TCP (plaintext — the
// production client still authenticates every RPC with a bearer token).
func startFixtureScorer(t *testing.T, impl riskscorev1.RiskScoreServiceServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	riskscorev1.RegisterRiskScoreServiceServer(server, impl)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func newTestScorer(t *testing.T, address string, configure func(*GRPCScorerConfig)) *GRPCScorer {
	t.Helper()
	config := GRPCScorerConfig{
		Address:     address,
		Plaintext:   true,
		Timeout:     2 * time.Second,
		TokenSource: staticTokenSource{token: "test-token"},
	}
	if configure != nil {
		configure(&config)
	}
	scorer, err := NewGRPCScorer(config)
	if err != nil {
		t.Fatalf("build gRPC scorer: %v", err)
	}
	t.Cleanup(func() { _ = scorer.Close() })
	return scorer
}

// reserveDeadAddress reserves a loopback address and releases it, so tests
// can point the scorer at a guaranteed-dead endpoint.
func reserveDeadAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release address: %v", err)
	}
	return address
}

func scoreRequest() ScoreRequest {
	return ScoreRequest{
		DeclarationRef:     "NCS-2026-ABC123",
		DeclarationType:    string(TypeImport),
		HSCode:             "870324",
		GoodsDescription:   "Used motor vehicles for transport",
		CountryOfOrigin:    "DE",
		PortOfEntry:        "APAPA",
		GrossWeightKg:      12000,
		NumberOfPackages:   4,
		InvoiceAmountMinor: 500000000,
		InvoiceCurrency:    "NGN",
		ConsigneeID:        "consignee-dangote-01",
		OperatorID:         "operator-apapa-01",
		TraderID:           "trader-01",
	}
}

func TestGRPCScorerConfigFailsClosed(t *testing.T) {
	valid := GRPCScorerConfig{
		Address:     "127.0.0.1:50051",
		Plaintext:   true,
		Timeout:     time.Second,
		TokenSource: staticTokenSource{token: "t"},
	}
	cases := map[string]func(*GRPCScorerConfig){
		"empty address":       func(c *GRPCScorerConfig) { c.Address = "" },
		"address with scheme": func(c *GRPCScorerConfig) { c.Address = "https://scorer.internal:50051" },
		"address without port": func(c *GRPCScorerConfig) {
			c.Address = "scorer.internal"
		},
		"zero timeout": func(c *GRPCScorerConfig) { c.Timeout = 0 },
		"missing token source": func(c *GRPCScorerConfig) {
			c.TokenSource = nil
		},
		"negative retries":   func(c *GRPCScorerConfig) { c.MaxRetries = -2 },
		"unbounded retries":  func(c *GRPCScorerConfig) { c.MaxRetries = scorerMaxRetryBudget + 1 },
		"backoff max < base": func(c *GRPCScorerConfig) { c.BackoffBase, c.BackoffMax = time.Second, time.Millisecond },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			config := valid
			mutate(&config)
			if _, err := NewGRPCScorer(config); err == nil {
				t.Fatalf("%s must fail closed", name)
			}
		})
	}
}

func TestGRPCScorerReturnsValidatedVerdict(t *testing.T) {
	fixture := &scriptedScorer{}
	scorer := newTestScorer(t, startFixtureScorer(t, fixture), nil)
	verdict, err := scorer.Score(context.Background(), scoreRequest())
	if err != nil {
		t.Fatalf("score: %v", err)
	}
	if verdict.Score != 42 || verdict.ModelVersion != "scorer-v3" {
		t.Fatalf("verdict = %+v", verdict)
	}
	if fixture.callCount() != 1 {
		t.Fatalf("calls = %d, want exactly 1", fixture.callCount())
	}
}

func TestGRPCScorerFailsClosedOnUnreachableOrInvalid(t *testing.T) {
	t.Run("unreachable", func(t *testing.T) {
		scorer := newTestScorer(t, reserveDeadAddress(t), func(config *GRPCScorerConfig) {
			config.MaxRetries = 1
			config.BackoffBase = time.Millisecond
			config.BackoffMax = 5 * time.Millisecond
			config.Timeout = 300 * time.Millisecond
		})
		if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
			t.Fatal("an unreachable scorer must error, never fabricate a score")
		}
	})
	t.Run("out-of-range score", func(t *testing.T) {
		fixture := &scriptedScorer{fallback: func() (*riskscorev1.ScoreDeclarationResponse, error) {
			return &riskscorev1.ScoreDeclarationResponse{Score: 140, ModelVersion: "scorer-v3"}, nil
		}}
		scorer := newTestScorer(t, startFixtureScorer(t, fixture), nil)
		if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
			t.Fatal("an out-of-range score must be rejected")
		}
		if fixture.callCount() != 1 {
			t.Fatalf("invalid verdicts are not retried: calls = %d", fixture.callCount())
		}
	})
	t.Run("missing model version", func(t *testing.T) {
		fixture := &scriptedScorer{fallback: func() (*riskscorev1.ScoreDeclarationResponse, error) {
			return &riskscorev1.ScoreDeclarationResponse{Score: 20}, nil
		}}
		scorer := newTestScorer(t, startFixtureScorer(t, fixture), nil)
		if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
			t.Fatal("a score without a model version must be rejected")
		}
	})
	t.Run("token source failure", func(t *testing.T) {
		fixture := &scriptedScorer{}
		scorer := newTestScorer(t, startFixtureScorer(t, fixture), func(config *GRPCScorerConfig) {
			config.TokenSource = staticTokenSource{err: errors.New("keycloak down")}
		})
		if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
			t.Fatal("a token-source failure must fail closed")
		}
		if fixture.callCount() != 0 {
			t.Fatalf("no RPC may be attempted without a token: calls = %d", fixture.callCount())
		}
	})
	t.Run("empty declaration reference", func(t *testing.T) {
		scorer := newTestScorer(t, startFixtureScorer(t, &scriptedScorer{}), nil)
		request := scoreRequest()
		request.DeclarationRef = " "
		if _, err := scorer.Score(context.Background(), request); err == nil {
			t.Fatal("a missing declaration reference must be rejected locally")
		}
	})
	t.Run("scorer internal error is not retried", func(t *testing.T) {
		fixture := &scriptedScorer{fallback: func() (*riskscorev1.ScoreDeclarationResponse, error) {
			return nil, status.Error(codes.Internal, "boom")
		}}
		scorer := newTestScorer(t, startFixtureScorer(t, fixture), nil)
		if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
			t.Fatal("a scoring service error must be rejected")
		}
		if fixture.callCount() != 1 {
			t.Fatalf("INTERNAL is not a retriable class: calls = %d", fixture.callCount())
		}
	})
}
