package declarations

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	riskscorev1 "github.com/munisp/blueeconomy-contracts/gen/go/blueeconomy/riskscore/v1"
	"github.com/sony/gobreaker/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Resilience defaults for the scorer call path (PRA-066..068): a bounded
// retry budget with exponential backoff and jitter, and a circuit breaker
// that opens on consecutive scorer-side failures. INVALID_ARGUMENT and
// UNAUTHENTICATED are never retried and never trip the breaker — they are
// caller faults, not scorer health signals.
const (
	scorerDefaultMaxRetries      = 3
	scorerDefaultBackoffBase     = 100 * time.Millisecond
	scorerDefaultBackoffMax      = 2 * time.Second
	scorerDefaultBreakerFailures = 5
	scorerDefaultBreakerTimeout  = 30 * time.Second
	scorerMaxRetryBudget         = 10
)

// GRPCScorerConfig wires the fail-closed gRPC scorer client (Phase-7). The
// token source is mandatory: the scorer requires a Keycloak RS256
// service-account token on every RPC (PRA-126) — the retired static bearer
// token has no equivalent here.
type GRPCScorerConfig struct {
	// Address is the scorer's host:port gRPC listen address.
	Address string
	// CACertFile optionally pins the scorer's TLS certificate chain; the
	// system pool is used when empty.
	CACertFile string
	// Plaintext disables transport security. It exists for in-process test
	// fixtures and loopback development only; production wiring (cmd/) never
	// sets it and authentication tokens would never safely cross a plaintext
	// link off-loopback.
	Plaintext bool
	// Timeout bounds each individual RPC attempt.
	Timeout time.Duration
	// MaxRetries bounds retries on UNAVAILABLE/DEADLINE_EXCEEDED (attempts
	// total = 1 + MaxRetries). -1 selects the platform default (3), 0
	// disables retries, and the budget is hard-capped at 10.
	MaxRetries int
	// BackoffBase and BackoffMax bound the exponential backoff with jitter
	// between retries.
	BackoffBase time.Duration
	BackoffMax  time.Duration
	// BreakerFailureThreshold is the consecutive retriable-failure count
	// that opens the circuit breaker (default 5); BreakerTimeout is the
	// open-to-half-open cooldown (default 30s).
	BreakerFailureThreshold uint32
	BreakerTimeout          time.Duration
	// TokenSource supplies the Keycloak client_credentials bearer tokens.
	TokenSource TokenSource
	// DialOptions are appended to the dial for test fixtures (e.g. bufconn).
	DialOptions []grpc.DialOption
}

// GRPCScorer is the production Scorer against the declaration scorer's
// blueeconomy.riskscore.v1.RiskScoreService gRPC contract. It preserves the
// HTTP predecessor's fail-closed semantics exactly: any transport failure,
// authentication gap or malformed verdict is an error and the declaration
// parks in SCORING_UNAVAILABLE — nothing is ever silently passed.
type GRPCScorer struct {
	client      riskscorev1.RiskScoreServiceClient
	conn        *grpc.ClientConn
	tokenSource TokenSource
	timeout     time.Duration
	maxRetries  int
	backoffBase time.Duration
	backoffMax  time.Duration
	breaker     *gobreaker.CircuitBreaker[any]
	sleep       func(ctx context.Context, d time.Duration) error
}

// NewGRPCScorer builds the client and fails closed on any invalid
// configuration.
func NewGRPCScorer(config GRPCScorerConfig) (*GRPCScorer, error) {
	host, port, found := strings.Cut(config.Address, ":")
	if !found || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" || strings.Contains(config.Address, "://") {
		return nil, errors.New("DECLARATIONS_SCORER_GRPC_ADDR must be a host:port address")
	}
	if config.Timeout <= 0 {
		return nil, errors.New("declarations scorer timeout must be positive")
	}
	if config.TokenSource == nil {
		return nil, errors.New("declarations scorer token source is required")
	}
	maxRetries := config.MaxRetries
	if maxRetries == -1 {
		maxRetries = scorerDefaultMaxRetries
	}
	if maxRetries < 0 || maxRetries > scorerMaxRetryBudget {
		return nil, fmt.Errorf("declarations scorer max retries must be -1 (default) or within [0,%d]", scorerMaxRetryBudget)
	}
	backoffBase := config.BackoffBase
	if backoffBase <= 0 {
		backoffBase = scorerDefaultBackoffBase
	}
	backoffMax := config.BackoffMax
	if backoffMax <= 0 {
		backoffMax = scorerDefaultBackoffMax
	}
	if backoffMax < backoffBase {
		return nil, errors.New("declarations scorer backoff max must not be below the base")
	}
	breakerFailures := config.BreakerFailureThreshold
	if breakerFailures == 0 {
		breakerFailures = scorerDefaultBreakerFailures
	}
	breakerTimeout := config.BreakerTimeout
	if breakerTimeout <= 0 {
		breakerTimeout = scorerDefaultBreakerTimeout
	}
	dialOptions := []grpc.DialOption{}
	if config.Plaintext {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if config.CACertFile != "" {
			pem, err := os.ReadFile(filepath.Clean(config.CACertFile))
			if err != nil {
				return nil, fmt.Errorf("read scorer CA certificate: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("scorer CA certificate file contains no PEM certificates")
			}
			tlsConfig.RootCAs = pool
		}
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}
	dialOptions = append(dialOptions, config.DialOptions...)
	conn, err := grpc.NewClient(config.Address, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("dial declaration scorer: %w", err)
	}
	scorer := &GRPCScorer{
		client:      riskscorev1.NewRiskScoreServiceClient(conn),
		conn:        conn,
		tokenSource: config.TokenSource,
		timeout:     config.Timeout,
		maxRetries:  maxRetries,
		backoffBase: backoffBase,
		backoffMax:  backoffMax,
		sleep: func(ctx context.Context, d time.Duration) error {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
	}
	scorer.breaker = gobreaker.NewCircuitBreaker[any](gobreaker.Settings{
		Name:        "declaration-scorer",
		MaxRequests: 1,
		Timeout:     breakerTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= breakerFailures
		},
	})
	return scorer, nil
}

// Close releases the underlying connection.
func (scorer *GRPCScorer) Close() error {
	return scorer.conn.Close()
}

// retriableScorerError reports whether an RPC failure is a scorer-side
// availability signal (retryable, breaker-counted) as opposed to a caller
// fault (INVALID_ARGUMENT/UNAUTHENTICATED — never retried, never counted).
func retriableScorerError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// callOutcome carries a non-retriable RPC error through the circuit breaker
// without counting it as a scorer failure: caller faults say nothing about
// scorer health.
type callOutcome struct {
	response *riskscorev1.ScoreDeclarationResponse
	err      error
}

// Score scores the declaration snapshot with the configured resilience
// policy. Every failure mode returns an error; the caller's fail-closed
// handling (terminal SCORING_UNAVAILABLE) is unchanged from the HTTP path.
func (scorer *GRPCScorer) Score(ctx context.Context, request ScoreRequest) (ScoreResponse, error) {
	if strings.TrimSpace(request.DeclarationRef) == "" {
		return ScoreResponse{}, errors.New("declaration reference is required for scoring")
	}
	token, err := scorer.tokenSource.Token(ctx)
	if err != nil {
		return ScoreResponse{}, fmt.Errorf("risk scorer authentication unavailable: %w", err)
	}
	protoRequest := &riskscorev1.ScoreDeclarationRequest{
		DeclarationRef:       request.DeclarationRef,
		DeclarationType:      request.DeclarationType,
		HsCode:               request.HSCode,
		GoodsDescription:     request.GoodsDescription,
		CountryOfOrigin:      request.CountryOfOrigin,
		CountryOfDestination: request.CountryOfDestination,
		PortOfEntry:          request.PortOfEntry,
		GrossWeightKg:        request.GrossWeightKg,
		NumberOfPackages:     int32(request.NumberOfPackages),
		InvoiceAmountMinor:   request.InvoiceAmountMinor,
		InvoiceCurrency:      request.InvoiceCurrency,
		ConsigneeId:          request.ConsigneeID,
		OperatorId:           request.OperatorID,
		TraderId:             request.TraderID,
		IsAeo:                request.IsAEO,
	}
	var lastErr error
	for attempt := 0; attempt <= scorer.maxRetries; attempt++ {
		if attempt > 0 {
			if err := scorer.sleep(ctx, scorer.backoff(attempt)); err != nil {
				return ScoreResponse{}, fmt.Errorf("risk scoring interrupted: %w", err)
			}
		}
		callCtx, cancel := context.WithTimeout(ctx, scorer.timeout)
		callCtx = metadata.AppendToOutgoingContext(callCtx, "authorization", "Bearer "+token)
		result, breakerErr := scorer.breaker.Execute(func() (any, error) {
			response, rpcErr := scorer.client.ScoreDeclaration(callCtx, protoRequest)
			if rpcErr != nil && !retriableScorerError(rpcErr) {
				return callOutcome{err: rpcErr}, nil
			}
			return callOutcome{response: response}, rpcErr
		})
		cancel()
		if breakerErr != nil {
			if errors.Is(breakerErr, gobreaker.ErrOpenState) || errors.Is(breakerErr, gobreaker.ErrTooManyRequests) {
				// The breaker is the fail-closed boundary: fast error, no retry
				// storm against a scorer that is already failing.
				return ScoreResponse{}, fmt.Errorf("risk scorer circuit breaker is open: %w", breakerErr)
			}
			// Retriable RPC failure, counted by the breaker.
			lastErr = fmt.Errorf("risk scorer unreachable: %w", breakerErr)
			continue
		}
		outcome := result.(callOutcome)
		if outcome.err != nil {
			// Caller fault: never retried.
			return ScoreResponse{}, fmt.Errorf("risk scorer rejected the request (%s): %w", status.Code(outcome.err), outcome.err)
		}
		return validateScorerVerdict(outcome.response)
	}
	return ScoreResponse{}, fmt.Errorf("risk scorer unreachable after %d attempts: %w", scorer.maxRetries+1, lastErr)
}

// backoff computes the exponential delay with ±25% jitter for the given
// 1-based retry attempt.
func (scorer *GRPCScorer) backoff(attempt int) time.Duration {
	delay := scorer.backoffBase
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= scorer.backoffMax {
			delay = scorer.backoffMax
			break
		}
	}
	if delay > scorer.backoffMax {
		delay = scorer.backoffMax
	}
	if delay < 8*time.Nanosecond {
		return delay
	}
	jitter := time.Duration(rand.Int64N(int64(delay)/2)) - time.Duration(int64(delay)/4)
	return delay + jitter
}

// validateScorerVerdict enforces the response contract exactly like the
// retired HTTP client: an out-of-range score or a missing model version is
// as fatal as a connection failure.
func validateScorerVerdict(response *riskscorev1.ScoreDeclarationResponse) (ScoreResponse, error) {
	if response == nil {
		return ScoreResponse{}, errors.New("risk scorer returned no verdict")
	}
	if response.GetScore() < 0 || response.GetScore() > 100 {
		return ScoreResponse{}, fmt.Errorf("risk scorer returned an out-of-range score %d", response.GetScore())
	}
	if strings.TrimSpace(response.GetModelVersion()) == "" {
		return ScoreResponse{}, errors.New("risk scorer response is missing the model version")
	}
	return ScoreResponse{
		Score:        int(response.GetScore()),
		ModelVersion: response.GetModelVersion(),
		Sanctioned:   response.GetSanctioned(),
	}, nil
}
