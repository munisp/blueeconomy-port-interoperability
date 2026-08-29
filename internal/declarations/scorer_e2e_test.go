package declarations

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	riskscorev1 "github.com/munisp/blueeconomy-contracts/gen/go/blueeconomy/riskscore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// End-to-end: the production GRPCScorer + production KeycloakTokenSource
// against an in-process RiskScoreService fixture that enforces the
// contract's Keycloak RS256 authentication exactly like the real scorer
// (financial-controls internal/riskscore), backed by a test JWKS and a
// token endpoint that mints real signed service-account JWTs.

const (
	e2eIssuer    = "https://keycloak.example/realms/blueeconomy"
	e2eAudience  = "declaration-scorer" // the scorer's KEYCLOAK_EXPECTED_AUDIENCE
	e2eKeyID     = "realm-key-1"
	e2eSubject   = "service-account-port-interoperability"
	e2eClientID  = "port-interoperability"
	e2eClientKey = "e2e-secret"
)

// e2eKeycloak is a test Keycloak: a JWKS endpoint publishing the realm key
// and a token endpoint minting client_credentials RS256 JWTs.
type e2eKeycloak struct {
	key        *rsa.PrivateKey
	jwks       *httptest.Server
	tokens     *httptest.Server
	tokenCalls atomic.Int32
	audience   string // audience minted into issued tokens (mutated by negative tests)
}

func newE2EKeycloak(t *testing.T) *e2eKeycloak {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate realm key: %v", err)
	}
	fixture := &e2eKeycloak{key: key, audience: e2eAudience}
	fixture.jwks = httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"keys": []map[string]string{{
				"kid": e2eKeyID,
				"kty": "RSA",
				"alg": "RS256",
				"use": "sig",
				"n":   base64.RawURLEncoding.EncodeToString(fixture.key.PublicKey.N.Bytes()),
				"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(fixture.key.PublicKey.E)).Bytes()),
			}},
		})
	}))
	t.Cleanup(fixture.jwks.Close)
	fixture.tokens = httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		fixture.tokenCalls.Add(1)
		if err := request.ParseForm(); err != nil ||
			request.PostForm.Get("grant_type") != "client_credentials" ||
			request.PostForm.Get("client_id") != e2eClientID ||
			request.PostForm.Get("client_secret") != e2eClientKey {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		now := time.Now()
		claims := jwt.RegisteredClaims{
			Issuer:    e2eIssuer,
			Subject:   e2eSubject,
			Audience:  jwt.ClaimStrings{fixture.audience},
			ExpiresAt: jwt.NewNumericDate(now.Add(5 * time.Minute)),
			IssuedAt:  jwt.NewNumericDate(now),
		}
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		token.Header["kid"] = e2eKeyID
		signed, err := token.SignedString(fixture.key)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"access_token": signed,
			"token_type":   "Bearer",
			"expires_in":   300,
		})
	}))
	t.Cleanup(fixture.tokens.Close)
	return fixture
}

// e2eScorerFixture is an in-process RiskScoreService that enforces the
// contract's server side: Keycloak RS256 verification against the test JWKS
// (UNAUTHENTICATED on any gap), field validation (INVALID_ARGUMENT) and a
// deterministic verdict. It mirrors the financial-controls scorer semantics.
type e2eScorerFixture struct {
	riskscorev1.UnimplementedRiskScoreServiceServer
	publicKey  *rsa.PublicKey
	calls      atomic.Int32
	lastCaller atomic.Value // verified token subject
}

var e2eHSCodePattern = regexp.MustCompile(`^[0-9]{2,10}$`)

func (fixture *e2eScorerFixture) ScoreDeclaration(ctx context.Context, request *riskscorev1.ScoreDeclarationRequest) (*riskscorev1.ScoreDeclarationResponse, error) {
	fixture.calls.Add(1)
	authorization := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get("authorization"); len(values) > 0 {
			authorization = values[0]
		}
	}
	if len(authorization) < 8 || authorization[:7] != "Bearer " {
		return nil, status.Error(codes.Unauthenticated, "bearer token is absent or unverifiable")
	}
	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(authorization[7:], claims, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != jwt.SigningMethodRS256.Alg() {
			return nil, errors.New("unexpected signing method")
		}
		if token.Header["kid"] != e2eKeyID {
			return nil, errors.New("unknown signing key")
		}
		return fixture.publicKey, nil
	}, jwt.WithIssuer(e2eIssuer), jwt.WithExpirationRequired(), jwt.WithValidMethods([]string{"RS256"}))
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "bearer token is absent or unverifiable")
	}
	audienceOK := claims["azp"] == e2eAudience
	if audiences, err := claims.GetAudience(); err == nil {
		for _, audience := range audiences {
			if audience == e2eAudience {
				audienceOK = true
			}
		}
	}
	if !audienceOK {
		return nil, status.Error(codes.Unauthenticated, "token is not issued for this API audience")
	}
	subject, _ := claims.GetSubject()
	fixture.lastCaller.Store(subject)
	if request.GetDeclarationRef() == "" || !e2eHSCodePattern.MatchString(request.GetHsCode()) {
		return nil, status.Error(codes.InvalidArgument, "request violates the scoring contract")
	}
	return &riskscorev1.ScoreDeclarationResponse{
		Score:        42,
		ModelVersion: "scorer-v3-e2e",
		RuleBased:    true,
		Reasons:      []string{"invoice amount band +20", "new trader +22"},
	}, nil
}

func startE2EScorer(t *testing.T, keycloak *e2eKeycloak) (*e2eScorerFixture, string) {
	t.Helper()
	fixture := &e2eScorerFixture{publicKey: &keycloak.key.PublicKey}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	riskscorev1.RegisterRiskScoreServiceServer(server, fixture)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return fixture, listener.Addr().String()
}

func newE2ETokenSource(t *testing.T, keycloak *e2eKeycloak) *KeycloakTokenSource {
	t.Helper()
	source, err := NewKeycloakTokenSource(KeycloakTokenSourceConfig{
		TokenURL:     keycloak.tokens.URL,
		ClientID:     e2eClientID,
		ClientSecret: e2eClientKey,
	})
	if err != nil {
		t.Fatalf("build token source: %v", err)
	}
	// The test Keycloak serves the shared httptest localhost certificate.
	source.httpClient = keycloak.tokens.Client()
	return source
}

func newE2EScorer(t *testing.T, address string, tokenSource TokenSource) *GRPCScorer {
	t.Helper()
	scorer, err := NewGRPCScorer(GRPCScorerConfig{
		Address:                 address,
		Plaintext:               true,
		Timeout:                 2 * time.Second,
		MaxRetries:              2,
		BackoffBase:             time.Millisecond,
		BackoffMax:              4 * time.Millisecond,
		BreakerFailureThreshold: 100,
		TokenSource:             tokenSource,
	})
	if err != nil {
		t.Fatalf("build scorer: %v", err)
	}
	t.Cleanup(func() { _ = scorer.Close() })
	return scorer
}

func TestEndToEndScoredDeclaration(t *testing.T) {
	keycloak := newE2EKeycloak(t)
	fixture, address := startE2EScorer(t, keycloak)
	scorer := newE2EScorer(t, address, newE2ETokenSource(t, keycloak))

	verdict, err := scorer.Score(context.Background(), scoreRequest())
	if err != nil {
		t.Fatalf("end-to-end scoring failed: %v", err)
	}
	if verdict.Score != 42 || verdict.ModelVersion != "scorer-v3-e2e" {
		t.Fatalf("verdict = %+v", verdict)
	}
	if caller, _ := fixture.lastCaller.Load().(string); caller != e2eSubject {
		t.Fatalf("scorer verified caller %q, want %q", caller, e2eSubject)
	}
	// A second score reuses the cached service-account token.
	if _, err := scorer.Score(context.Background(), scoreRequest()); err != nil {
		t.Fatalf("second scoring failed: %v", err)
	}
	if keycloak.tokenCalls.Load() != 1 {
		t.Fatalf("token endpoint calls = %d, want 1 (cached client_credentials token)", keycloak.tokenCalls.Load())
	}
}

func TestEndToEndBadTokenIsUnauthenticatedAndNotRetried(t *testing.T) {
	keycloak := newE2EKeycloak(t)
	fixture, address := startE2EScorer(t, keycloak)
	// The realm mints a token for the wrong audience (misprovisioned client):
	// the scorer rejects it UNAUTHENTICATED and the client never retries and
	// never silently passes the declaration.
	keycloak.audience = "some-other-api"
	scorer := newE2EScorer(t, address, newE2ETokenSource(t, keycloak))
	if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
		t.Fatal("a wrong-audience token must fail closed")
	}
	if fixture.calls.Load() != 1 {
		t.Fatalf("UNAUTHENTICATED must never be retried: scorer calls = %d", fixture.calls.Load())
	}
}

func TestEndToEndScorerDownFailsClosed(t *testing.T) {
	keycloak := newE2EKeycloak(t)
	// Scorer down: reserve-and-release an address so nothing is listening.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	deadAddress := listener.Addr().String()
	_ = listener.Close()
	scorer := newE2EScorer(t, deadAddress, newE2ETokenSource(t, keycloak))
	if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
		t.Fatal("scorer down must fail closed — the declaration must not pass silently")
	}
}
