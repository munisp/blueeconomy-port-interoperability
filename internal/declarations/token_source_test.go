package declarations

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// tokenEndpointFixture is a test Keycloak token endpoint. It counts calls
// and serves the programmed response.
type tokenEndpointFixture struct {
	server *httptest.Server
	calls  atomic.Int32
	status int
	body   map[string]any
}

func newTokenEndpointFixture(t *testing.T, status int, body map[string]any) *tokenEndpointFixture {
	t.Helper()
	fixture := &tokenEndpointFixture{status: status, body: body}
	fixture.server = httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		fixture.calls.Add(1)
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err := request.ParseForm(); err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if request.PostForm.Get("grant_type") != "client_credentials" ||
			request.PostForm.Get("client_id") != "port-interoperability" ||
			request.PostForm.Get("client_secret") != "test-secret" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(fixture.status)
		_ = json.NewEncoder(writer).Encode(fixture.body)
	}))
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (fixture *tokenEndpointFixture) source(t *testing.T, configure func(*KeycloakTokenSourceConfig)) *KeycloakTokenSource {
	t.Helper()
	config := KeycloakTokenSourceConfig{
		TokenURL:     fixture.server.URL,
		ClientID:     "port-interoperability",
		ClientSecret: "test-secret",
	}
	if configure != nil {
		configure(&config)
	}
	source, err := NewKeycloakTokenSource(config)
	if err != nil {
		t.Fatalf("build token source: %v", err)
	}
	// Trust the test server's certificate (production keeps the system pool).
	source.httpClient = fixture.server.Client()
	return source
}

func bearerBody(expiresIn int64) map[string]any {
	return map[string]any{"access_token": "token-1", "token_type": "Bearer", "expires_in": expiresIn}
}

func TestTokenSourceConfigFailsClosed(t *testing.T) {
	for name, config := range map[string]KeycloakTokenSourceConfig{
		"http endpoint":    {TokenURL: "http://keycloak/token", ClientID: "c", ClientSecret: "s"},
		"empty endpoint":   {ClientID: "c", ClientSecret: "s"},
		"missing client":   {TokenURL: "https://keycloak/token", ClientSecret: "s"},
		"missing secret":   {TokenURL: "https://keycloak/token", ClientID: "c"},
		"negative refresh": {TokenURL: "https://keycloak/token", ClientID: "c", ClientSecret: "s", EarlyRefresh: -time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewKeycloakTokenSource(config); err == nil {
				t.Fatalf("%s must fail closed", name)
			}
		})
	}
}

func TestTokenSourceCachesUntilEarlyRefresh(t *testing.T) {
	fixture := newTokenEndpointFixture(t, http.StatusOK, bearerBody(300))
	source := fixture.source(t, nil)
	for i := 0; i < 3; i++ {
		token, err := source.Token(context.Background())
		if err != nil {
			t.Fatalf("token: %v", err)
		}
		if token != "token-1" {
			t.Fatalf("token = %q", token)
		}
	}
	if fixture.calls.Load() != 1 {
		t.Fatalf("endpoint calls = %d, want 1 (cached)", fixture.calls.Load())
	}
	// Advance past the early-refresh window (300s lifetime, 30s default
	// early refresh → refresh at +270s): the next call refetches.
	now := time.Now()
	source.now = func() time.Time { return now.Add(280 * time.Second) }
	if _, err := source.Token(context.Background()); err != nil {
		t.Fatalf("token after early-refresh window: %v", err)
	}
	if fixture.calls.Load() != 2 {
		t.Fatalf("endpoint calls = %d, want 2 after early refresh", fixture.calls.Load())
	}
}

func TestTokenSourceNeverCachesFailures(t *testing.T) {
	fixture := newTokenEndpointFixture(t, http.StatusServiceUnavailable, map[string]any{"error": "temporarily_unavailable"})
	source := fixture.source(t, nil)
	for i := 0; i < 2; i++ {
		if _, err := source.Token(context.Background()); err == nil {
			t.Fatal("a failing token endpoint must fail closed")
		}
	}
	if fixture.calls.Load() != 2 {
		t.Fatalf("failures must never be cached: endpoint calls = %d", fixture.calls.Load())
	}
}

func TestTokenSourceRejectsDishonestResponses(t *testing.T) {
	for name, body := range map[string]map[string]any{
		"no access token": {"token_type": "Bearer", "expires_in": 300},
		"not a bearer":    {"access_token": "t", "token_type": "mac", "expires_in": 300},
		"zero lifetime":   {"access_token": "t", "token_type": "Bearer", "expires_in": 0},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newTokenEndpointFixture(t, http.StatusOK, body)
			source := fixture.source(t, nil)
			if _, err := source.Token(context.Background()); err == nil {
				t.Fatalf("%s must be rejected", name)
			}
		})
	}
}

func TestTokenSourceDoesNotCacheShortLivedTokens(t *testing.T) {
	// A token whose lifetime fits inside the early-refresh window is used
	// once and never cached.
	fixture := newTokenEndpointFixture(t, http.StatusOK, bearerBody(20))
	source := fixture.source(t, func(config *KeycloakTokenSourceConfig) { config.EarlyRefresh = 30 * time.Second })
	for i := 0; i < 2; i++ {
		if _, err := source.Token(context.Background()); err != nil {
			t.Fatalf("token: %v", err)
		}
	}
	if fixture.calls.Load() != 2 {
		t.Fatalf("short-lived tokens must not be cached: endpoint calls = %d", fixture.calls.Load())
	}
}
