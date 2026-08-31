package declarations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// TokenSource supplies Keycloak access tokens for the declaration scorer
// RPCs (PRA-126). The static DECLARATIONS_SCORER_BEARER_TOKEN path is
// retired: every token is fetched from the realm token endpoint via the
// client_credentials grant.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// KeycloakTokenSourceConfig wires a client_credentials service-account token
// source. The token endpoint must be HTTPS and the client credentials are
// env-only secrets; there is no default and no fallback. The provisioned
// service-account client must carry an audience mapper for the scorer's
// approved audience (contract value "declaration-scorer") so issued tokens
// satisfy the scorer's KEYCLOAK_EXPECTED_AUDIENCE check.
type KeycloakTokenSourceConfig struct {
	TokenURL     string
	ClientID     string
	ClientSecret string
	// Scopes is the optional space-separated scope list requested with the
	// client_credentials grant.
	Scopes []string
	// EarlyRefresh is how long before expiry a cached token is renewed;
	// defaults to 30 seconds. Tokens issued with a lifetime shorter than
	// the early-refresh window are used once and never cached.
	EarlyRefresh time.Duration
}

const (
	defaultEarlyRefresh      = 30 * time.Second
	maxTokenEndpointResponse = 1 << 20
)

// KeycloakTokenSource fetches and caches client_credentials access tokens.
// Tokens are cached until the early-refresh window before their expiry;
// every fetch failure is an error (fail closed) and is never cached.
type KeycloakTokenSource struct {
	config     KeycloakTokenSourceConfig
	httpClient *http.Client
	now        func() time.Time

	mu        sync.Mutex
	token     string
	refreshAt time.Time
}

type tokenEndpointResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
}

// NewKeycloakTokenSource validates the configuration fail closed.
func NewKeycloakTokenSource(config KeycloakTokenSourceConfig) (*KeycloakTokenSource, error) {
	parsed, err := url.Parse(config.TokenURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, errors.New("KEYCLOAK_TOKEN_URL must be an HTTPS URL")
	}
	if strings.TrimSpace(config.ClientID) == "" || strings.TrimSpace(config.ClientSecret) == "" {
		return nil, errors.New("KEYCLOAK_CLIENT_ID and KEYCLOAK_CLIENT_SECRET are required")
	}
	if config.EarlyRefresh < 0 {
		return nil, errors.New("token early-refresh window must not be negative")
	}
	return &KeycloakTokenSource{
		config: config,
		httpClient: &http.Client{
			// otelhttp transport: token fetches become CLIENT spans (no-op
			// when telemetry is disabled).
			Transport: otelhttp.NewTransport(http.DefaultTransport),
			Timeout:   10 * time.Second,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return errors.New("token endpoint redirects are not permitted")
			},
		},
		now: time.Now,
	}, nil
}

// Token returns a cached token or fetches a fresh one from the realm token
// endpoint. A nil error always carries a non-empty token.
func (source *KeycloakTokenSource) Token(ctx context.Context) (string, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.token != "" && source.now().Before(source.refreshAt) {
		return source.token, nil
	}
	// client_credentials fetch span: a cache miss is a Keycloak round-trip
	// and lands as a child of the first scoring call that needed the token.
	ctx, span := tracer().Start(ctx, "keycloak.client_credentials.fetch")
	defer span.End()
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {source.config.ClientID},
		"client_secret": {source.config.ClientSecret},
	}
	if len(source.config.Scopes) > 0 {
		form.Set("scope", strings.Join(source.config.Scopes, " "))
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, source.config.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("build token request: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Accept", "application/json")
	response, err := source.httpClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("request access token: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxTokenEndpointResponse+1))
	if err != nil {
		return "", fmt.Errorf("read token response: %w", err)
	}
	if len(body) > maxTokenEndpointResponse {
		return "", errors.New("token endpoint response exceeds the size bound")
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token endpoint answered status %d", response.StatusCode)
	}
	var granted tokenEndpointResponse
	if err := json.Unmarshal(body, &granted); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}
	if granted.AccessToken == "" || !strings.EqualFold(granted.TokenType, "Bearer") {
		return "", errors.New("token endpoint returned no bearer access token")
	}
	if granted.ExpiresIn <= 0 {
		return "", errors.New("token endpoint returned a non-positive token lifetime")
	}
	earlyRefresh := source.config.EarlyRefresh
	if earlyRefresh == 0 {
		earlyRefresh = defaultEarlyRefresh
	}
	if lifetime := time.Duration(granted.ExpiresIn) * time.Second; lifetime > earlyRefresh {
		source.token = granted.AccessToken
		source.refreshAt = source.now().Add(lifetime - earlyRefresh)
	} else {
		// Too short-lived to cache safely: use once, refetch next time.
		source.token = ""
		source.refreshAt = time.Time{}
	}
	return granted.AccessToken, nil
}
