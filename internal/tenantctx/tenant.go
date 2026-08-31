package tenantctx

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

type contextKey struct{}

type Claims struct {
	Issuer   string `json:"iss"`
	Audience string `json:"aud"`
	TenantID string `json:"tenant_id"`
	Subject  string `json:"sub"`
	Expires  int64  `json:"exp"`
	// Roles are the verified platform PBAC roles carried by the token. They
	// are only ever populated from a verified token payload — never from
	// request bodies or headers.
	Roles []string `json:"roles,omitempty"`
}

// HasRole reports whether the verified claims carry the given platform role.
func (claims Claims) HasRole(role string) bool {
	for _, held := range claims.Roles {
		if held == role {
			return true
		}
	}
	return false
}

// HasAnyRole reports whether the verified claims carry at least one of the
// given platform roles.
func (claims Claims) HasAnyRole(roles ...string) bool {
	for _, role := range roles {
		if claims.HasRole(role) {
			return true
		}
	}
	return false
}

// validRole restricts role strings to canonical platform role names.
func validRole(value string) bool {
	if len(value) < 2 || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-') {
			return false
		}
	}
	return value[0] >= 'a' && value[0] <= 'z'
}

// sanitizeRoles drops malformed role entries and deduplicates; a token
// carrying only malformed roles verifies with zero roles (fail closed: every
// role-gated route then denies).
func sanitizeRoles(roles []string) []string {
	var clean []string
	seen := map[string]bool{}
	for _, role := range roles {
		if !validRole(role) || seen[role] {
			continue
		}
		seen[role] = true
		clean = append(clean, role)
	}
	return clean
}

type Verifier struct {
	Key      []byte
	Issuer   string
	Audience string
	Now      func() time.Time
}

// TokenVerifier is the gateway token verification boundary: the HS256
// shared-key Verifier (local loopback profile) or the RS256 Keycloak
// JWKSVerifier (production profile).
type TokenVerifier interface {
	Verify(token string) (Claims, error)
}

func Middleware(verifier TokenVerifier, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if strings.TrimSpace(request.Header.Get("X-Tenant-ID")) != "" {
			http.Error(response, "caller-supplied tenant header is prohibited", http.StatusBadRequest)
			return
		}
		claims, err := verifier.Verify(bearerToken(request.Header.Get("Authorization")))
		if err != nil {
			http.Error(response, "invalid gateway tenant token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(response, request.WithContext(context.WithValue(request.Context(), contextKey{}, claims)))
	})
}

func Tenant(request context.Context) (Claims, error) {
	claims, ok := request.Value(contextKey{}).(Claims)
	if !ok {
		return Claims{}, errors.New("validated tenant context is required")
	}
	return claims, nil
}

// WithClaims attaches already-verified claims to a context. It is used by ingress
// paths (e.g. the NSW JWS authority ingress) that verify identity through a
// non-Bearer mechanism and then need the same tenant-scoped storage behaviour.
func WithClaims(ctx context.Context, claims Claims) (context.Context, error) {
	if !validTenantID(claims.TenantID) || strings.TrimSpace(claims.Subject) == "" {
		return nil, errors.New("cannot attach unverified tenant claims")
	}
	return context.WithValue(ctx, contextKey{}, claims), nil
}

// Ready reports whether the verifier has all required fail-closed configuration.
func (verifier Verifier) Ready() bool {
	return len(verifier.Key) >= 32 && verifier.Issuer != "" && verifier.Audience != ""
}

func (verifier Verifier) Verify(token string) (Claims, error) {
	if len(verifier.Key) < 32 || verifier.Issuer == "" || verifier.Audience == "" {
		return Claims{}, errors.New("tenant verifier is not configured")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Claims{}, errors.New("gateway token must be compact JWT")
	}
	decode := base64.RawURLEncoding.DecodeString
	headerJSON, err := decode(parts[0])
	if err != nil {
		return Claims{}, errors.New("invalid gateway token header")
	}
	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	if json.Unmarshal(headerJSON, &header) != nil || header.Alg != "HS256" || (header.Typ != "" && header.Typ != "JWT") {
		return Claims{}, errors.New("unsupported gateway token header")
	}
	signature, err := decode(parts[2])
	if err != nil {
		return Claims{}, errors.New("invalid gateway token signature")
	}
	mac := hmac.New(sha256.New, verifier.Key)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return Claims{}, errors.New("gateway token signature mismatch")
	}
	payloadJSON, err := decode(parts[1])
	if err != nil {
		return Claims{}, errors.New("invalid gateway token payload")
	}
	var claims Claims
	if json.Unmarshal(payloadJSON, &claims) != nil || !validTenantID(claims.TenantID) || claims.Subject == "" || claims.Issuer != verifier.Issuer || claims.Audience != verifier.Audience {
		return Claims{}, errors.New("gateway tenant claims rejected")
	}
	claims.Roles = sanitizeRoles(claims.Roles)
	now := time.Now
	if verifier.Now != nil {
		now = verifier.Now
	}
	if claims.Expires <= now().Unix() {
		return Claims{}, errors.New("gateway tenant token expired")
	}
	return claims, nil
}

func bearerToken(value string) string {
	parts := strings.Fields(value)
	if len(parts) != 2 || parts[0] != "Bearer" {
		return ""
	}
	return parts[1]
}

func validTenantID(value string) bool {
	if len(value) < 11 || len(value) > 135 || !strings.HasPrefix(value, "tenant-") {
		return false
	}
	for _, character := range value[7:] {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '.' || character == '_' || character == ':' || character == '-') {
			return false
		}
	}
	return true
}
