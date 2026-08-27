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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type contextKey struct{}

type Claims struct {
	Issuer   string `json:"iss"`
	Audience string `json:"aud"`
	TenantID string `json:"tenant_id"`
	Subject  string `json:"sub"`
	Expires  int64  `json:"exp"`
}

type Verifier struct {
	Key      []byte
	Issuer   string
	Audience string
	Now      func() time.Time
}

func Middleware(verifier Verifier, next http.Handler) http.Handler {
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
		// Annotate the active trace span (no-op when telemetry is disabled).
		trace.SpanFromContext(request.Context()).SetAttributes(attribute.String("blueeconomy.tenant_id", claims.TenantID))
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
