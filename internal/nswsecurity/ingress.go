package nswsecurity

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"time"
)

// ReplayStore must atomically reserve a hash before calling the business handler.
// Implementations should use a PostgreSQL unique key and return (false,nil) on replay.
type ReplayStore interface {
	Reserve(ctx context.Context, replayHash string, expiresAt time.Time) (bool, error)
}

type claimsContextKey struct{}

// ClaimsFrom returns the validated NSW authority claims attached by the ingress
// middleware. Handlers behind NewIngress must fail closed when claims are absent.
func ClaimsFrom(ctx context.Context) (Claims, error) {
	claims, ok := ctx.Value(claimsContextKey{}).(Claims)
	if !ok {
		return Claims{}, errors.New("validated NSW authority claims are required")
	}
	return claims, nil
}

type IngressConfig struct {
	SignatureHeader string
	Verifier        *Verifier
	ReplayStore     ReplayStore
	ReplayTTL       time.Duration
	Now             func() time.Time
}

func NewIngress(config IngressConfig, next http.Handler) (http.Handler, error) {
	if config.Verifier == nil || config.ReplayStore == nil || strings.TrimSpace(config.SignatureHeader) == "" || config.ReplayTTL <= 0 || next == nil {
		return nil, errors.New("NSW ingress requires verifier, replay store, signature header, TTL and downstream handler")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compact := strings.TrimSpace(r.Header.Get(config.SignatureHeader))
		if compact == "" {
			http.Error(w, "missing authority signature", http.StatusUnauthorized)
			return
		}
		now := config.Now().UTC()
		claims, err := config.Verifier.Verify(r.Context(), compact, now)
		if err != nil {
			http.Error(w, "authority signature rejected", http.StatusUnauthorized)
			return
		}
		sum := sha256.Sum256([]byte(claims.JTI))
		reserved, err := config.ReplayStore.Reserve(r.Context(), base64.RawURLEncoding.EncodeToString(sum[:]), now.Add(config.ReplayTTL))
		if err != nil {
			http.Error(w, "replay store unavailable", http.StatusServiceUnavailable)
			return
		}
		if !reserved {
			http.Error(w, "replayed authority message", http.StatusConflict)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), claimsContextKey{}, claims)))
	}), nil
}
