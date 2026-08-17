package nswsecurity

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
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
		if err := config.Verifier.Verify(r.Context(), compact, now); err != nil {
			http.Error(w, "authority signature rejected", http.StatusUnauthorized)
			return
		}
		jti, err := protectedJTI(compact)
		if err != nil {
			http.Error(w, "authority signature missing replay identity", http.StatusUnauthorized)
			return
		}
		sum := sha256.Sum256([]byte(jti))
		reserved, err := config.ReplayStore.Reserve(r.Context(), base64.RawURLEncoding.EncodeToString(sum[:]), now.Add(config.ReplayTTL))
		if err != nil {
			http.Error(w, "replay store unavailable", http.StatusServiceUnavailable)
			return
		}
		if !reserved {
			http.Error(w, "replayed authority message", http.StatusConflict)
			return
		}
		next.ServeHTTP(w, r)
	}), nil
}

func protectedJTI(compact string) (string, error) {
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		return "", errors.New("invalid compact JWS")
	}
	bytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", err
	}
	var header struct {
		JTI string `json:"jti"`
	}
	if err := json.Unmarshal(bytes, &header); err != nil || strings.TrimSpace(header.JTI) == "" {
		return "", errors.New("missing jti")
	}
	return header.JTI, nil
}
