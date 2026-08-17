package nswsecurity

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Policy struct {
	JWKSURL           string
	PinnedJWKSHA256   string // sha256:<lowercase hex>; empty is prohibited.
	AllowedAlgorithms map[string]bool
	AllowedKIDs       map[string]time.Time // KID -> expiry; zero means active without scheduled expiry.
	MaxClockSkew      time.Duration
	HTTPClient        *http.Client
}

type jwksDocument struct {
	Keys []jwk `json:"keys"`
}
type jwk struct {
	KTY, KID, N, E, X5C string `json:"kty","kid","n","e","x5c"`
}
type cachedKeys struct {
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time
}

type Verifier struct {
	policy Policy
	mu     sync.RWMutex
	cache  cachedKeys
}

func New(policy Policy) (*Verifier, error) {
	if !strings.HasPrefix(policy.JWKSURL, "https://") || !strings.HasPrefix(policy.PinnedJWKSHA256, "sha256:") || len(policy.AllowedAlgorithms) == 0 || len(policy.AllowedKIDs) == 0 {
		return nil, errors.New("JWKS policy must pin HTTPS authority source, digest, algorithms and KIDs")
	}
	if policy.MaxClockSkew <= 0 {
		policy.MaxClockSkew = time.Minute
	}
	if policy.HTTPClient == nil {
		policy.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Verifier{policy: policy}, nil
}

func (v *Verifier) Refresh(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.policy.JWKSURL, nil)
	if err != nil {
		return err
	}
	resp, err := v.policy.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch authority JWKS: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("authority JWKS status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	sum := sha256.Sum256(body)
	if "sha256:"+hex.EncodeToString(sum[:]) != v.policy.PinnedJWKSHA256 {
		return errors.New("authority JWKS digest pin mismatch")
	}
	var document jwksDocument
	if err := json.Unmarshal(body, &document); err != nil {
		return fmt.Errorf("decode JWKS: %w", err)
	}
	keys := map[string]*rsa.PublicKey{}
	for _, item := range document.Keys {
		expiry, allowed := v.policy.AllowedKIDs[item.KID]
		if item.KTY != "RSA" || item.KID == "" || !allowed || (!expiry.IsZero() && expiry.Before(time.Now().UTC())) {
			continue
		}
		key, err := rsaKey(item)
		if err != nil {
			return fmt.Errorf("invalid allowed JWKS key %q: %w", item.KID, err)
		}
		keys[item.KID] = key
	}
	if len(keys) == 0 {
		return errors.New("authority JWKS contains no active allowlisted RSA KID")
	}
	v.mu.Lock()
	v.cache = cachedKeys{keys: keys, fetchedAt: time.Now().UTC()}
	v.mu.Unlock()
	return nil
}

func (v *Verifier) Verify(ctx context.Context, compact string, now time.Time) error {
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		return errors.New("JWS must use compact serialization")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return errors.New("invalid JWS protected header")
	}
	var header struct {
		Alg, KID string `json:"alg","kid"`
	}
	if json.Unmarshal(headerBytes, &header) != nil || !v.policy.AllowedAlgorithms[header.Alg] || header.KID == "" {
		return errors.New("JWS protected header violates algorithm/KID policy")
	}
	if expiry, allowed := v.policy.AllowedKIDs[header.KID]; !allowed || (!expiry.IsZero() && now.After(expiry)) {
		return errors.New("JWS KID is not active")
	}
	v.mu.RLock()
	key := v.cache.keys[header.KID]
	v.mu.RUnlock()
	if key == nil {
		if err := v.Refresh(ctx); err != nil {
			return err
		}
		v.mu.RLock()
		key = v.cache.keys[header.KID]
		v.mu.RUnlock()
	}
	if key == nil {
		return errors.New("JWS KID missing from pinned authority JWKS")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return errors.New("invalid JWS signature")
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if header.Alg != "RS256" {
		return errors.New("only RS256 is implemented by this verifier")
	}
	return rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], sig)
}

func rsaKey(item jwk) (*rsa.PublicKey, error) {
	if item.X5C != "" {
		der, err := base64.StdEncoding.DecodeString(item.X5C)
		if err != nil {
			return nil, err
		}
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, err
		}
		key, ok := cert.PublicKey.(*rsa.PublicKey)
		if !ok {
			return nil, errors.New("x5c key is not RSA")
		}
		return key, nil
	}
	n, err := base64.RawURLEncoding.DecodeString(item.N)
	if err != nil {
		return nil, err
	}
	e, err := base64.RawURLEncoding.DecodeString(item.E)
	if err != nil {
		return nil, err
	}
	exponent := 0
	for _, b := range e {
		exponent = exponent<<8 + int(b)
	}
	if exponent < 3 {
		return nil, errors.New("invalid RSA exponent")
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: exponent}, nil
}
