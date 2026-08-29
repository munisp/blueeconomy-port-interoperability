// RS256/JWKS verification for Keycloak-issued gateway tenant tokens,
// mirroring the administration-service pattern: HTTPS-only JWKS, eager
// startup fetch (fail-closed boot), RS256-only, RSA >= 2048-bit with exponent
// >= 3, redirects forbidden, and unknown-KID refreshes bounded to one per 30s
// so forged-token floods cannot stampede the identity provider. When the
// verifier is configured with a JWKS URL this path replaces HS256 entirely.
package tenantctx

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// jwksRefreshBound limits identity-provider refreshes triggered by unknown
// KIDs to one per 30 seconds.
const jwksRefreshBound = 30 * time.Second

// JWKSVerifier verifies RS256 Keycloak tokens against a pinned-HTTPS JWKS
// endpoint. Construct it with NewJWKSVerifier; the zero value is not usable.
type JWKSVerifier struct {
	issuer   string
	audience string
	jwksURL  string
	client   *http.Client
	now      func() time.Time

	mu          sync.Mutex
	keys        map[string]*rsa.PublicKey
	lastRefresh time.Time
}

// NewJWKSVerifier builds a Keycloak RS256 verifier and eagerly fetches the
// JWKS document; any failure (non-HTTPS URL, unreachable endpoint, weak or
// unusable keys) refuses startup.
func NewJWKSVerifier(jwksURL, issuer, audience, caFile string) (*JWKSVerifier, error) {
	if !strings.HasPrefix(jwksURL, "https://") {
		return nil, errors.New("tenant gateway JWKS URL must be HTTPS")
	}
	if strings.TrimSpace(issuer) == "" || strings.TrimSpace(audience) == "" {
		return nil, errors.New("tenant gateway issuer and audience are required")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if caFile != "" {
		pemBytes, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read tenant gateway JWKS CA bundle: %w", err)
		}
		pool, poolErr := x509.SystemCertPool()
		if poolErr != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, errors.New("tenant gateway JWKS CA bundle has no PEM certificates")
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	verifier := &JWKSVerifier{
		issuer:   issuer,
		audience: audience,
		jwksURL:  jwksURL,
		client: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return errors.New("JWKS redirects are not permitted")
			},
		},
		now:  time.Now,
		keys: make(map[string]*rsa.PublicKey),
	}
	if err := verifier.refresh(); err != nil {
		return nil, fmt.Errorf("fetch tenant gateway JWKS: %w", err)
	}
	return verifier, nil
}

// jwksDocument is the RFC 7517 key set subset the verifier consumes.
type jwksDocument struct {
	Keys []struct {
		KeyType   string `json:"kty"`
		Use       string `json:"use"`
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
		Modulus   string `json:"n"`
		Exponent  string `json:"e"`
	} `json:"keys"`
}

func (verifier *JWKSVerifier) refresh() error {
	response, err := verifier.client.Get(verifier.jwksURL)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("JWKS endpoint returned %s", response.Status)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read JWKS document: %w", err)
	}
	var document jwksDocument
	if err := json.Unmarshal(body, &document); err != nil {
		return fmt.Errorf("parse JWKS document: %w", err)
	}
	keys := make(map[string]*rsa.PublicKey, len(document.Keys))
	for _, jwk := range document.Keys {
		if jwk.KeyType != "RSA" || (jwk.Use != "" && jwk.Use != "sig") ||
			(jwk.Algorithm != "" && jwk.Algorithm != "RS256") || strings.TrimSpace(jwk.KeyID) == "" {
			continue
		}
		modulusBytes, err := base64.RawURLEncoding.DecodeString(jwk.Modulus)
		if err != nil {
			continue
		}
		exponentBytes, err := base64.RawURLEncoding.DecodeString(jwk.Exponent)
		if err != nil || len(exponentBytes) > 4 {
			continue
		}
		modulus := new(big.Int).SetBytes(modulusBytes)
		exponent := 0
		for _, b := range exponentBytes {
			exponent = exponent<<8 | int(b)
		}
		if modulus.BitLen() < 2048 || exponent < 3 {
			return fmt.Errorf("JWKS key %q is below the RSA 2048-bit / exponent-3 floor", jwk.KeyID)
		}
		keys[jwk.KeyID] = &rsa.PublicKey{N: modulus, E: exponent}
	}
	if len(keys) == 0 {
		return errors.New("JWKS document carries no usable RS256 signing keys")
	}
	verifier.mu.Lock()
	verifier.keys = keys
	verifier.lastRefresh = verifier.now()
	verifier.mu.Unlock()
	return nil
}

// key resolves the verification key for a KID, refreshing the JWKS document
// at most once per jwksRefreshBound when the KID is unknown.
func (verifier *JWKSVerifier) key(kid string) (*rsa.PublicKey, error) {
	verifier.mu.Lock()
	key := verifier.keys[kid]
	stale := verifier.now().Sub(verifier.lastRefresh) >= jwksRefreshBound
	verifier.mu.Unlock()
	if key != nil {
		return key, nil
	}
	if !stale {
		return nil, errors.New("unknown tenant token key id")
	}
	if err := verifier.refresh(); err != nil {
		return nil, fmt.Errorf("refresh tenant gateway JWKS: %w", err)
	}
	verifier.mu.Lock()
	key = verifier.keys[kid]
	verifier.mu.Unlock()
	if key == nil {
		return nil, errors.New("unknown tenant token key id")
	}
	return key, nil
}

// jwksClaims mirrors Claims but tolerates Keycloak's string-or-array aud and
// its realm_access role nesting.
type jwksClaims struct {
	Issuer      string          `json:"iss"`
	Audience    json.RawMessage `json:"aud"`
	TenantID    string          `json:"tenant_id"`
	Subject     string          `json:"sub"`
	Expires     json.Number     `json:"exp"`
	Roles       []string        `json:"roles"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

func audienceContains(raw json.RawMessage, audience string) bool {
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return single == audience
	}
	var multiple []string
	if err := json.Unmarshal(raw, &multiple); err == nil {
		for _, entry := range multiple {
			if entry == audience {
				return true
			}
		}
	}
	return false
}

// Verify validates an RS256 compact JWT: header alg/kid, signature against
// the JWKS key, issuer, audience, expiry and the tenant claims.
func (verifier *JWKSVerifier) Verify(token string) (Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
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
		Kid string `json:"kid"`
	}
	if json.Unmarshal(headerJSON, &header) != nil || header.Alg != "RS256" ||
		(header.Typ != "" && header.Typ != "JWT") || strings.TrimSpace(header.Kid) == "" {
		return Claims{}, errors.New("unsupported gateway token header")
	}
	key, err := verifier.key(header.Kid)
	if err != nil {
		return Claims{}, err
	}
	signature, err := decode(parts[2])
	if err != nil {
		return Claims{}, errors.New("invalid gateway token signature")
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature); err != nil {
		return Claims{}, errors.New("gateway token signature mismatch")
	}
	payloadJSON, err := decode(parts[1])
	if err != nil {
		return Claims{}, errors.New("invalid gateway token payload")
	}
	var parsed jwksClaims
	decoder := json.NewDecoder(strings.NewReader(string(payloadJSON)))
	decoder.UseNumber()
	if err := decoder.Decode(&parsed); err != nil {
		return Claims{}, errors.New("invalid gateway token payload")
	}
	if parsed.Issuer != verifier.issuer || !audienceContains(parsed.Audience, verifier.audience) ||
		!validTenantID(parsed.TenantID) || parsed.Subject == "" {
		return Claims{}, errors.New("gateway tenant claims rejected")
	}
	expires, err := parsed.Expires.Int64()
	if err != nil || expires <= verifier.now().Unix() {
		return Claims{}, errors.New("gateway tenant token expired")
	}
	return Claims{
		Issuer:   parsed.Issuer,
		Audience: verifier.audience,
		TenantID: parsed.TenantID,
		Subject:  parsed.Subject,
		Expires:  expires,
		Roles:    sanitizeRoles(append(parsed.Roles, parsed.RealmAccess.Roles...)),
	}, nil
}
