package nswsecurity

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Signer produces outbound RS256 JWS messages for the NSW operator endpoint,
// mirroring the inbound verification posture: compact serialization, kid
// header, and iss/aud/sub/tenant_id/jti/exp claims plus a SHA-256 digest of
// the exact message body so the signature is bound to the payload.
type Signer struct {
	key      *rsa.PrivateKey
	kid      string
	issuer   string
	audience string
	subject  string
	ttl      time.Duration
	now      func() time.Time
}

// OutboundClaims are signed into every outbound NSW message. PayloadSHA256
// binds the JWS to the serialized body (JSON envelope or XML handoff).
type OutboundClaims struct {
	TenantID      string
	JTI           string
	PayloadSHA256 string
}

// NewSigner fails closed: a 2048-bit-or-larger RSA key, key id, issuer,
// audience, subject and positive token TTL are all mandatory.
func NewSigner(key *rsa.PrivateKey, kid, issuer, audience, subject string, ttl time.Duration) (*Signer, error) {
	if key == nil {
		return nil, errors.New("NSW outbound signing key is required")
	}
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("NSW outbound signing key is invalid: %w", err)
	}
	if key.N.BitLen() < 2048 {
		return nil, errors.New("NSW outbound signing key must be at least 2048 bits")
	}
	if strings.TrimSpace(kid) == "" || strings.TrimSpace(issuer) == "" || strings.TrimSpace(audience) == "" || strings.TrimSpace(subject) == "" {
		return nil, errors.New("NSW outbound signer requires kid, issuer, audience and subject")
	}
	if ttl <= 0 {
		return nil, errors.New("NSW outbound token TTL must be positive")
	}
	return &Signer{key: key, kid: kid, issuer: issuer, audience: audience, subject: subject, ttl: ttl, now: time.Now}, nil
}

// LoadRSAPrivateKeyFile reads a PEM-encoded RSA private key (PKCS#1 or
// PKCS#8) from an env-injected key file. Missing or undecodable material is a
// hard error — the adapter must never run unsigned.
func LoadRSAPrivateKeyFile(path string) (*rsa.PrivateKey, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("NSW signing key file path is required")
	}
	pemBytes, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("read NSW signing key file: %w", err)
	}
	var key *rsa.PrivateKey
	for {
		var block *pem.Block
		block, pemBytes = pem.Decode(pemBytes)
		if block == nil {
			break
		}
		switch block.Type {
		case "RSA PRIVATE KEY":
			parsed, err := x509.ParsePKCS1PrivateKey(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("parse NSW PKCS#1 signing key: %w", err)
			}
			key = parsed
		case "PRIVATE KEY":
			parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("parse NSW PKCS#8 signing key: %w", err)
			}
			rsaKey, ok := parsed.(*rsa.PrivateKey)
			if !ok {
				return nil, errors.New("NSW signing key file does not contain an RSA key")
			}
			key = rsaKey
		}
	}
	if key == nil {
		return nil, errors.New("NSW signing key file contains no PEM private key")
	}
	return key, nil
}

// KID exposes the configured key id so operators can cross-check JWKS
// publications.
func (signer *Signer) KID() string {
	return signer.kid
}

// Sign builds the compact JWS for one outbound delivery. The jti is the
// delivery's replay identity and must be stable across retries of the same
// event so the NSW replay store deduplicates redelivery.
func (signer *Signer) Sign(claims OutboundClaims) (string, error) {
	if strings.TrimSpace(claims.TenantID) == "" || !strings.HasPrefix(claims.TenantID, "tenant-") {
		return "", errors.New("outbound NSW claims require a valid tenant binding")
	}
	if strings.TrimSpace(claims.JTI) == "" {
		return "", errors.New("outbound NSW claims require a replay identity (jti)")
	}
	if !strings.HasPrefix(claims.PayloadSHA256, "sha256:") {
		return "", errors.New("outbound NSW claims require the payload digest")
	}
	headerJSON, err := json.Marshal(map[string]string{"alg": "RS256", "kid": signer.kid})
	if err != nil {
		return "", fmt.Errorf("encode JWS header: %w", err)
	}
	payloadJSON, err := json.Marshal(map[string]any{
		"iss":            signer.issuer,
		"aud":            signer.audience,
		"sub":            signer.subject,
		"tenant_id":      claims.TenantID,
		"jti":            claims.JTI,
		"exp":            signer.now().UTC().Add(signer.ttl).Unix(),
		"payload_sha256": claims.PayloadSHA256,
	})
	if err != nil {
		return "", fmt.Errorf("encode JWS claims: %w", err)
	}
	head := base64.RawURLEncoding.EncodeToString(headerJSON)
	payload := base64.RawURLEncoding.EncodeToString(payloadJSON)
	digest := sha256.Sum256([]byte(head + "." + payload))
	signature, err := rsa.SignPKCS1v15(rand.Reader, signer.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign outbound NSW message: %w", err)
	}
	return head + "." + payload + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}
