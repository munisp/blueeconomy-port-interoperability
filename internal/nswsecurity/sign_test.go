package nswsecurity

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// outboundVerifier builds the existing ingress Verifier against a pinned JWKS
// that serves the signer's public key, proving outbound messages are
// verifiable by the same verification path that guards NSW ingress.
func outboundVerifier(t *testing.T, key *rsa.PublicKey, kid, issuer, audience string) *Verifier {
	t.Helper()
	jwksBody := []byte(fmt.Sprintf(`{"keys":[{"kty":"RSA","kid":%q,"n":%q,"e":%q}]}`,
		kid,
		base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	))
	sum := sha256.Sum256(jwksBody)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwksBody)
	}))
	t.Cleanup(server.Close)
	verifier, err := New(Policy{
		JWKSURL:           server.URL,
		PinnedJWKSHA256:   "sha256:" + hex.EncodeToString(sum[:]),
		AllowedAlgorithms: map[string]bool{"RS256": true},
		AllowedKIDs:       map[string]time.Time{kid: {}},
		ExpectedIssuer:    issuer,
		ExpectedAudience:  audience,
		MaxClockSkew:      time.Minute,
		HTTPClient:        server.Client(),
	})
	if err != nil {
		t.Fatalf("build verifier: %v", err)
	}
	if err := verifier.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh pinned JWKS: %v", err)
	}
	return verifier
}

func TestSignerRoundTripVerifiesWithExistingVerifier(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate signing key: %v", err)
	}
	signer, err := NewSigner(key, "s1-outbound-2026", "s1-port-interoperability", "nsw.operator.ng", "nsw-adapter", 5*time.Minute)
	if err != nil {
		t.Fatalf("build signer: %v", err)
	}
	compact, err := signer.Sign(OutboundClaims{
		TenantID:      "tenant-apapa-port",
		JTI:           "delivery-0001",
		PayloadSHA256: "sha256:" + strings.Repeat("ab", 32),
	})
	if err != nil {
		t.Fatalf("sign outbound message: %v", err)
	}
	// The NSW operator side runs the same Verifier with swapped iss/aud pins.
	verifier := outboundVerifier(t, &key.PublicKey, "s1-outbound-2026", "s1-port-interoperability", "nsw.operator.ng")
	claims, err := verifier.Verify(context.Background(), compact, time.Now().UTC())
	if err != nil {
		t.Fatalf("verify outbound signature with existing Verifier: %v", err)
	}
	if claims.Issuer != "s1-port-interoperability" || claims.Audience != "nsw.operator.ng" ||
		claims.TenantID != "tenant-apapa-port" || claims.JTI != "delivery-0001" {
		t.Fatalf("claims = %#v, want signed outbound claims", claims)
	}
	// The payload digest claim must survive the round trip.
	parts := strings.Split(compact, ".")
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		t.Fatalf("decode claims: %v", err)
	}
	if payload["payload_sha256"] != "sha256:"+strings.Repeat("ab", 32) {
		t.Fatalf("payload_sha256 claim = %v, want the body digest", payload["payload_sha256"])
	}
}

func TestSignerFailsClosedWithoutKeyOrIdentity(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	weak, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatalf("generate weak key: %v", err)
	}
	for name, build := range map[string]func() (*Signer, error){
		"nil key":        func() (*Signer, error) { return NewSigner(nil, "kid", "iss", "aud", "sub", time.Minute) },
		"weak key":       func() (*Signer, error) { return NewSigner(weak, "kid", "iss", "aud", "sub", time.Minute) },
		"empty kid":      func() (*Signer, error) { return NewSigner(key, "", "iss", "aud", "sub", time.Minute) },
		"empty issuer":   func() (*Signer, error) { return NewSigner(key, "kid", "", "aud", "sub", time.Minute) },
		"empty audience": func() (*Signer, error) { return NewSigner(key, "kid", "iss", "", "sub", time.Minute) },
		"empty subject":  func() (*Signer, error) { return NewSigner(key, "kid", "iss", "aud", "", time.Minute) },
		"zero ttl":       func() (*Signer, error) { return NewSigner(key, "kid", "iss", "aud", "sub", 0) },
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := build(); err == nil {
				t.Fatalf("%s must be rejected (fail closed)", name)
			}
		})
	}
}

func TestSignerRejectsInvalidOutboundClaims(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	signer, err := NewSigner(key, "kid", "iss", "aud", "sub", time.Minute)
	if err != nil {
		t.Fatalf("build signer: %v", err)
	}
	for name, claims := range map[string]OutboundClaims{
		"missing tenant":   {JTI: "jti", PayloadSHA256: "sha256:" + strings.Repeat("ab", 32)},
		"non-tenant value": {TenantID: "attacker", JTI: "jti", PayloadSHA256: "sha256:" + strings.Repeat("ab", 32)},
		"missing jti":      {TenantID: "tenant-a", PayloadSHA256: "sha256:" + strings.Repeat("ab", 32)},
		"missing digest":   {TenantID: "tenant-a", JTI: "jti"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := signer.Sign(claims); err == nil {
				t.Fatalf("%s must be rejected", name)
			}
		})
	}
}

func TestLoadRSAPrivateKeyFileParsesPKCS1AndPKCS8(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	dir := t.TempDir()
	pkcs1Path := filepath.Join(dir, "pkcs1.pem")
	if err := os.WriteFile(pkcs1Path, pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}), 0o600); err != nil {
		t.Fatalf("write pkcs1: %v", err)
	}
	pkcs8Der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal pkcs8: %v", err)
	}
	pkcs8Path := filepath.Join(dir, "pkcs8.pem")
	if err := os.WriteFile(pkcs8Path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8Der}), 0o600); err != nil {
		t.Fatalf("write pkcs8: %v", err)
	}
	for _, path := range []string{pkcs1Path, pkcs8Path} {
		loaded, err := LoadRSAPrivateKeyFile(path)
		if err != nil {
			t.Fatalf("load %s: %v", path, err)
		}
		if loaded.N.Cmp(key.N) != 0 {
			t.Fatalf("loaded key from %s does not match", path)
		}
	}
}

func TestLoadRSAPrivateKeyFileFailsClosed(t *testing.T) {
	dir := t.TempDir()
	garbage := filepath.Join(dir, "garbage.pem")
	if err := os.WriteFile(garbage, []byte("not a pem file"), 0o600); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	for name, path := range map[string]string{
		"missing file": filepath.Join(dir, "absent.pem"),
		"empty path":   "",
		"garbage pem":  garbage,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadRSAPrivateKeyFile(path); err == nil {
				t.Fatalf("%s must fail closed", name)
			}
		})
	}
}
