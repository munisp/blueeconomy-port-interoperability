package tenantctx

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type jwksFixture struct {
	verifier *JWKSVerifier
	key      *rsa.PrivateKey
	kid      string
	server   *httptest.Server
}

func newJWKSFixture(t *testing.T, key *rsa.PrivateKey) jwksFixture {
	t.Helper()
	kid := "test-key-1"
	jwks := map[string]any{"keys": []map[string]any{{
		"kty": "RSA",
		"use": "sig",
		"alg": "RS256",
		"kid": kid,
		"n":   base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes()),
	}}}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	t.Cleanup(server.Close)
	caFile := filepath.Join(t.TempDir(), "jwks-ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(caFile, pemBytes, 0o600); err != nil {
		t.Fatalf("write CA bundle: %v", err)
	}
	verifier, err := NewJWKSVerifier(server.URL, "https://keycloak.blueeconomy.ng/realms/ports", "s1-port-interoperability", caFile)
	if err != nil {
		t.Fatalf("build JWKS verifier: %v", err)
	}
	return jwksFixture{verifier: verifier, key: key, kid: kid, server: server}
}

func generateRSA(t *testing.T, bits int) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	return key
}

func (fixture jwksFixture) mintToken(t *testing.T, claims map[string]any) string {
	t.Helper()
	encode := func(value any) string {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("encode token part: %v", err)
		}
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	header := encode(map[string]string{"alg": "RS256", "typ": "JWT", "kid": fixture.kid})
	payload := encode(claims)
	digest := sha256.Sum256([]byte(header + "." + payload))
	signature, err := rsa.SignPKCS1v15(rand.Reader, fixture.key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return fmt.Sprintf("%s.%s.%s", header, payload, base64.RawURLEncoding.EncodeToString(signature))
}

func validClaims() map[string]any {
	return map[string]any{
		"iss":       "https://keycloak.blueeconomy.ng/realms/ports",
		"aud":       "s1-port-interoperability",
		"tenant_id": "tenant-apapa-001",
		"sub":       "trader-1",
		"exp":       time.Now().Add(5 * time.Minute).Unix(),
	}
}

func TestJWKSVerifierRoundTrip(t *testing.T) {
	fixture := newJWKSFixture(t, generateRSA(t, 2048))
	claims, err := fixture.verifier.Verify(fixture.mintToken(t, validClaims()))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if claims.TenantID != "tenant-apapa-001" || claims.Subject != "trader-1" {
		t.Fatalf("claims = %+v", claims)
	}
	// Keycloak array-form aud is accepted.
	arrayAud := validClaims()
	arrayAud["aud"] = []string{"other", "s1-port-interoperability"}
	if _, err := fixture.verifier.Verify(fixture.mintToken(t, arrayAud)); err != nil {
		t.Fatalf("array aud must verify: %v", err)
	}
}

func TestJWKSVerifierRejectsForgery(t *testing.T) {
	fixture := newJWKSFixture(t, generateRSA(t, 2048))
	t.Run("wrong signing key", func(t *testing.T) {
		attacker := jwksFixture{key: generateRSA(t, 2048), kid: fixture.kid}
		if _, err := fixture.verifier.Verify(attacker.mintToken(t, validClaims())); err == nil {
			t.Fatal("a token signed by another key must be rejected")
		}
	})
	t.Run("payload tamper", func(t *testing.T) {
		token := fixture.mintToken(t, validClaims())
		parts := []byte(token)
		// Flip a payload character without touching the signature.
		for i := 0; i < len(token); i++ {
			if token[i] == '.' {
				if parts[i+1] == 'a' {
					parts[i+1] = 'b'
				} else {
					parts[i+1] = 'a'
				}
				break
			}
		}
		if _, err := fixture.verifier.Verify(string(parts)); err == nil {
			t.Fatal("a tampered payload must be rejected")
		}
	})
	t.Run("hs256 alg confusion", func(t *testing.T) {
		encode := func(value any) string {
			raw, _ := json.Marshal(value)
			return base64.RawURLEncoding.EncodeToString(raw)
		}
		token := encode(map[string]string{"alg": "HS256", "typ": "JWT"}) + "." + encode(validClaims()) + "." + encode("sig")
		if _, err := fixture.verifier.Verify(token); err == nil {
			t.Fatal("HS256 tokens must be rejected in JWKS mode")
		}
	})
	t.Run("unknown kid", func(t *testing.T) {
		other := jwksFixture{key: fixture.key, kid: "unknown-kid"}
		if _, err := fixture.verifier.Verify(other.mintToken(t, validClaims())); err == nil {
			t.Fatal("an unknown kid must be rejected")
		}
	})
	t.Run("expired", func(t *testing.T) {
		claims := validClaims()
		claims["exp"] = time.Now().Add(-time.Minute).Unix()
		if _, err := fixture.verifier.Verify(fixture.mintToken(t, claims)); err == nil {
			t.Fatal("an expired token must be rejected")
		}
	})
	t.Run("wrong issuer", func(t *testing.T) {
		claims := validClaims()
		claims["iss"] = "https://attacker.example"
		if _, err := fixture.verifier.Verify(fixture.mintToken(t, claims)); err == nil {
			t.Fatal("a foreign issuer must be rejected")
		}
	})
	t.Run("wrong audience", func(t *testing.T) {
		claims := validClaims()
		claims["aud"] = "other-service"
		if _, err := fixture.verifier.Verify(fixture.mintToken(t, claims)); err == nil {
			t.Fatal("a foreign audience must be rejected")
		}
	})
	t.Run("bad tenant id", func(t *testing.T) {
		claims := validClaims()
		claims["tenant_id"] = "not-a-tenant"
		if _, err := fixture.verifier.Verify(fixture.mintToken(t, claims)); err == nil {
			t.Fatal("a malformed tenant_id must be rejected")
		}
	})
}

func TestJWKSVerifierFailsClosedAtStartup(t *testing.T) {
	if _, err := NewJWKSVerifier("http://insecure.example/jwks", "iss", "aud", ""); err == nil {
		t.Fatal("a non-HTTPS JWKS URL must be refused")
	}
	if _, err := NewJWKSVerifier("https://127.0.0.1:1/jwks", "iss", "aud", ""); err == nil {
		t.Fatal("an unreachable JWKS endpoint must refuse startup")
	}
	if _, err := NewJWKSVerifier("https://jwks.example", "", "aud", ""); err == nil {
		t.Fatal("a missing issuer must be refused")
	}
	// A JWKS document with only a weak (1024-bit) key refuses startup.
	weakServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		weak := generateRSA(t, 1024)
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{{
			"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "weak",
			"n": base64.RawURLEncoding.EncodeToString(weak.PublicKey.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(weak.PublicKey.E)).Bytes()),
		}}})
	}))
	defer weakServer.Close()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: weakServer.Certificate().Raw}), 0o600); err != nil {
		t.Fatalf("write CA bundle: %v", err)
	}
	if _, err := NewJWKSVerifier(weakServer.URL, "iss", "aud", caFile); err == nil {
		t.Fatal("a sub-2048-bit JWKS key must refuse startup")
	}
	if _, err := NewJWKSVerifier(weakServer.URL, "iss", "aud", filepath.Join(t.TempDir(), "missing.pem")); err == nil {
		t.Fatal("a missing CA bundle must be refused")
	}
}
