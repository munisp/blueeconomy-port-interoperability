package nswsecurity

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type authorityFixture struct {
	key       *rsa.PrivateKey
	kid       string
	jwksBody  []byte
	jwksURL   string
	pinnedSHA string
	client    *http.Client
}

func newAuthorityFixture(t *testing.T) authorityFixture {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate authority key: %v", err)
	}
	fixture := authorityFixture{key: key, kid: "authority-key-2026"}
	fixture.jwksBody = []byte(fmt.Sprintf(`{"keys":[{"kty":"RSA","kid":%q,"n":%q,"e":%q}]}`,
		fixture.kid,
		base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
		base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes()),
	))
	sum := sha256.Sum256(fixture.jwksBody)
	fixture.pinnedSHA = "sha256:" + hex.EncodeToString(sum[:])
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(fixture.jwksBody)
	}))
	t.Cleanup(server.Close)
	fixture.jwksURL = server.URL
	fixture.client = server.Client()
	return fixture
}

func (fixture authorityFixture) verifier(t *testing.T, algorithms map[string]bool) *Verifier {
	t.Helper()
	verifier, err := New(Policy{
		JWKSURL:           fixture.jwksURL,
		PinnedJWKSHA256:   fixture.pinnedSHA,
		AllowedAlgorithms: algorithms,
		AllowedKIDs:       map[string]time.Time{fixture.kid: {}},
		ExpectedIssuer:    "nsw.authority.ng",
		ExpectedAudience:  "s1-port-interoperability",
		MaxClockSkew:      time.Minute,
		HTTPClient:        fixture.client,
	})
	if err != nil {
		t.Fatalf("build verifier: %v", err)
	}
	if err := verifier.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh pinned JWKS: %v", err)
	}
	return verifier
}

func (fixture authorityFixture) sign(t *testing.T, header map[string]any, claims map[string]any) string {
	t.Helper()
	headerJSON, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	head := base64.RawURLEncoding.EncodeToString(headerJSON)
	payload := base64.RawURLEncoding.EncodeToString(claimsJSON)
	digest := sha256.Sum256([]byte(head + "." + payload))
	signature, err := rsa.SignPKCS1v15(rand.Reader, fixture.key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return head + "." + payload + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func validClaims(now time.Time) map[string]any {
	return map[string]any{
		"iss":       "nsw.authority.ng",
		"aud":       "s1-port-interoperability",
		"sub":       "nsw-clearance-officer-7",
		"tenant_id": "tenant-apapa-port",
		"jti":       "nsw-msg-0001",
		"exp":       now.Add(5 * time.Minute).Unix(),
	}
}

func TestVerifierAcceptsValidAuthoritySignatureAndClaims(t *testing.T) {
	fixture := newAuthorityFixture(t)
	verifier := fixture.verifier(t, map[string]bool{"RS256": true})
	now := time.Now().UTC()
	token := fixture.sign(t, map[string]any{"alg": "RS256", "kid": fixture.kid}, validClaims(now))
	claims, err := verifier.Verify(context.Background(), token, now)
	if err != nil {
		t.Fatalf("verify valid token: %v", err)
	}
	if claims.Issuer != "nsw.authority.ng" || claims.Audience != "s1-port-interoperability" ||
		claims.TenantID != "tenant-apapa-port" || claims.JTI != "nsw-msg-0001" {
		t.Fatalf("claims = %#v, want validated NSW authority claims", claims)
	}
}

func TestVerifierRejectsTamperedPayload(t *testing.T) {
	fixture := newAuthorityFixture(t)
	verifier := fixture.verifier(t, map[string]bool{"RS256": true})
	now := time.Now().UTC()
	token := fixture.sign(t, map[string]any{"alg": "RS256", "kid": fixture.kid}, validClaims(now))
	parts := strings.Split(token, ".")
	// Swap in a different payload without re-signing.
	forged := validClaims(now)
	forged["tenant_id"] = "tenant-attacker"
	forgedClaims, _ := json.Marshal(forged)
	parts[1] = base64.RawURLEncoding.EncodeToString(forgedClaims)
	if _, err := verifier.Verify(context.Background(), strings.Join(parts, "."), now); err == nil {
		t.Fatal("tampered payload must be rejected")
	}
}

func TestVerifierRejectsSignatureFromWrongKey(t *testing.T) {
	fixture := newAuthorityFixture(t)
	verifier := fixture.verifier(t, map[string]bool{"RS256": true})
	now := time.Now().UTC()
	attacker, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate attacker key: %v", err)
	}
	headerJSON, _ := json.Marshal(map[string]any{"alg": "RS256", "kid": fixture.kid})
	claimsJSON, _ := json.Marshal(validClaims(now))
	head := base64.RawURLEncoding.EncodeToString(headerJSON)
	payload := base64.RawURLEncoding.EncodeToString(claimsJSON)
	digest := sha256.Sum256([]byte(head + "." + payload))
	signature, err := rsa.SignPKCS1v15(rand.Reader, attacker, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatalf("attacker sign: %v", err)
	}
	token := head + "." + payload + "." + base64.RawURLEncoding.EncodeToString(signature)
	if _, err := verifier.Verify(context.Background(), token, now); err == nil {
		t.Fatal("signature from a non-authority key must be rejected")
	}
}

func TestVerifierRejectsExpiredToken(t *testing.T) {
	fixture := newAuthorityFixture(t)
	verifier := fixture.verifier(t, map[string]bool{"RS256": true})
	now := time.Now().UTC()
	claims := validClaims(now)
	claims["exp"] = now.Add(-10 * time.Minute).Unix()
	token := fixture.sign(t, map[string]any{"alg": "RS256", "kid": fixture.kid}, claims)
	if _, err := verifier.Verify(context.Background(), token, now); err == nil {
		t.Fatal("expired authority token must be rejected")
	}
}

func TestVerifierRejectsWrongIssuerAndAudience(t *testing.T) {
	fixture := newAuthorityFixture(t)
	verifier := fixture.verifier(t, map[string]bool{"RS256": true})
	now := time.Now().UTC()
	for name, mutate := range map[string]func(map[string]any){
		"wrong issuer":   func(claims map[string]any) { claims["iss"] = "evil.example" },
		"wrong audience": func(claims map[string]any) { claims["aud"] = "other-service" },
		"missing jti":    func(claims map[string]any) { delete(claims, "jti") },
		"missing tenant": func(claims map[string]any) { delete(claims, "tenant_id") },
		"missing exp":    func(claims map[string]any) { delete(claims, "exp") },
	} {
		t.Run(name, func(t *testing.T) {
			claims := validClaims(now)
			mutate(claims)
			token := fixture.sign(t, map[string]any{"alg": "RS256", "kid": fixture.kid}, claims)
			if _, err := verifier.Verify(context.Background(), token, now); err == nil {
				t.Fatalf("%s must be rejected", name)
			}
		})
	}
}

func TestPolicyRejectsSharedSecretAlgorithms(t *testing.T) {
	fixture := newAuthorityFixture(t)
	for _, algorithm := range []string{"HS256", "HS384", "HS512", "none"} {
		_, err := New(Policy{
			JWKSURL:           fixture.jwksURL,
			PinnedJWKSHA256:   fixture.pinnedSHA,
			AllowedAlgorithms: map[string]bool{algorithm: true},
			AllowedKIDs:       map[string]time.Time{fixture.kid: {}},
			ExpectedIssuer:    "nsw.authority.ng",
			ExpectedAudience:  "s1-port-interoperability",
			HTTPClient:        fixture.client,
		})
		if err == nil {
			t.Fatalf("algorithm %q must be prohibited for NSW ingress", algorithm)
		}
	}
}

func TestVerifierRejectsHS256HeaderEvenIfSignedWithRSAKeyMaterial(t *testing.T) {
	fixture := newAuthorityFixture(t)
	verifier := fixture.verifier(t, map[string]bool{"RS256": true})
	now := time.Now().UTC()
	// Craft an HS256-header token: the allowlist must reject it before signature handling.
	headerJSON, _ := json.Marshal(map[string]any{"alg": "HS256", "kid": fixture.kid})
	claimsJSON, _ := json.Marshal(validClaims(now))
	head := base64.RawURLEncoding.EncodeToString(headerJSON)
	payload := base64.RawURLEncoding.EncodeToString(claimsJSON)
	token := head + "." + payload + "." + base64.RawURLEncoding.EncodeToString([]byte("forged"))
	if _, err := verifier.Verify(context.Background(), token, now); err == nil {
		t.Fatal("HS256-header token must be rejected by the algorithm allowlist")
	}
}

func TestVerifierRejectsUnknownKeyID(t *testing.T) {
	fixture := newAuthorityFixture(t)
	verifier := fixture.verifier(t, map[string]bool{"RS256": true})
	now := time.Now().UTC()
	token := fixture.sign(t, map[string]any{"alg": "RS256", "kid": "attacker-kid"}, validClaims(now))
	if _, err := verifier.Verify(context.Background(), token, now); err == nil {
		t.Fatal("token with non-allowlisted KID must be rejected")
	}
}

func TestVerifierRejectsCorruptedSignatureBytes(t *testing.T) {
	fixture := newAuthorityFixture(t)
	verifier := fixture.verifier(t, map[string]bool{"RS256": true})
	now := time.Now().UTC()
	token := fixture.sign(t, map[string]any{"alg": "RS256", "kid": fixture.kid}, validClaims(now))
	parts := strings.Split(token, ".")
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	signature[0] ^= 0xff
	parts[2] = base64.RawURLEncoding.EncodeToString(signature)
	if _, err := verifier.Verify(context.Background(), strings.Join(parts, "."), now); err == nil {
		t.Fatal("corrupted signature must be rejected")
	}
}

func TestJWKSDocumentDecodesAllRequiredKeyFields(t *testing.T) {
	var document jwksDocument
	if err := json.Unmarshal([]byte(`{"keys":[{"kty":"RSA","kid":"authority-key-2026","n":"modulus","e":"AQAB","x5c":"certificate"}]}`), &document); err != nil {
		t.Fatalf("decode JWKS document: %v", err)
	}
	if len(document.Keys) != 1 {
		t.Fatalf("decoded key count = %d, want 1", len(document.Keys))
	}
	key := document.Keys[0]
	if key.KTY != "RSA" || key.KID != "authority-key-2026" || key.N != "modulus" || key.E != "AQAB" || key.X5C != "certificate" {
		t.Fatalf("decoded JWK = %#v, want all supplied fields", key)
	}
}

func TestProtectedHeaderDecodesAlgorithmAndKeyID(t *testing.T) {
	var header protectedHeader
	if err := json.Unmarshal([]byte(`{"alg":"RS256","kid":"authority-key-2026"}`), &header); err != nil {
		t.Fatalf("decode protected header: %v", err)
	}
	if header.Alg != "RS256" || header.KID != "authority-key-2026" {
		t.Fatalf("decoded protected header = %#v, want RS256 and authority key ID", header)
	}
}
