package nswsecurity

import (
	"encoding/json"
	"testing"
)

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
