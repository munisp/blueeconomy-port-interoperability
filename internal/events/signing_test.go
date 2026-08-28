package events

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func generateKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return key
}

func TestSignerRoundTripVerify(t *testing.T) {
	signer, err := NewSigner(generateKey(t), "7")
	if err != nil {
		t.Fatalf("build signer: %v", err)
	}
	envelope, err := Message(
		"trade.declaration.cleared.v1", TopicDeclarations, "req-0001", "DECL-2026-0001",
		json.RawMessage(`{"declaration_ref":"DECL-2026-0001","status":"CLEARED"}`),
		map[string]string{"declaration-ref": "DECL-2026-0001"},
		Provenance{PrincipalID: "customs-engine", PrincipalRole: "customs-engine"},
		time.Now().UTC(), signer,
	)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	parts := strings.Split(envelope.Provenance.Signature, ".")
	if len(parts) != 3 {
		t.Fatalf("provenance signature must be a JWS compact serialization, got %d parts", len(parts))
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatalf("decode protected header: %v", err)
	}
	var parsed struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}
	if err := json.Unmarshal(header, &parsed); err != nil {
		t.Fatalf("parse protected header: %v", err)
	}
	if parsed.Algorithm != "EdDSA" || parsed.KeyID != "port-interoperability-7" {
		t.Fatalf("protected header = %#v, want alg=EdDSA kid=port-interoperability-7", parsed)
	}
	if !envelope.VerifySignature(signer.PublicKey()) {
		t.Fatal("signed envelope must round-trip verify")
	}
	// The serialized envelope (with a reserialized FHIR key order) must still
	// verify: the JCS payload is canonicalization-stable.
	serialized, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	var decoded Envelope
	if err := json.Unmarshal(serialized, &decoded); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if !decoded.VerifySignature(signer.PublicKey()) {
		t.Fatal("reserialized envelope must still verify")
	}
}

func TestSignerTamperDetection(t *testing.T) {
	signer, err := NewSigner(generateKey(t), "3")
	if err != nil {
		t.Fatalf("build signer: %v", err)
	}
	build := func(t *testing.T) Envelope {
		t.Helper()
		envelope, err := Message(
			"booking.paid", TopicBooking, "req-0001", "booking-0001",
			json.RawMessage(`{"booking_id":"booking-0001","amount_kobo":250000}`),
			nil, Provenance{PrincipalID: "p", PrincipalRole: "r"},
			time.Now().UTC(), signer,
		)
		if err != nil {
			t.Fatalf("build envelope: %v", err)
		}
		return envelope
	}
	t.Run("payload tamper", func(t *testing.T) {
		envelope := build(t)
		envelope.FHIR = json.RawMessage(`{"resourceType":"Bundle","type":"message","entry":[]}`)
		if envelope.VerifySignature(signer.PublicKey()) {
			t.Fatal("tampered FHIR payload must fail verification")
		}
	})
	t.Run("principal tamper", func(t *testing.T) {
		envelope := build(t)
		envelope.Provenance.PrincipalID = "attacker"
		if envelope.VerifySignature(signer.PublicKey()) {
			t.Fatal("tampered principal must fail verification")
		}
	})
	t.Run("classification tamper", func(t *testing.T) {
		envelope := build(t)
		envelope.Classification = "PUBLIC"
		if envelope.VerifySignature(signer.PublicKey()) {
			t.Fatal("tampered classification must fail verification")
		}
	})
	t.Run("signature swap", func(t *testing.T) {
		envelope := build(t)
		other := build(t)
		envelope.Provenance.Signature = other.Provenance.Signature
		if envelope.VerifySignature(signer.PublicKey()) {
			t.Fatal("a signature from another envelope must fail verification")
		}
	})
	t.Run("wrong key", func(t *testing.T) {
		envelope := build(t)
		attackerPublic := generateKey(t).Public().(ed25519.PublicKey)
		if envelope.VerifySignature(attackerPublic) {
			t.Fatal("verification under a wrong public key must fail")
		}
	})
	t.Run("alg confusion", func(t *testing.T) {
		envelope := build(t)
		parts := strings.Split(envelope.Provenance.Signature, ".")
		forged := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","kid":"port-interoperability-3"}`))
		envelope.Provenance.Signature = forged + "." + parts[1] + "." + parts[2]
		if envelope.VerifySignature(signer.PublicKey()) {
			t.Fatal("an alg-confused header must fail verification")
		}
	})
	t.Run("foreign kid", func(t *testing.T) {
		envelope := build(t)
		parts := strings.Split(envelope.Provenance.Signature, ".")
		forged := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"EdDSA","kid":"other-service-3"}`))
		envelope.Provenance.Signature = forged + "." + parts[1] + "." + parts[2]
		if envelope.VerifySignature(signer.PublicKey()) {
			t.Fatal("a kid from another producer must fail verification")
		}
	})
}

func TestSignerFailsClosedWithoutKey(t *testing.T) {
	t.Setenv(SigningKeyEnv, "")
	t.Setenv(SigningKeyEpochEnv, "")
	if _, err := SignerFromEnv(); err == nil {
		t.Fatal("startup must refuse to sign without a private key")
	}
	t.Setenv(SigningKeyEnv, "not-base64-not-hex!!!")
	t.Setenv(SigningKeyEpochEnv, "1")
	if _, err := SignerFromEnv(); err == nil {
		t.Fatal("startup must refuse an undecodable private key")
	}
	t.Setenv(SigningKeyEnv, base64.StdEncoding.EncodeToString([]byte("too short")))
	if _, err := SignerFromEnv(); err == nil {
		t.Fatal("startup must refuse a wrong-length private key")
	}
	validKey := generateKey(t)
	t.Setenv(SigningKeyEnv, base64.StdEncoding.EncodeToString(validKey))
	t.Setenv(SigningKeyEpochEnv, "")
	if _, err := SignerFromEnv(); err == nil {
		t.Fatal("startup must refuse a missing key epoch")
	}
	t.Setenv(SigningKeyEpochEnv, "not-a-number")
	if _, err := SignerFromEnv(); err == nil {
		t.Fatal("startup must refuse a non-decimal key epoch")
	}
	t.Setenv(SigningKeyEpochEnv, "12")
	signer, err := SignerFromEnv()
	if err != nil {
		t.Fatalf("valid env configuration must build a signer: %v", err)
	}
	if signer.KeyID() != "port-interoperability-12" {
		t.Fatalf("kid = %q, want port-interoperability-12", signer.KeyID())
	}
}

func TestParsePrivateKeyEncodings(t *testing.T) {
	key := generateKey(t)
	seed := key.Seed()
	for name, encoded := range map[string]string{
		"base64-std seed":       base64.StdEncoding.EncodeToString(seed),
		"base64-rawurl seed":    base64.RawURLEncoding.EncodeToString(seed),
		"hex seed":              hex.EncodeToString(seed),
		"base64-std private":    base64.StdEncoding.EncodeToString(key),
		"base64-rawurl private": base64.RawURLEncoding.EncodeToString(key),
	} {
		parsed, err := ParsePrivateKey(encoded)
		if err != nil {
			t.Fatalf("%s must decode: %v", name, err)
		}
		if !parsed.Equal(key) {
			t.Fatalf("%s decoded to a different key", name)
		}
	}
	if _, err := NewSigner(nil, "1"); err == nil {
		t.Fatal("a nil private key must fail closed")
	}
	if _, err := NewSigner(key, ""); err == nil {
		t.Fatal("an empty epoch must fail closed")
	}
}

func TestMessageFailsClosedWithoutSigner(t *testing.T) {
	if _, err := Message(
		"booking.paid", TopicBooking, "req-0001", "booking-0001",
		json.RawMessage(`{}`), nil,
		Provenance{PrincipalID: "p", PrincipalRole: "r"},
		time.Now().UTC(), nil,
	); err == nil {
		t.Fatal("a nil signer must fail closed")
	}
}
