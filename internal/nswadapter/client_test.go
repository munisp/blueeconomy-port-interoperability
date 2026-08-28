package nswadapter

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/nswsecurity"
)

func writeTestCA(t *testing.T) string {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	return path
}

type capturedRequest struct {
	body        []byte
	contentType string
	signature   string
}

// nswFixture is an httptest NSW operator endpoint with a pinned CA and a
// signer whose compact JWS the server verifies with the signer's public key.
type nswFixture struct {
	server  *httptest.Server
	client  *Client
	signer  *nswsecurity.Signer
	key     *rsa.PrivateKey
	capture chan capturedRequest
}

func newNSWFixture(t *testing.T, status int) nswFixture {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate signing key: %v", err)
	}
	fixture := nswFixture{key: key, capture: make(chan capturedRequest, 16)}
	fixture.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fixture.capture <- capturedRequest{body: body, contentType: r.Header.Get("Content-Type"), signature: r.Header.Get(SignatureHeader)}
		if status == http.StatusFound {
			w.Header().Set("Location", "https://evil.example/collect")
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(fixture.server.Close)
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: fixture.server.Certificate().Raw}), 0o600); err != nil {
		t.Fatalf("write pinned CA: %v", err)
	}
	config := Config{
		EndpointURL:  fixture.server.URL,
		CACertFile:   caFile,
		Timeout:      2 * time.Second,
		MaxAttempts:  3,
		BackoffBase:  time.Second,
		BackoffMax:   time.Minute,
		PollInterval: time.Second,
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	fixture.client, err = NewClient(config)
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	fixture.signer, err = nswsecurity.NewSigner(key, "s1-outbound-2026", "s1-port-interoperability", "nsw.operator.ng", "nsw-adapter", 5*time.Minute)
	if err != nil {
		t.Fatalf("build signer: %v", err)
	}
	return fixture
}

// verifySignature re-runs the RS256 verification and claim checks the NSW
// operator side performs, and returns the decoded claims for caller-specific
// assertions (tenant, jti).
func (fixture nswFixture) verifySignature(t *testing.T, request capturedRequest) map[string]any {
	t.Helper()
	parts := strings.Split(request.signature, ".")
	if len(parts) != 3 {
		t.Fatalf("signature is not compact JWS: %q", request.signature)
	}
	var header struct {
		Alg string `json:"alg"`
		KID string `json:"kid"`
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || json.Unmarshal(headerBytes, &header) != nil {
		t.Fatalf("decode JWS header: %v", err)
	}
	if header.Alg != "RS256" || header.KID != "s1-outbound-2026" {
		t.Fatalf("JWS header = %#v", header)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(&fixture.key.PublicKey, crypto.SHA256, digest[:], signature); err != nil {
		t.Fatalf("NSW endpoint cannot verify the outbound signature: %v", err)
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode claims: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		t.Fatalf("decode claims: %v", err)
	}
	if claims["iss"] != "s1-port-interoperability" || claims["aud"] != "nsw.operator.ng" {
		t.Fatalf("claims = %v", claims)
	}
	exp, ok := claims["exp"].(float64)
	if !ok || time.Unix(int64(exp), 0).Before(time.Now()) {
		t.Fatalf("exp claim = %v, want a future expiry", claims["exp"])
	}
	bodyDigest := sha256.Sum256(request.body)
	want := "sha256:" + hexDigest(bodyDigest[:])
	if claims["payload_sha256"] != want {
		t.Fatalf("payload_sha256 = %v, want %s (digest of the exact body)", claims["payload_sha256"], want)
	}
	return claims
}

func hexDigest(b []byte) string {
	const digits = "0123456789abcdef"
	var out [64]byte
	for i, v := range b {
		out[i*2] = digits[v>>4]
		out[i*2+1] = digits[v&0x0f]
	}
	return string(out[:])
}

func (fixture nswFixture) sendOne(t *testing.T, body string) error {
	t.Helper()
	digest := sha256.Sum256([]byte(body))
	signature, err := fixture.signer.Sign(nswsecurity.OutboundClaims{
		TenantID:      "tenant-apapa-port",
		JTI:           "delivery-0001",
		PayloadSHA256: "sha256:" + hexDigest(digest[:]),
	})
	if err != nil {
		t.Fatalf("sign delivery: %v", err)
	}
	return fixture.client.Send(context.Background(), []byte(body), ContentTypeJSON, signature)
}

func TestClientDeliversSignedMessageOverPinnedHTTPS(t *testing.T) {
	fixture := newNSWFixture(t, http.StatusOK)
	if err := fixture.sendOne(t, `{"envelopeVersion":"1.0"}`); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	request := <-fixture.capture
	if request.contentType != ContentTypeJSON {
		t.Fatalf("content type = %q", request.contentType)
	}
	claims := fixture.verifySignature(t, request)
	if claims["tenant_id"] != "tenant-apapa-port" || claims["jti"] != "delivery-0001" {
		t.Fatalf("claims = %v", claims)
	}
}

func TestClientTreatsReplayConflictAsDelivered(t *testing.T) {
	fixture := newNSWFixture(t, http.StatusConflict)
	if err := fixture.sendOne(t, `{"redelivered":true}`); err != nil {
		t.Fatalf("409 replay dedup must count as delivered: %v", err)
	}
}

func TestClientFailsOnServerError(t *testing.T) {
	fixture := newNSWFixture(t, http.StatusInternalServerError)
	if err := fixture.sendOne(t, `{"x":1}`); err == nil {
		t.Fatal("a 500 from the NSW endpoint must consume an attempt")
	}
}

func TestClientNeverFollowsRedirects(t *testing.T) {
	fixture := newNSWFixture(t, http.StatusFound)
	if err := fixture.sendOne(t, `{"x":1}`); err == nil {
		t.Fatal("redirects must fail closed, never be followed")
	}
}

func TestClientRefusesUnsignedMessages(t *testing.T) {
	fixture := newNSWFixture(t, http.StatusOK)
	if err := fixture.client.Send(context.Background(), []byte(`{}`), ContentTypeJSON, ""); err == nil {
		t.Fatal("the client must refuse to send an unsigned message")
	}
}
