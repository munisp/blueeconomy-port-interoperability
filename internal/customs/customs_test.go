package customs

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func expectation() BookingExpectation {
	return BookingExpectation{
		DeclarationRef:     "NCS-2026-ABC123",
		DeclaredWeightKg:   10000,
		ConsigneeID:        "consignee-dangote-01",
		OperatorID:         "operator-apapa-01",
		WeightToleranceBPS: 500, // 5%
	}
}

func validDeclaration() Declaration {
	return Declaration{
		DeclarationRef: "NCS-2026-ABC123",
		Status:         "VALID",
		WeightKg:       10000,
		ConsigneeID:    "consignee-dangote-01",
		OperatorID:     "operator-apapa-01",
	}
}

func TestEvaluateMatchesValidAndReleasedDeclarations(t *testing.T) {
	for _, status := range []string{"VALID", "RELEASED"} {
		declaration := validDeclaration()
		declaration.Status = status
		evaluation := Evaluate(declaration, expectation())
		if evaluation.Decision != DecisionMatch || evaluation.ReasonCode != "" {
			t.Fatalf("status %s: decision = %s/%s, want MATCH", status, evaluation.Decision, evaluation.ReasonCode)
		}
	}
}

func TestEvaluateRejectsInvalidDeclarationStatus(t *testing.T) {
	for _, status := range []string{"PENDING", "CANCELLED", "EXPIRED", ""} {
		declaration := validDeclaration()
		declaration.Status = status
		evaluation := Evaluate(declaration, expectation())
		if evaluation.Decision != DecisionMismatch || evaluation.ReasonCode != ReasonDeclarationInvalid {
			t.Fatalf("status %q: decision = %s/%s, want MISMATCH/%s", status, evaluation.Decision, evaluation.ReasonCode, ReasonDeclarationInvalid)
		}
	}
}

func TestEvaluateWeightToleranceBoundaryIsInclusive(t *testing.T) {
	// 5% of 10000 kg is exactly 500 kg: the boundary itself must still match.
	for name, weight := range map[string]int64{
		"exact upper boundary": 10500,
		"exact lower boundary": 9500,
		"one over upper":       10501,
		"one under lower":      9499,
		"identical":            10000,
	} {
		declaration := validDeclaration()
		declaration.WeightKg = weight
		evaluation := Evaluate(declaration, expectation())
		switch name {
		case "exact upper boundary", "exact lower boundary", "identical":
			if evaluation.Decision != DecisionMatch {
				t.Fatalf("%s (%d kg): decision = %s/%s, want MATCH", name, weight, evaluation.Decision, evaluation.ReasonCode)
			}
		default:
			if evaluation.Decision != DecisionMismatch || evaluation.ReasonCode != ReasonWeightTolerance {
				t.Fatalf("%s (%d kg): decision = %s/%s, want MISMATCH/%s", name, weight, evaluation.Decision, evaluation.ReasonCode, ReasonWeightTolerance)
			}
		}
	}
}

func TestEvaluateRejectsConsigneeAndOperatorMismatch(t *testing.T) {
	declaration := validDeclaration()
	declaration.ConsigneeID = "consignee-other"
	if evaluation := Evaluate(declaration, expectation()); evaluation.ReasonCode != ReasonConsigneeMismatch {
		t.Fatalf("consignee mismatch: reason = %s, want %s", evaluation.ReasonCode, ReasonConsigneeMismatch)
	}
	declaration = validDeclaration()
	declaration.OperatorID = "operator-other"
	if evaluation := Evaluate(declaration, expectation()); evaluation.ReasonCode != ReasonOperatorMismatch {
		t.Fatalf("operator mismatch: reason = %s, want %s", evaluation.ReasonCode, ReasonOperatorMismatch)
	}
	declaration = validDeclaration()
	declaration.ConsigneeID = ""
	if evaluation := Evaluate(declaration, expectation()); evaluation.ReasonCode != ReasonConsigneeMismatch {
		t.Fatalf("empty consignee: reason = %s, want %s", evaluation.ReasonCode, ReasonConsigneeMismatch)
	}
}

func TestWeightWithinToleranceRejectsDegenerateInputs(t *testing.T) {
	if weightWithinTolerance(0, 10000, 500) || weightWithinTolerance(10000, 0, 500) || weightWithinTolerance(10000, 10000, -1) {
		t.Fatal("degenerate weights or negative tolerance must never match")
	}
}

// pinnedValidator builds an HTTPValidator pinned to the test server's
// self-signed certificate.
func pinnedValidator(t *testing.T, server *httptest.Server, configure func(*HTTPConfig)) *HTTPValidator {
	t.Helper()
	der := server.Certificate().Raw
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})) {
		t.Fatal("test server certificate must be PEM encodable")
	}
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write pinned CA: %v", err)
	}
	config := HTTPConfig{
		BaseURL:     server.URL,
		BearerToken: "test-bearer-token",
		CACertFile:  caFile,
		Timeout:     2 * time.Second,
	}
	if configure != nil {
		configure(&config)
	}
	validator, err := NewHTTPValidator(config)
	if err != nil {
		t.Fatalf("build validator: %v", err)
	}
	return validator
}

func TestHTTPValidatorFetchesDeclarationWithBearerAndPinnedCA(t *testing.T) {
	var sawAuthorization, sawPath string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuthorization = r.Header.Get("Authorization")
		sawPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(validDeclaration())
	}))
	defer server.Close()
	validator := pinnedValidator(t, server, nil)
	declaration, err := validator.Declaration(context.Background(), "NCS-2026-ABC123")
	if err != nil {
		t.Fatalf("fetch declaration: %v", err)
	}
	if declaration.DeclarationRef != "NCS-2026-ABC123" || declaration.Status != "VALID" {
		t.Fatalf("declaration = %#v", declaration)
	}
	if sawAuthorization != "Bearer test-bearer-token" {
		t.Fatalf("authorization header = %q, want bearer token", sawAuthorization)
	}
	if sawPath != "/v1/declarations/NCS-2026-ABC123" {
		t.Fatalf("request path = %q", sawPath)
	}
}

func TestHTTPValidatorMaps404ToDeclarationNotFound(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	validator := pinnedValidator(t, server, nil)
	_, err := validator.Declaration(context.Background(), "NCS-MISSING")
	if !errors.Is(err, ErrDeclarationNotFound) {
		t.Fatalf("error = %v, want ErrDeclarationNotFound", err)
	}
}

func TestHTTPValidatorFailsClosedWhenUnreachableOrBroken(t *testing.T) {
	t.Run("server error", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()
		validator := pinnedValidator(t, server, nil)
		if _, err := validator.Declaration(context.Background(), "NCS-2026-ABC123"); err == nil || errors.Is(err, ErrDeclarationNotFound) {
			t.Fatalf("error = %v, want a reachable-but-broken validator error", err)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(3 * time.Second)
		}))
		defer server.Close()
		validator := pinnedValidator(t, server, nil)
		if _, err := validator.Declaration(context.Background(), "NCS-2026-ABC123"); err == nil {
			t.Fatal("a hung validator must surface a timeout error, never a fabricated declaration")
		}
	})
	t.Run("connection refused", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
		validator := pinnedValidator(t, server, nil)
		server.Close() // down before the call
		if _, err := validator.Declaration(context.Background(), "NCS-2026-ABC123"); err == nil {
			t.Fatal("an unreachable validator must error, never fabricate a declaration")
		}
	})
	t.Run("ref echo mismatch", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			declaration := validDeclaration()
			declaration.DeclarationRef = "NCS-OTHER"
			_ = json.NewEncoder(w).Encode(declaration)
		}))
		defer server.Close()
		validator := pinnedValidator(t, server, nil)
		if _, err := validator.Declaration(context.Background(), "NCS-2026-ABC123"); err == nil {
			t.Fatal("a declaration ref echo mismatch must be rejected")
		}
	})
}

func TestHTTPValidatorConfigFailsClosed(t *testing.T) {
	for name, config := range map[string]HTTPConfig{
		"non-https base URL":     {BaseURL: "http://customs.example", BearerToken: "token", Timeout: time.Second},
		"missing auth":           {BaseURL: "https://customs.example", Timeout: time.Second},
		"bearer and mtls":        {BaseURL: "https://customs.example", BearerToken: "token", ClientCertFile: "c", ClientKeyFile: "k", Timeout: time.Second},
		"zero timeout":           {BaseURL: "https://customs.example", BearerToken: "token"},
		"missing CA file":        {BaseURL: "https://customs.example", BearerToken: "token", CACertFile: "/nonexistent/ca.pem", Timeout: time.Second},
		"mtls missing key files": {BaseURL: "https://customs.example", ClientCertFile: "/nonexistent/c.pem", ClientKeyFile: "/nonexistent/k.pem", Timeout: time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewHTTPValidator(config); err == nil {
				t.Fatalf("%s must fail closed", name)
			}
		})
	}
}
