package declarations

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// pinnedScorer builds an HTTPScorer pinned to the test server's self-signed
// certificate.
func pinnedScorer(t *testing.T, server *httptest.Server, configure func(*ScorerConfig)) *HTTPScorer {
	t.Helper()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatalf("write pinned CA: %v", err)
	}
	config := ScorerConfig{BaseURL: server.URL, CACertFile: caFile, Timeout: 2 * time.Second}
	if configure != nil {
		configure(&config)
	}
	scorer, err := NewHTTPScorer(config)
	if err != nil {
		t.Fatalf("build scorer: %v", err)
	}
	return scorer
}

func scoreRequest() ScoreRequest {
	return ScoreRequest{
		DeclarationRef:     "NCS-2026-ABC123",
		DeclarationType:    string(TypeImport),
		HSCode:             "870324",
		GoodsDescription:   "Used motor vehicles for transport",
		CountryOfOrigin:    "DE",
		PortOfEntry:        "APAPA",
		GrossWeightKg:      12000,
		NumberOfPackages:   4,
		InvoiceAmountMinor: 500000000,
		InvoiceCurrency:    "NGN",
		ConsigneeID:        "consignee-dangote-01",
		OperatorID:         "operator-apapa-01",
		TraderID:           "trader-01",
	}
}

func TestHTTPScorerConfigFailsClosed(t *testing.T) {
	for name, config := range map[string]ScorerConfig{
		"non-https URL": {BaseURL: "http://scorer.internal", Timeout: time.Second},
		"empty URL":     {BaseURL: "", Timeout: time.Second},
		"zero timeout":  {BaseURL: "https://scorer.internal"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewHTTPScorer(config); err == nil {
				t.Fatalf("%s must fail closed", name)
			}
		})
	}
}

func TestHTTPScorerReturnsValidatedVerdict(t *testing.T) {
	var sawAuthorization bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuthorization = r.Header.Get("Authorization") == "Bearer scorer-token"
		if r.URL.Path != "/v1/risk-scores" || r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var request ScoreRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.DeclarationRef != "NCS-2026-ABC123" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(ScoreResponse{Score: 42, ModelVersion: "scorer-v3"})
	}))
	defer server.Close()
	scorer := pinnedScorer(t, server, func(config *ScorerConfig) { config.BearerToken = "scorer-token" })
	verdict, err := scorer.Score(context.Background(), scoreRequest())
	if err != nil {
		t.Fatalf("score: %v", err)
	}
	if verdict.Score != 42 || verdict.ModelVersion != "scorer-v3" {
		t.Fatalf("verdict = %+v", verdict)
	}
	if !sawAuthorization {
		t.Fatal("bearer token must be sent")
	}
}

func TestHTTPScorerFailsClosedOnUnreachableOrInvalid(t *testing.T) {
	t.Run("unreachable", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
		scorer := pinnedScorer(t, server, nil)
		server.Close()
		if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
			t.Fatal("an unreachable scorer must error, never fabricate a score")
		}
	})
	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(3 * time.Second)
		}))
		defer server.Close()
		scorer := pinnedScorer(t, server, func(config *ScorerConfig) { config.Timeout = time.Second })
		if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
			t.Fatal("a hung scorer must surface a timeout error")
		}
	})
	t.Run("out-of-range score", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"score": 140, "model_version": "scorer-v3"})
		}))
		defer server.Close()
		scorer := pinnedScorer(t, server, nil)
		if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
			t.Fatal("an out-of-range score must be rejected")
		}
	})
	t.Run("missing model version", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"score": 20})
		}))
		defer server.Close()
		scorer := pinnedScorer(t, server, nil)
		if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
			t.Fatal("a score without a model version must be rejected")
		}
	})
	t.Run("malformed body", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("not-json"))
		}))
		defer server.Close()
		scorer := pinnedScorer(t, server, nil)
		if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
			t.Fatal("a malformed response must be rejected")
		}
	})
	t.Run("server error", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()
		scorer := pinnedScorer(t, server, nil)
		if _, err := scorer.Score(context.Background(), scoreRequest()); err == nil {
			t.Fatal("a scoring service error must be rejected")
		}
	})
}
