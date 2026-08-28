package declarations

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Scorer is the pluggable risk-scoring boundary. Implementations must be
// fail-closed: any transport failure or invalid response is an error and the
// declaration is parked in the terminal SCORING_UNAVAILABLE state. There is
// no hash, heuristic or LLM fallback anywhere in this package.
type Scorer interface {
	Score(ctx context.Context, request ScoreRequest) (ScoreResponse, error)
}

// ScoreRequest is the declaration snapshot handed to the risk scorer.
type ScoreRequest struct {
	DeclarationRef       string `json:"declaration_ref"`
	DeclarationType      string `json:"declaration_type"`
	HSCode               string `json:"hs_code"`
	GoodsDescription     string `json:"goods_description"`
	CountryOfOrigin      string `json:"country_of_origin"`
	CountryOfDestination string `json:"country_of_destination,omitempty"`
	PortOfEntry          string `json:"port_of_entry"`
	GrossWeightKg        int64  `json:"gross_weight_kg"`
	NumberOfPackages     int    `json:"number_of_packages"`
	InvoiceAmountMinor   int64  `json:"invoice_amount_minor"`
	InvoiceCurrency      string `json:"invoice_currency"`
	ConsigneeID          string `json:"consignee_id"`
	OperatorID           string `json:"operator_id"`
	TraderID             string `json:"trader_id"`
	IsAEO                bool   `json:"is_aeo"`
}

// ScoreResponse is the scorer's verdict. Lane assignment is computed locally
// by the ported business rules from Score — the scorer never assigns lanes.
type ScoreResponse struct {
	Score        int    `json:"score"`
	ModelVersion string `json:"model_version"`
	Sanctioned   bool   `json:"sanctioned,omitempty"`
}

// ScorerConfig configures the HTTP scorer client. The base URL must be HTTPS
// and the timeout positive; a bearer token is optional but never defaulted,
// and an optional CA file pins the scoring service certificate chain.
type ScorerConfig struct {
	BaseURL     string
	BearerToken string
	CACertFile  string // optional pinned CA; system pool when empty
	Timeout     time.Duration
}

const maxScorerResponseSize = 1 << 20

// HTTPScorer is the production Scorer against the configured scoring
// service. It bounds the response body, never follows redirects and rejects
// any response that is not a well-formed score.
type HTTPScorer struct {
	baseURL string
	bearer  string
	client  *http.Client
}

// NewHTTPScorer builds the fail-closed scorer client; missing or invalid
// configuration is a startup error.
func NewHTTPScorer(config ScorerConfig) (*HTTPScorer, error) {
	parsed, err := url.Parse(config.BaseURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, errors.New("DECLARATIONS_SCORER_URL must be an HTTPS URL")
	}
	if config.Timeout <= 0 {
		return nil, errors.New("declarations scorer timeout must be positive")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if config.CACertFile != "" {
		pem, err := os.ReadFile(filepath.Clean(config.CACertFile))
		if err != nil {
			return nil, fmt.Errorf("read scorer CA certificate: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("scorer CA certificate file contains no PEM certificates")
		}
		tlsConfig.RootCAs = pool
	}
	return &HTTPScorer{
		baseURL: strings.TrimRight(config.BaseURL, "/"),
		bearer:  config.BearerToken,
		client: &http.Client{
			Timeout:   config.Timeout,
			Transport: &http.Transport{TLSClientConfig: tlsConfig},
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// Score posts the declaration snapshot and validates the verdict. A 2xx with
// an out-of-range or otherwise malformed score is as fatal as a connection
// failure: the caller must treat every error as SCORING_UNAVAILABLE.
func (scorer *HTTPScorer) Score(ctx context.Context, request ScoreRequest) (ScoreResponse, error) {
	if strings.TrimSpace(request.DeclarationRef) == "" {
		return ScoreResponse{}, errors.New("declaration reference is required for scoring")
	}
	body, err := json.Marshal(request)
	if err != nil {
		return ScoreResponse{}, fmt.Errorf("encode scoring request: %w", err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, scorer.baseURL+"/v1/risk-scores", bytes.NewReader(body))
	if err != nil {
		return ScoreResponse{}, fmt.Errorf("build scoring request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	if scorer.bearer != "" {
		httpRequest.Header.Set("Authorization", "Bearer "+scorer.bearer)
	}
	response, err := scorer.client.Do(httpRequest)
	if err != nil {
		return ScoreResponse{}, fmt.Errorf("risk scorer unreachable: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return ScoreResponse{}, fmt.Errorf("risk scorer answered status %d", response.StatusCode)
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxScorerResponseSize+1))
	if err != nil {
		return ScoreResponse{}, fmt.Errorf("read risk score: %w", err)
	}
	if len(responseBody) > maxScorerResponseSize {
		return ScoreResponse{}, errors.New("risk scorer response exceeds the size bound")
	}
	var verdict ScoreResponse
	if err := json.Unmarshal(responseBody, &verdict); err != nil {
		return ScoreResponse{}, fmt.Errorf("decode risk score: %w", err)
	}
	if verdict.Score < 0 || verdict.Score > 100 {
		return ScoreResponse{}, fmt.Errorf("risk scorer returned an out-of-range score %d", verdict.Score)
	}
	if strings.TrimSpace(verdict.ModelVersion) == "" {
		return ScoreResponse{}, errors.New("risk scorer response is missing the model version")
	}
	return verdict, nil
}
