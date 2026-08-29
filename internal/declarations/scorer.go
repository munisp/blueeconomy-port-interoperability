package declarations

import (
	"context"
)

// Scorer is the pluggable risk-scoring boundary. Implementations must be
// fail-closed: any transport failure or invalid response is an error and the
// declaration is parked in the terminal SCORING_UNAVAILABLE state. There is
// no hash, heuristic or LLM fallback anywhere in this package.
//
// Phase-7 (PRA-066..068, PRA-126): the production implementation is
// GRPCScorer against blueeconomy.riskscore.v1.RiskScoreService with
// Keycloak client_credentials authentication, bounded retries and a circuit
// breaker. The former HTTP client and its static
// DECLARATIONS_SCORER_BEARER_TOKEN credential are retired.
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
