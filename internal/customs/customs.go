// Package customs cross-validates eCallUp bookings against the Nigeria
// Customs cargo declaration surface. The HTTP client is fail-closed: it only
// speaks HTTPS, authenticates with a bearer token or an mTLS client
// certificate per configuration, bounds the response body and never invents
// declaration data. The pure rule evaluation is separated from transport so
// the booking state machine can unit-test every decision boundary.
package customs

import (
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

// Decision values recorded in customs_validations and emitted on
// ports.booking.v1.
const (
	DecisionMatch    = "MATCH"
	DecisionMismatch = "MISMATCH"
)

// Reason codes for MISMATCH decisions. DECLARATION_NOT_FOUND is produced when
// the customs surface answers 404: the service was reachable, the declaration
// does not exist — a mismatch, not an outage.
const (
	ReasonDeclarationNotFound  = "DECLARATION_NOT_FOUND"
	ReasonDeclarationInvalid   = "DECLARATION_STATUS_INVALID"
	ReasonWeightTolerance      = "WEIGHT_TOLERANCE_EXCEEDED"
	ReasonConsigneeMismatch    = "CONSIGNEE_MISMATCH"
	ReasonOperatorMismatch     = "OPERATOR_MISMATCH"
	declarationStatusValid     = "VALID"
	declarationStatusReleased  = "RELEASED"
	maxDeclarationResponseSize = 1 << 20
)

// ErrDeclarationNotFound maps to a MISMATCH/DECLARATION_NOT_FOUND decision;
// every other transport or decoding error means the validator is unreachable
// and the booking must stay VALIDATION_PENDING.
var ErrDeclarationNotFound = errors.New("customs declaration does not exist")

// Declaration is the Nigeria Customs declaration surface record.
type Declaration struct {
	DeclarationRef string `json:"declaration_ref"`
	Status         string `json:"status"`
	WeightKg       int64  `json:"weight_kg"`
	ConsigneeID    string `json:"consignee_id"`
	OperatorID     string `json:"operator_id"`
}

// BookingExpectation is the booking-side declaration the customs record is
// cross-checked against.
type BookingExpectation struct {
	DeclarationRef     string
	DeclaredWeightKg   int64
	ConsigneeID        string
	OperatorID         string
	WeightToleranceBPS int64
}

// Evaluation is the outcome of the cross-validation rules.
type Evaluation struct {
	Decision        string
	ReasonCode      string
	DeclarationRef  string
	CustomsStatus   string
	CustomsWeightKg int64
	BookingWeightKg int64
	ConsigneeID     string
	OperatorID      string
}

// Evaluate applies the fail-closed rules in order: the declaration status
// must be VALID or RELEASED, the declared cargo weight must be within the
// configured basis-point tolerance of the booking declaration, and the
// consignee and operator identities must match exactly. Any violation is a
// MISMATCH with a stable reason code; only a clean pass is a MATCH.
func Evaluate(declaration Declaration, expectation BookingExpectation) Evaluation {
	evaluation := Evaluation{
		Decision:        DecisionMismatch,
		DeclarationRef:  declaration.DeclarationRef,
		CustomsStatus:   declaration.Status,
		CustomsWeightKg: declaration.WeightKg,
		BookingWeightKg: expectation.DeclaredWeightKg,
		ConsigneeID:     declaration.ConsigneeID,
		OperatorID:      declaration.OperatorID,
	}
	if declaration.Status != declarationStatusValid && declaration.Status != declarationStatusReleased {
		evaluation.ReasonCode = ReasonDeclarationInvalid
		return evaluation
	}
	if !weightWithinTolerance(declaration.WeightKg, expectation.DeclaredWeightKg, expectation.WeightToleranceBPS) {
		evaluation.ReasonCode = ReasonWeightTolerance
		return evaluation
	}
	if declaration.ConsigneeID == "" || declaration.ConsigneeID != expectation.ConsigneeID {
		evaluation.ReasonCode = ReasonConsigneeMismatch
		return evaluation
	}
	if declaration.OperatorID == "" || declaration.OperatorID != expectation.OperatorID {
		evaluation.ReasonCode = ReasonOperatorMismatch
		return evaluation
	}
	evaluation.Decision = DecisionMatch
	evaluation.ReasonCode = ""
	return evaluation
}

// weightWithinTolerance reports whether |customs - booking| / booking stays
// within toleranceBPS basis points. The boundary is inclusive: a deviation of
// exactly toleranceBPS still matches.
func weightWithinTolerance(customsKg, bookingKg, toleranceBPS int64) bool {
	if customsKg <= 0 || bookingKg <= 0 || toleranceBPS < 0 {
		return false
	}
	difference := customsKg - bookingKg
	if difference < 0 {
		difference = -difference
	}
	return difference*10000 <= bookingKg*toleranceBPS
}

// Validator fetches a cargo declaration from the Nigeria Customs declaration
// surface. Implementations must be fail-closed.
type Validator interface {
	Declaration(ctx context.Context, declarationRef string) (Declaration, error)
}

// HTTPConfig configures the declaration API client. Exactly one
// authentication mechanism may be configured: a bearer token or an mTLS
// client certificate pair.
type HTTPConfig struct {
	BaseURL        string
	BearerToken    string
	ClientCertFile string
	ClientKeyFile  string
	CACertFile     string // optional pinned CA; system pool when empty
	Timeout        time.Duration
}

// HTTPValidator is the production Validator against the declaration API.
type HTTPValidator struct {
	baseURL string
	client  *http.Client
	bearer  string
}

// NewHTTPValidator builds a fail-closed declaration client: HTTPS base URLs
// only, bounded body, explicit timeout, and exactly one configured
// authentication mechanism.
func NewHTTPValidator(config HTTPConfig) (*HTTPValidator, error) {
	parsed, err := url.Parse(config.BaseURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, errors.New("CUSTOMS_BASE_URL must be an HTTPS URL")
	}
	if config.Timeout <= 0 {
		return nil, errors.New("customs validator timeout must be positive")
	}
	bearer := config.BearerToken != ""
	mtls := config.ClientCertFile != "" || config.ClientKeyFile != ""
	if bearer == mtls {
		return nil, errors.New("configure exactly one customs authentication mechanism: bearer token or mTLS client certificate")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if config.CACertFile != "" {
		pem, err := os.ReadFile(filepath.Clean(config.CACertFile))
		if err != nil {
			return nil, fmt.Errorf("read customs CA certificate: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("customs CA certificate file contains no PEM certificates")
		}
		tlsConfig.RootCAs = pool
	}
	if mtls {
		certificate, err := tls.LoadX509KeyPair(filepath.Clean(config.ClientCertFile), filepath.Clean(config.ClientKeyFile))
		if err != nil {
			return nil, fmt.Errorf("load customs mTLS client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return &HTTPValidator{
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

// Declaration fetches one cargo declaration. A 404 is ErrDeclarationNotFound
// (a rule mismatch); any other failure means the validator is unreachable.
func (validator *HTTPValidator) Declaration(ctx context.Context, declarationRef string) (Declaration, error) {
	if strings.TrimSpace(declarationRef) == "" {
		return Declaration{}, errors.New("declaration reference is required")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		validator.baseURL+"/v1/declarations/"+url.PathEscape(declarationRef), nil)
	if err != nil {
		return Declaration{}, fmt.Errorf("build customs declaration request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if validator.bearer != "" {
		request.Header.Set("Authorization", "Bearer "+validator.bearer)
	}
	response, err := validator.client.Do(request)
	if err != nil {
		return Declaration{}, fmt.Errorf("customs declaration lookup: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return Declaration{}, ErrDeclarationNotFound
	}
	if response.StatusCode != http.StatusOK {
		return Declaration{}, fmt.Errorf("customs declaration API status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxDeclarationResponseSize+1))
	if err != nil {
		return Declaration{}, fmt.Errorf("read customs declaration: %w", err)
	}
	if len(body) > maxDeclarationResponseSize {
		return Declaration{}, errors.New("customs declaration response exceeds the size bound")
	}
	var declaration Declaration
	if err := json.Unmarshal(body, &declaration); err != nil {
		return Declaration{}, fmt.Errorf("decode customs declaration: %w", err)
	}
	if declaration.DeclarationRef != declarationRef {
		return Declaration{}, errors.New("customs declaration response ref does not match the request")
	}
	return declaration, nil
}
