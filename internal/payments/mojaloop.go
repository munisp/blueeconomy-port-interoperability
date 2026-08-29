// Package payments integrates the Mojaloop switch for NGN per-truck payment
// intents. The gateway is fail-closed: it requires an HTTPS switch endpoint and
// a bearer credential, and refuses to operate without them.
package payments

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"strings"
	"time"
)

// Intent is an idempotent NGN payment request for one truck booking.
type Intent struct {
	RequestID   string // idempotency key, also sent as transactionRequestId
	BookingID   string
	AmountKobo  int64
	Currency    string // always NGN
	PayerMSISDN string // trucker E.164 number
}

// Receipt is the switch-accepted handle for an intent.
type Receipt struct {
	TxRef      string    `json:"tx_ref"`
	AcceptedAt time.Time `json:"accepted_at"`
}

// TransferStateCommitted is the only switch transfer state that settles a
// payment; every other state fails verification closed.
const TransferStateCommitted = "COMMITTED"

// TransferStatus is the switch-side settlement state of one transaction,
// queried by tx_ref before any booking is marked paid.
type TransferStatus struct {
	TxRef      string `json:"tx_ref"`
	State      string `json:"state"`
	AmountKobo int64  `json:"amount_kobo"`
	Currency   string `json:"currency"`
	Fulfilment string `json:"fulfilment"`
}

// ErrPaymentUnverified means the switch answered but the transfer is not a
// settled payment for the expected amount (wrong state, wrong amount, wrong
// currency or a mismatched tx_ref). It is not transient: the caller must
// reject the confirmation, not retry.
var ErrPaymentUnverified = errors.New("payment is not settled at the switch for the expected amount")

type Gateway interface {
	RequestPayment(ctx context.Context, intent Intent) (Receipt, error)
	// VerifyPayment queries the switch for the settlement state of txRef and
	// fails closed: an unreachable switch or an unreadable answer is an
	// error, and any state other than a committed transfer for exactly
	// expectedAmountKobo NGN is ErrPaymentUnverified.
	VerifyPayment(ctx context.Context, txRef string, expectedAmountKobo int64) (TransferStatus, error)
}

type MojaloopGateway struct {
	baseURL string
	token   string
	client  *http.Client
}

// NewMojaloop builds a gateway against the Mojaloop transaction-requests
// service. baseURL must be HTTPS and token must be a real credential; both are
// mandatory (fail closed).
func NewMojaloop(baseURL, token string, client *http.Client) (*MojaloopGateway, error) {
	if !strings.HasPrefix(baseURL, "https://") {
		return nil, errors.New("MOJALOOP_BASE_URL must be an HTTPS switch endpoint")
	}
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("MOJALOOP_BEARER_TOKEN must be configured")
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	// otelhttp transport: FSPIOP calls become CLIENT spans with the live
	// traceparent injected (no-op when telemetry is disabled). An existing
	// client keeps its own timeout/redirect policy; only the transport is
	// wrapped when none was set, otherwise wrap its transport.
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	wrapped := *client
	wrapped.Transport = otelhttp.NewTransport(base)
	return &MojaloopGateway{baseURL: strings.TrimRight(baseURL, "/"), token: token, client: &wrapped}, nil
}

type transactionRequest struct {
	TransactionRequestID string `json:"transactionRequestId"`
	Payee                party  `json:"payee"`
	Payer                party  `json:"payer"`
	Amount               amount `json:"amount"`
	TransactionType      txType `json:"transactionType"`
}

type party struct {
	PartyIDType string `json:"partyIdType"`
	PartyID     string `json:"partyIdentifier"`
}

type amount struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type txType struct {
	Scenario   string `json:"scenario"`
	Initiator  string `json:"initiator"`
	InitiatorT string `json:"initiatorType"`
}

func (gateway *MojaloopGateway) RequestPayment(ctx context.Context, intent Intent) (Receipt, error) {
	if len(intent.RequestID) < 8 || intent.AmountKobo <= 0 || intent.Currency != "NGN" || intent.BookingID == "" {
		return Receipt{}, errors.New("payment intent is incomplete")
	}
	// NGN major units with two decimals; kobo is the minor unit.
	naira := fmt.Sprintf("%d.%02d", intent.AmountKobo/100, intent.AmountKobo%100)
	body, err := json.Marshal(transactionRequest{
		TransactionRequestID: intent.RequestID,
		Payee:                party{PartyIDType: "ALIAS", PartyID: intent.BookingID},
		Payer:                party{PartyIDType: "MSISDN", PartyID: strings.TrimPrefix(intent.PayerMSISDN, "+")},
		Amount:               amount{Amount: naira, Currency: "NGN"},
		TransactionType:      txType{Scenario: "PAYMENT", Initiator: "PAYER", InitiatorT: "CONSUMER"},
	})
	if err != nil {
		return Receipt{}, fmt.Errorf("encode transaction request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gateway.baseURL+"/transactionRequests", bytes.NewReader(body))
	if err != nil {
		return Receipt{}, err
	}
	request.Header.Set("Content-Type", "application/vnd.interoperability.transactionRequests+json;version=1.0")
	request.Header.Set("Accept", "application/vnd.interoperability.transactionRequests+json;version=1.0")
	request.Header.Set("FSPIOP-Source", "ecallup-dfsp")
	request.Header.Set("FSPIOP-Destination", "npa-switch")
	request.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	request.Header.Set("Authorization", "Bearer "+gateway.token)
	// Idempotency at the HTTP layer in addition to the transactionRequestId.
	request.Header.Set("Idempotency-Key", intent.RequestID)
	response, err := gateway.client.Do(request)
	if err != nil {
		return Receipt{}, fmt.Errorf("mojaloop transaction request: %w", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return Receipt{}, fmt.Errorf("read mojaloop response: %w", err)
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted {
		return Receipt{}, fmt.Errorf("mojaloop rejected payment intent: status %d", response.StatusCode)
	}
	var decoded struct {
		TransactionRequestID string `json:"transactionRequestId"`
	}
	if len(responseBody) > 0 {
		if err := json.Unmarshal(responseBody, &decoded); err != nil {
			return Receipt{}, fmt.Errorf("decode mojaloop response: %w", err)
		}
	}
	txRef := decoded.TransactionRequestID
	if txRef == "" {
		txRef = intent.RequestID
	}
	if txRef != intent.RequestID {
		return Receipt{}, errors.New("mojaloop returned a mismatched transaction reference")
	}
	return Receipt{TxRef: txRef, AcceptedAt: time.Now().UTC()}, nil
}

// parseNairaMinor converts an NGN major-unit decimal string ("123.45") into
// integer kobo. Anything that is not a well-formed non-negative NGN amount is
// rejected — money never passes through floats.
func parseNairaMinor(value string) (int64, error) {
	if value == "" || len(value) > 20 {
		return 0, errors.New("amount is missing or too long")
	}
	naira, kobo, _ := strings.Cut(value, ".")
	if naira == "" {
		return 0, errors.New("amount is malformed")
	}
	for _, digit := range naira {
		if digit < '0' || digit > '9' {
			return 0, errors.New("amount contains non-numeric characters")
		}
	}
	var whole, fraction int64
	if _, err := fmt.Sscanf(naira, "%d", &whole); err != nil {
		return 0, errors.New("amount is malformed")
	}
	switch len(kobo) {
	case 0:
	case 1:
		fraction = int64(kobo[0]-'0') * 10
	case 2:
		fraction = int64(kobo[0]-'0')*10 + int64(kobo[1]-'0')
	default:
		return 0, errors.New("amount carries sub-kobo precision")
	}
	if kobo != "" {
		for _, digit := range kobo {
			if digit < '0' || digit > '9' {
				return 0, errors.New("amount contains non-numeric characters")
			}
		}
	}
	return whole*100 + fraction, nil
}

// VerifyPayment queries the switch transfer resource for txRef and fails
// closed unless the transfer is COMMITTED for exactly expectedAmountKobo NGN
// with a fulfilment present. A switch outage, non-200 status, undecodable or
// self-contradicting answer is always an error — never a verified payment.
func (gateway *MojaloopGateway) VerifyPayment(ctx context.Context, txRef string, expectedAmountKobo int64) (TransferStatus, error) {
	if txRef == "" || len(txRef) > 128 || expectedAmountKobo <= 0 {
		return TransferStatus{}, ErrPaymentUnverified
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, gateway.baseURL+"/transfers/"+txRef, nil)
	if err != nil {
		return TransferStatus{}, err
	}
	request.Header.Set("Accept", "application/vnd.interoperability.transfers+json;version=1.0")
	request.Header.Set("FSPIOP-Source", "ecallup-dfsp")
	request.Header.Set("FSPIOP-Destination", "npa-switch")
	request.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	request.Header.Set("Authorization", "Bearer "+gateway.token)
	response, err := gateway.client.Do(request)
	if err != nil {
		return TransferStatus{}, fmt.Errorf("mojaloop transfer status query: %w", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return TransferStatus{}, fmt.Errorf("read mojaloop transfer status: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return TransferStatus{}, fmt.Errorf("mojaloop transfer status query failed: status %d", response.StatusCode)
	}
	var decoded struct {
		TransferID    string `json:"transferId"`
		TransferState string `json:"transferState"`
		Fulfilment    string `json:"fulfilment"`
		Amount        *struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"amount"`
	}
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return TransferStatus{}, fmt.Errorf("decode mojaloop transfer status: %w", err)
	}
	if decoded.TransferState != TransferStateCommitted || strings.TrimSpace(decoded.Fulfilment) == "" {
		return TransferStatus{}, ErrPaymentUnverified
	}
	if decoded.TransferID != "" && decoded.TransferID != txRef {
		return TransferStatus{}, ErrPaymentUnverified
	}
	if decoded.Amount == nil {
		return TransferStatus{}, fmt.Errorf("mojaloop transfer status carries no amount; refusing to verify")
	}
	amountKobo, err := parseNairaMinor(decoded.Amount.Amount)
	if err != nil || decoded.Amount.Currency != "NGN" {
		return TransferStatus{}, fmt.Errorf("mojaloop transfer amount is unreadable; refusing to verify")
	}
	if amountKobo != expectedAmountKobo {
		return TransferStatus{}, ErrPaymentUnverified
	}
	return TransferStatus{
		TxRef:      txRef,
		State:      decoded.TransferState,
		AmountKobo: amountKobo,
		Currency:   decoded.Amount.Currency,
		Fulfilment: decoded.Fulfilment,
	}, nil
}
