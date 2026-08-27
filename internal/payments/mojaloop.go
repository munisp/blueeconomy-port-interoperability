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

type Gateway interface {
	RequestPayment(ctx context.Context, intent Intent) (Receipt, error)
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
	return &MojaloopGateway{baseURL: strings.TrimRight(baseURL, "/"), token: token, client: client}, nil
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
