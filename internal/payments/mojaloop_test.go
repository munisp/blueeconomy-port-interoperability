package payments

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testIntent() Intent {
	return Intent{
		RequestID:   "pay-req-0001",
		BookingID:   "booking-0001",
		AmountKobo:  250000,
		Currency:    "NGN",
		PayerMSISDN: "+2348012345678",
	}
}

func TestNewMojaloopFailsClosedWithoutHTTPSAndToken(t *testing.T) {
	if _, err := NewMojaloop("http://switch.example", "token", nil); err == nil {
		t.Fatal("plaintext switch endpoint must be rejected")
	}
	if _, err := NewMojaloop("https://switch.example", "", nil); err == nil {
		t.Fatal("missing bearer token must be rejected")
	}
	if _, err := NewMojaloop("https://switch.example", "token", nil); err != nil {
		t.Fatalf("valid configuration rejected: %v", err)
	}
}

func TestRequestPaymentPostsFSPIOPTransactionRequest(t *testing.T) {
	var capturedAuth, capturedIdempotency string
	var capturedBody transactionRequest
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/transactionRequests" || r.Method != http.MethodPost {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		capturedAuth = r.Header.Get("Authorization")
		capturedIdempotency = r.Header.Get("Idempotency-Key")
		if r.Header.Get("FSPIOP-Source") == "" {
			http.Error(w, "missing FSPIOP-Source", http.StatusBadRequest)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&capturedBody); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.interoperability.transactionRequests+json;version=1.0")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"transactionRequestId": capturedBody.TransactionRequestID})
	}))
	defer server.Close()
	gateway, err := NewMojaloop(server.URL, "test-token", server.Client())
	if err != nil {
		t.Fatalf("build gateway: %v", err)
	}
	receipt, err := gateway.RequestPayment(context.Background(), testIntent())
	if err != nil {
		t.Fatalf("request payment: %v", err)
	}
	if receipt.TxRef != "pay-req-0001" {
		t.Fatalf("tx ref = %q, want idempotent request id echoed", receipt.TxRef)
	}
	if capturedAuth != "Bearer test-token" || capturedIdempotency != "pay-req-0001" {
		t.Fatalf("auth/idempotency headers = %q / %q", capturedAuth, capturedIdempotency)
	}
	if capturedBody.Amount.Amount != "2500.00" || capturedBody.Amount.Currency != "NGN" {
		t.Fatalf("amount = %#v, want 2500.00 NGN", capturedBody.Amount)
	}
	if capturedBody.Payer.PartyIDType != "MSISDN" || !strings.HasSuffix(capturedBody.Payer.PartyID, "2348012345678") {
		t.Fatalf("payer = %#v", capturedBody.Payer)
	}
}

func TestRequestPaymentRejectsMismatchedReference(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"transactionRequestId": "different-ref"})
	}))
	defer server.Close()
	gateway, _ := NewMojaloop(server.URL, "test-token", server.Client())
	if _, err := gateway.RequestPayment(context.Background(), testIntent()); err == nil {
		t.Fatal("mismatched transaction reference must be rejected")
	}
}

func TestRequestPaymentSurfacesSwitchRejection(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "rejected", http.StatusBadRequest)
	}))
	defer server.Close()
	gateway, _ := NewMojaloop(server.URL, "test-token", server.Client())
	if _, err := gateway.RequestPayment(context.Background(), testIntent()); err == nil {
		t.Fatal("switch rejection must surface as an error")
	}
}

func TestRequestPaymentValidatesIntent(t *testing.T) {
	gateway, err := NewMojaloop("https://switch.example", "token", nil)
	if err != nil {
		t.Fatalf("build gateway: %v", err)
	}
	if _, err := gateway.RequestPayment(context.Background(), Intent{}); err == nil {
		t.Fatal("incomplete intent must be rejected")
	}
}
