package payments

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// transferServer stubs the switch transfer resource for verification tests.
func transferServer(t *testing.T, status int, body string) (*MojaloopGateway, func()) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/transfers/pay-req-0001" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.interoperability.transfers+json;version=1.0")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	gateway, err := NewMojaloop(server.URL, "test-token", server.Client())
	if err != nil {
		server.Close()
		t.Fatalf("build gateway: %v", err)
	}
	return gateway, server.Close
}

func TestVerifyPaymentAcceptsCommittedTransferForExactAmount(t *testing.T) {
	gateway, closeServer := transferServer(t, http.StatusOK, `{
		"transferId": "pay-req-0001",
		"transferState": "COMMITTED",
		"fulfilment": "WLctttbu2HvTsa1XWvUoGRcQozHsqeu9Ahl2JW9Bsu8",
		"completedTimestamp": "2026-01-01T00:00:00Z",
		"amount": {"amount": "2500.00", "currency": "NGN"}
	}`)
	defer closeServer()
	status, err := gateway.VerifyPayment(context.Background(), "pay-req-0001", 250000)
	if err != nil {
		t.Fatalf("verify committed transfer: %v", err)
	}
	if status.State != TransferStateCommitted || status.AmountKobo != 250000 || status.Currency != "NGN" || status.Fulfilment == "" {
		t.Fatalf("status = %#v", status)
	}
}

func TestVerifyPaymentRejectsNonCommittedStates(t *testing.T) {
	for _, state := range []string{"RECEIVED", "RESERVED", "ABORTED"} {
		gateway, closeServer := transferServer(t, http.StatusOK, `{
			"transferId": "pay-req-0001",
			"transferState": "`+state+`",
			"amount": {"amount": "2500.00", "currency": "NGN"}
		}`)
		_, err := gateway.VerifyPayment(context.Background(), "pay-req-0001", 250000)
		closeServer()
		if !errors.Is(err, ErrPaymentUnverified) {
			t.Fatalf("state %s: err = %v, want ErrPaymentUnverified", state, err)
		}
	}
}

func TestVerifyPaymentRejectsAmountMismatch(t *testing.T) {
	gateway, closeServer := transferServer(t, http.StatusOK, `{
		"transferId": "pay-req-0001",
		"transferState": "COMMITTED",
		"fulfilment": "WLctttbu2HvTsa1XWvUoGRcQozHsqeu9Ahl2JW9Bsu8",
		"amount": {"amount": "2499.99", "currency": "NGN"}
	}`)
	defer closeServer()
	if _, err := gateway.VerifyPayment(context.Background(), "pay-req-0001", 250000); !errors.Is(err, ErrPaymentUnverified) {
		t.Fatalf("amount mismatch: err = %v, want ErrPaymentUnverified", err)
	}
}

func TestVerifyPaymentFailsClosedOnSwitchOutage(t *testing.T) {
	// Non-200 from the switch: transient-looking, but never a verification.
	gateway, closeServer := transferServer(t, http.StatusBadGateway, `{}`)
	defer closeServer()
	if _, err := gateway.VerifyPayment(context.Background(), "pay-req-0001", 250000); err == nil || errors.Is(err, ErrPaymentUnverified) {
		t.Fatalf("switch 502: err = %v, want a transient (retryable) error, not ErrPaymentUnverified", err)
	}
}

func TestVerifyPaymentFailsClosedWithoutAmountOrFulfilment(t *testing.T) {
	// A COMMITTED claim without an amount cannot be checked against the
	// intent: fail closed with a transient error rather than trusting it.
	gateway, closeServer := transferServer(t, http.StatusOK, `{
		"transferId": "pay-req-0001",
		"transferState": "COMMITTED",
		"fulfilment": "WLctttbu2HvTsa1XWvUoGRcQozHsqeu9Ahl2JW9Bsu8"
	}`)
	defer closeServer()
	if _, err := gateway.VerifyPayment(context.Background(), "pay-req-0001", 250000); err == nil {
		t.Fatal("amount-less COMMITTED answer must not verify")
	}

	gateway2, closeServer2 := transferServer(t, http.StatusOK, `{
		"transferId": "pay-req-0001",
		"transferState": "COMMITTED",
		"amount": {"amount": "2500.00", "currency": "NGN"}
	}`)
	defer closeServer2()
	if _, err := gateway2.VerifyPayment(context.Background(), "pay-req-0001", 250000); !errors.Is(err, ErrPaymentUnverified) {
		t.Fatalf("fulfilment-less COMMITTED answer: err = %v, want ErrPaymentUnverified", err)
	}
}

func TestVerifyPaymentRejectsContradictoryTransferID(t *testing.T) {
	gateway, closeServer := transferServer(t, http.StatusOK, `{
		"transferId": "someone-elses-transfer",
		"transferState": "COMMITTED",
		"fulfilment": "WLctttbu2HvTsa1XWvUoGRcQozHsqeu9Ahl2JW9Bsu8",
		"amount": {"amount": "2500.00", "currency": "NGN"}
	}`)
	defer closeServer()
	if _, err := gateway.VerifyPayment(context.Background(), "pay-req-0001", 250000); !errors.Is(err, ErrPaymentUnverified) {
		t.Fatalf("contradictory transfer id: err = %v, want ErrPaymentUnverified", err)
	}
}

func TestParseNairaMinorIsExactIntegerMoney(t *testing.T) {
	cases := map[string]int64{"2500.00": 250000, "0.01": 1, "0.10": 10, "7": 700, "1.5": 150}
	for input, want := range cases {
		got, err := parseNairaMinor(input)
		if err != nil || got != want {
			t.Fatalf("parseNairaMinor(%q) = %d, %v; want %d", input, got, err, want)
		}
	}
	for _, invalid := range []string{"", "abc", "1.234", "1,00", "-5.00", "1.0.0", " 1.00"} {
		if _, err := parseNairaMinor(invalid); err == nil {
			t.Fatalf("parseNairaMinor(%q) must be rejected", invalid)
		}
	}
}
