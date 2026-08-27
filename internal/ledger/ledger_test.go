package ledger

import (
	"testing"
)

func TestDeterministicIdentifiers(t *testing.T) {
	first := TransferID("booking-0001", "operator")
	second := TransferID("booking-0001", "operator")
	if first != second {
		t.Fatal("transfer ids must be deterministic for idempotent retries")
	}
	if TransferID("booking-0001", "operator") == TransferID("booking-0001", "fgn") {
		t.Fatal("settlement legs must have distinct transfer ids")
	}
	if TransferID("booking-0001", "operator") == TransferID("booking-0002", "operator") {
		t.Fatal("different bookings must have distinct transfer ids")
	}
	if AccountID("trucker-payable") == AccountID("terminal-operator") {
		t.Fatal("ledger roles must have distinct account ids")
	}
	if CommitHash("booking-0001") != CommitHash("booking-0001") {
		t.Fatal("commit hash must be deterministic")
	}
	if len(CommitHash("booking-0001")) != len("sha256:")+64 {
		t.Fatal("commit hash must be a sha256 digest")
	}
}

func TestNewTigerBeetleFailsClosedWithoutConfiguration(t *testing.T) {
	if _, err := NewTigerBeetle("", []string{"127.0.0.1:3000"}); err == nil {
		t.Fatal("missing cluster id must fail closed")
	}
	if _, err := NewTigerBeetle("not-a-number", []string{"127.0.0.1:3000"}); err == nil {
		t.Fatal("non-numeric cluster id must fail closed")
	}
	if _, err := NewTigerBeetle("0", nil); err == nil {
		t.Fatal("missing replica addresses must fail closed")
	}
	if _, err := NewTigerBeetle("0", []string{"no-port"}); err == nil {
		t.Fatal("malformed replica address must fail closed")
	}
}

func TestCommitRejectsInvalidSettlements(t *testing.T) {
	// Settlement validation happens before any cluster interaction, so these
	// checks hold without a running TigerBeetle.
	ledgerClient, err := NewTigerBeetle("0", []string{"127.0.0.1:1"})
	if err != nil {
		t.Fatalf("client construction: %v", err)
	}
	defer ledgerClient.Close()
	for _, settlement := range []Settlement{
		{BookingID: "", AmountKobo: 250000, FgnShareKobo: 6250},
		{BookingID: "booking-1", AmountKobo: 0, FgnShareKobo: 0},
		{BookingID: "booking-1", AmountKobo: 250000, FgnShareKobo: 0},
		{BookingID: "booking-1", AmountKobo: 250000, FgnShareKobo: 250000},
	} {
		if _, err := ledgerClient.CommitBookingSettlement(t.Context(), settlement); err == nil {
			t.Fatalf("invalid settlement %#v must be rejected", settlement)
		}
	}
}
