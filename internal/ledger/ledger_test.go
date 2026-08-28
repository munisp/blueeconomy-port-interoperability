package ledger

import (
	"errors"
	"testing"

	tb "github.com/tigerbeetle/tigerbeetle-go"
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

// fakeClient scripts TigerBeetle 0.17.x dense results without a live cluster.
type fakeClient struct {
	accountStatuses  []tb.CreateAccountStatus
	transferStatuses []tb.CreateTransferStatus
	accountErr       error
	transferErr      error
	accountsCalls    int
	transfersCalls   int
	sawTransfers     []tb.Transfer
}

func (fake *fakeClient) CreateAccounts(accounts []tb.Account) ([]tb.CreateAccountResult, error) {
	fake.accountsCalls++
	if fake.accountErr != nil {
		return nil, fake.accountErr
	}
	results := make([]tb.CreateAccountResult, len(fake.accountStatuses))
	for i, status := range fake.accountStatuses {
		results[i] = tb.CreateAccountResult{Status: status}
	}
	return results, nil
}

func (fake *fakeClient) CreateTransfers(transfers []tb.Transfer) ([]tb.CreateTransferResult, error) {
	fake.transfersCalls++
	fake.sawTransfers = append(fake.sawTransfers, transfers...)
	if fake.transferErr != nil {
		return nil, fake.transferErr
	}
	results := make([]tb.CreateTransferResult, len(fake.transferStatuses))
	for i, status := range fake.transferStatuses {
		results[i] = tb.CreateTransferResult{Status: status}
	}
	return results, nil
}

func (fake *fakeClient) LookupAccounts([]tb.Uint128) ([]tb.Account, error) {
	return nil, errors.New("fake: unimplemented")
}
func (fake *fakeClient) LookupTransfers([]tb.Uint128) ([]tb.Transfer, error) {
	return nil, errors.New("fake: unimplemented")
}
func (fake *fakeClient) GetAccountTransfers(tb.AccountFilter) ([]tb.Transfer, error) {
	return nil, errors.New("fake: unimplemented")
}
func (fake *fakeClient) GetAccountBalances(tb.AccountFilter) ([]tb.AccountBalance, error) {
	return nil, errors.New("fake: unimplemented")
}
func (fake *fakeClient) QueryAccounts(tb.QueryFilter) ([]tb.Account, error) {
	return nil, errors.New("fake: unimplemented")
}
func (fake *fakeClient) QueryTransfers(tb.QueryFilter) ([]tb.Transfer, error) {
	return nil, errors.New("fake: unimplemented")
}
func (fake *fakeClient) GetChangeEvents(tb.ChangeEventsFilter) ([]tb.ChangeEvent, error) {
	return nil, errors.New("fake: unimplemented")
}
func (fake *fakeClient) Nop() error { return nil }
func (fake *fakeClient) Close()     {}

func settlementFixture() Settlement {
	return Settlement{BookingID: "booking-0001", AmountKobo: 250000, FgnShareKobo: 6250}
}

func TestCommitAcceptsCreatedOnFirstCommit(t *testing.T) {
	fake := &fakeClient{
		accountStatuses:  []tb.CreateAccountStatus{tb.AccountCreated, tb.AccountCreated, tb.AccountCreated},
		transferStatuses: []tb.CreateTransferStatus{tb.TransferCreated, tb.TransferCreated},
	}
	ledger := &TigerBeetleLedger{client: fake}
	hash, err := ledger.CommitBookingSettlement(t.Context(), settlementFixture())
	if err != nil {
		t.Fatalf("first-time commit with Created dense results must succeed: %v", err)
	}
	if hash != CommitHash("booking-0001") {
		t.Fatalf("commit hash mismatch: %q", hash)
	}
	if fake.accountsCalls != 1 || fake.transfersCalls != 1 {
		t.Fatalf("expected one account and one transfer call, got %d/%d", fake.accountsCalls, fake.transfersCalls)
	}
	if len(fake.sawTransfers) != 2 {
		t.Fatalf("expected two settlement legs, got %d", len(fake.sawTransfers))
	}
	operator := fake.sawTransfers[0]
	fgn := fake.sawTransfers[1]
	if operator.ID != TransferID("booking-0001", "operator") || fgn.ID != TransferID("booking-0001", "fgn") {
		t.Fatal("settlement legs must use deterministic transfer ids")
	}
	if operator.Amount != tb.ToUint128(250000-6250) || fgn.Amount != tb.ToUint128(6250) {
		t.Fatal("operator/FGN split must sum to the booking amount")
	}
}

func TestCommitAcceptsExistsOnIdempotentRetry(t *testing.T) {
	fake := &fakeClient{
		accountStatuses:  []tb.CreateAccountStatus{tb.AccountExists, tb.AccountExists, tb.AccountExists},
		transferStatuses: []tb.CreateTransferStatus{tb.TransferExists, tb.TransferExists},
	}
	ledger := &TigerBeetleLedger{client: fake}
	if _, err := ledger.CommitBookingSettlement(t.Context(), settlementFixture()); err != nil {
		t.Fatalf("idempotent retry with Exists dense results must succeed: %v", err)
	}
}

func TestCommitRejectsGenuineTransferConflict(t *testing.T) {
	// ExistsWithDifferent* means the deterministic id was reused with
	// different stored fields — a real conflict, never an idempotent retry.
	for _, status := range []tb.CreateTransferStatus{
		tb.TransferExistsWithDifferentAmount,
		tb.TransferExistsWithDifferentDebitAccountID,
		tb.TransferExistsWithDifferentCreditAccountID,
		tb.TransferExistsWithDifferentLedger,
		tb.TransferExceedsCredits,
	} {
		fake := &fakeClient{
			accountStatuses:  []tb.CreateAccountStatus{tb.AccountExists, tb.AccountExists, tb.AccountExists},
			transferStatuses: []tb.CreateTransferStatus{tb.TransferCreated, status},
		}
		ledger := &TigerBeetleLedger{client: fake}
		if _, err := ledger.CommitBookingSettlement(t.Context(), settlementFixture()); err == nil {
			t.Fatalf("status %v must fail the commit", status)
		}
	}
}

func TestCommitRejectsGenuineAccountConflict(t *testing.T) {
	for _, status := range []tb.CreateAccountStatus{
		tb.AccountExistsWithDifferentLedger,
		tb.AccountExistsWithDifferentCode,
		tb.AccountExistsWithDifferentFlags,
	} {
		fake := &fakeClient{
			accountStatuses:  []tb.CreateAccountStatus{tb.AccountCreated, tb.AccountCreated, status},
			transferStatuses: []tb.CreateTransferStatus{tb.TransferCreated, tb.TransferCreated},
		}
		ledger := &TigerBeetleLedger{client: fake}
		if _, err := ledger.CommitBookingSettlement(t.Context(), settlementFixture()); err == nil {
			t.Fatalf("status %v must fail the commit", status)
		}
		if fake.transfersCalls != 0 {
			t.Fatalf("status %v must abort before any transfer posts", status)
		}
	}
}

func TestCommitRejectsNonDenseResults(t *testing.T) {
	// A truncated (pre-0.17 sparse-style) result array must fail closed rather
	// than silently treat missing statuses as success.
	fake := &fakeClient{
		accountStatuses:  []tb.CreateAccountStatus{tb.AccountCreated, tb.AccountCreated, tb.AccountCreated},
		transferStatuses: []tb.CreateTransferStatus{tb.TransferCreated},
	}
	ledger := &TigerBeetleLedger{client: fake}
	if _, err := ledger.CommitBookingSettlement(t.Context(), settlementFixture()); err == nil {
		t.Fatal("a dense-result count mismatch must fail the commit")
	}
}

func TestCommitPropagatesClientErrors(t *testing.T) {
	fake := &fakeClient{
		accountStatuses: []tb.CreateAccountStatus{tb.AccountCreated, tb.AccountCreated, tb.AccountCreated},
		transferErr:     errors.New("cluster unavailable"),
	}
	ledger := &TigerBeetleLedger{client: fake}
	if _, err := ledger.CommitBookingSettlement(t.Context(), settlementFixture()); err == nil {
		t.Fatal("client errors must propagate")
	}
}
