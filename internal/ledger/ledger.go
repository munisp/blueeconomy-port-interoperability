// Package ledger commits per-truck booking settlements to TigerBeetle as
// double-entry transfers: the trucker's payable is split between the terminal
// operator and the FGN share. All identifiers are deterministic functions of
// the booking id, so retries are idempotent at the ledger. The package is
// fail-closed: without a configured cluster it refuses to operate.
package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	tb "github.com/tigerbeetle/tigerbeetle-go"
)

// Ledger is the settlement boundary used by the booking workflow.
type Ledger interface {
	CommitBookingSettlement(ctx context.Context, settlement Settlement) (string, error)
	Refunder
}

// Refunder is the refund rail: paid bookings that expire or are cancelled
// before settlement is consumed get a compensating transfer back to the
// trucker clearing account. Wired into stores that must never strand money.
type Refunder interface {
	RefundBookingSettlement(ctx context.Context, refund Refund) (string, error)
}

// Settlement is one truck booking's NGN amount split across operator and FGN.
type Settlement struct {
	BookingID    string
	AmountKobo   uint64
	FgnShareKobo uint64
}

// Refund is the compensating mirror of a Settlement: operator and FGN shares
// flow back to the trucker clearing account. Amounts are integer minor units
// and must reproduce the original split exactly.
type Refund struct {
	BookingID    string
	AmountKobo   uint64
	FgnShareKobo uint64
}

// Commit hash domain separators keep ledger ids collision-free across roles.
const (
	accountNamespace  = "ecallup:account:"
	transferNamespace = "ecallup:settlement:"
	refundNamespace   = "ecallup:refund:"
	ledgerCodeNGN     = 566 // ISO-4217 numeric code for NGN

	ledgerTruckerPayable  = "trucker-payable"
	ledgerOperator        = "terminal-operator"
	ledgerFGNShare        = "fgn-share"
	ledgerTruckerClearing = "trucker-clearing"
)

// AccountID deterministically derives the TigerBeetle account id for a role.
func AccountID(role string) tb.Uint128 {
	sum := sha256.Sum256([]byte(accountNamespace + role))
	var id [16]byte
	copy(id[:], sum[:16])
	return tb.BytesToUint128(id)
}

// TransferID deterministically derives the idempotent transfer id for one
// settlement leg; repeating a commit yields the same id and is deduplicated.
func TransferID(bookingID, leg string) tb.Uint128 {
	sum := sha256.Sum256([]byte(transferNamespace + bookingID + ":" + leg))
	var id [16]byte
	copy(id[:], sum[:16])
	return tb.BytesToUint128(id)
}

// CommitHash is the audit-facing hash of both settlement transfer ids.
func CommitHash(bookingID string) string {
	sum := sha256.Sum256([]byte(transferNamespace + bookingID))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// RefundTransferID deterministically derives the idempotent transfer id for
// one refund leg; a refund retry after a crash reuses the id and TigerBeetle
// deduplicates it.
func RefundTransferID(bookingID, leg string) tb.Uint128 {
	sum := sha256.Sum256([]byte(refundNamespace + bookingID + ":" + leg))
	var id [16]byte
	copy(id[:], sum[:16])
	return tb.BytesToUint128(id)
}

// RefundCommitHash is the audit-facing hash of both refund transfer ids.
func RefundCommitHash(bookingID string) string {
	sum := sha256.Sum256([]byte(refundNamespace + bookingID))
	return "sha256:" + hex.EncodeToString(sum[:])
}

type TigerBeetleLedger struct {
	client tb.Client
}

// NewTigerBeetle connects to a TigerBeetle cluster. clusterID is parsed from
// decimal text; addresses must be non-empty host:port pairs. Both are
// mandatory — there is no default cluster to fall back to.
func NewTigerBeetle(clusterID string, addresses []string) (*TigerBeetleLedger, error) {
	parsed, err := strconv.ParseUint(strings.TrimSpace(clusterID), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("TIGERBEETLE_CLUSTER_ID must be a decimal cluster id: %w", err)
	}
	if len(addresses) == 0 {
		return nil, errors.New("TIGERBEETLE_ADDRESSES must list at least one replica")
	}
	for _, address := range addresses {
		if strings.TrimSpace(address) == "" || !strings.Contains(address, ":") {
			return nil, fmt.Errorf("TIGERBEETLE_ADDRESSES entry %q is not host:port", address)
		}
	}
	client, err := tb.NewClient(tb.ToUint128(parsed), addresses)
	if err != nil {
		return nil, fmt.Errorf("connect tigerbeetle: %w", err)
	}
	return &TigerBeetleLedger{client: client}, nil
}

func (ledger *TigerBeetleLedger) Close() {
	ledger.client.Close()
}

func (ledger *TigerBeetleLedger) ensureAccounts() error {
	accounts := make([]tb.Account, 0, 4)
	for _, role := range []string{ledgerTruckerPayable, ledgerOperator, ledgerFGNShare, ledgerTruckerClearing} {
		accounts = append(accounts, tb.Account{
			ID:     AccountID(role),
			Ledger: 1,
			Code:   ledgerCodeNGN,
		})
	}
	results, err := ledger.client.CreateAccounts(accounts)
	if err != nil {
		return fmt.Errorf("create settlement accounts: %w", err)
	}
	// TigerBeetle 0.17.x dense results: one result per account, in input
	// order. AccountCreated is the first-time success status; AccountExists is
	// an idempotent retry whose stored fields match exactly. Any other status
	// (AccountExistsWithDifferent*, validation failures) is a real error.
	if len(results) != len(accounts) {
		return fmt.Errorf("create settlement accounts: expected %d dense results, got %d", len(accounts), len(results))
	}
	for i, result := range results {
		switch result.Status {
		case tb.AccountCreated, tb.AccountExists:
		default:
			return fmt.Errorf("create settlement account %d: unexpected status %v", i, result.Status)
		}
	}
	return nil
}

// CommitBookingSettlement posts two double-entry transfers per booking:
// trucker-payable -> terminal-operator (operator share) and
// trucker-payable -> FGN share. Transfer ids are deterministic, so a retry
// after a crash reuses the same ids and TigerBeetle deduplicates them.
func (ledger *TigerBeetleLedger) CommitBookingSettlement(_ context.Context, settlement Settlement) (string, error) {
	if settlement.BookingID == "" || settlement.AmountKobo == 0 {
		return "", errors.New("settlement booking id and amount are required")
	}
	if settlement.FgnShareKobo == 0 || settlement.FgnShareKobo >= settlement.AmountKobo {
		return "", errors.New("FGN share must be positive and below the total amount")
	}
	if err := ledger.ensureAccounts(); err != nil {
		return "", err
	}
	operatorShare := settlement.AmountKobo - settlement.FgnShareKobo
	transfers := []tb.Transfer{
		{
			ID:              TransferID(settlement.BookingID, "operator"),
			DebitAccountID:  AccountID(ledgerTruckerPayable),
			CreditAccountID: AccountID(ledgerOperator),
			Amount:          tb.ToUint128(operatorShare),
			Ledger:          1,
			Code:            ledgerCodeNGN,
		},
		{
			ID:              TransferID(settlement.BookingID, "fgn"),
			DebitAccountID:  AccountID(ledgerTruckerPayable),
			CreditAccountID: AccountID(ledgerFGNShare),
			Amount:          tb.ToUint128(settlement.FgnShareKobo),
			Ledger:          1,
			Code:            ledgerCodeNGN,
		},
	}
	results, err := ledger.client.CreateTransfers(transfers)
	if err != nil {
		return "", fmt.Errorf("commit settlement transfers: %w", err)
	}
	// TigerBeetle 0.17.x dense results: one result per transfer, in input
	// order. TransferCreated is the first-time success status; TransferExists
	// is an idempotent retry whose stored fields match exactly. Any other
	// status (TransferExistsWithDifferent*, validation failures) is a genuine
	// conflict and must fail the commit.
	if len(results) != len(transfers) {
		return "", fmt.Errorf("commit settlement transfers: expected %d dense results, got %d", len(transfers), len(results))
	}
	for i, result := range results {
		switch result.Status {
		case tb.TransferCreated, tb.TransferExists:
		default:
			return "", fmt.Errorf("commit settlement transfer %d: unexpected status %v", i, result.Status)
		}
	}
	return CommitHash(settlement.BookingID), nil
}

// RefundBookingSettlement posts the compensating mirror of a settlement: the
// operator share flows terminal-operator -> trucker-clearing and the FGN
// share flows fgn-share -> trucker-clearing, so a paid booking that expires
// or is cancelled never strands the trucker's money. Transfer ids are
// deterministic per booking, so refund retries are idempotent at the ledger.
// A ledger outage fails closed: the caller must not transition the booking.
func (ledger *TigerBeetleLedger) RefundBookingSettlement(_ context.Context, refund Refund) (string, error) {
	if refund.BookingID == "" || refund.AmountKobo == 0 {
		return "", errors.New("refund booking id and amount are required")
	}
	if refund.FgnShareKobo == 0 || refund.FgnShareKobo >= refund.AmountKobo {
		return "", errors.New("refund FGN share must be positive and below the total amount")
	}
	if err := ledger.ensureAccounts(); err != nil {
		return "", err
	}
	operatorShare := refund.AmountKobo - refund.FgnShareKobo
	transfers := []tb.Transfer{
		{
			ID:              RefundTransferID(refund.BookingID, "operator"),
			DebitAccountID:  AccountID(ledgerOperator),
			CreditAccountID: AccountID(ledgerTruckerClearing),
			Amount:          tb.ToUint128(operatorShare),
			Ledger:          1,
			Code:            ledgerCodeNGN,
		},
		{
			ID:              RefundTransferID(refund.BookingID, "fgn"),
			DebitAccountID:  AccountID(ledgerFGNShare),
			CreditAccountID: AccountID(ledgerTruckerClearing),
			Amount:          tb.ToUint128(refund.FgnShareKobo),
			Ledger:          1,
			Code:            ledgerCodeNGN,
		},
	}
	results, err := ledger.client.CreateTransfers(transfers)
	if err != nil {
		return "", fmt.Errorf("commit refund transfers: %w", err)
	}
	if len(results) != len(transfers) {
		return "", fmt.Errorf("commit refund transfers: expected %d dense results, got %d", len(transfers), len(results))
	}
	for i, result := range results {
		switch result.Status {
		case tb.TransferCreated, tb.TransferExists:
		default:
			return "", fmt.Errorf("commit refund transfer %d: unexpected status %v", i, result.Status)
		}
	}
	return RefundCommitHash(refund.BookingID), nil
}
