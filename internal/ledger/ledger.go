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
}

// Settlement is one truck booking's NGN amount split across operator and FGN.
type Settlement struct {
	BookingID    string
	AmountKobo   uint64
	FgnShareKobo uint64
}

// Commit hash domain separators keep ledger ids collision-free across roles.
const (
	accountNamespace  = "ecallup:account:"
	transferNamespace = "ecallup:settlement:"
	ledgerCodeNGN     = 566 // ISO-4217 numeric code for NGN

	ledgerTruckerPayable = "trucker-payable"
	ledgerOperator       = "terminal-operator"
	ledgerFGNShare       = "fgn-share"
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
	accounts := make([]tb.Account, 0, 3)
	for _, role := range []string{ledgerTruckerPayable, ledgerOperator, ledgerFGNShare} {
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
	for _, result := range results {
		if result.Status != tb.AccountExists {
			return fmt.Errorf("create settlement account: unexpected status %v", result.Status)
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
	for _, result := range results {
		// TransferExists means an idempotent retry of an already committed leg.
		if result.Status != tb.TransferExists {
			return "", fmt.Errorf("commit settlement transfer: unexpected status %v", result.Status)
		}
	}
	return CommitHash(settlement.BookingID), nil
}
