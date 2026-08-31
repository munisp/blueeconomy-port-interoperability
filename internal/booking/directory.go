package booking

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// SlotInfo is a slot joined with its terminal's booking fee.
type SlotInfo struct {
	Slot
	BookingFeeKobo int64
}

// GetSlot loads one slot with the terminal fee used to price USSD bookings.
func (store *Store) GetSlot(ctx context.Context, slotID string) (SlotInfo, error) {
	var info SlotInfo
	err := store.withTx(ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		err := tx.QueryRow(ctx, `
			SELECT s.slot_id, s.terminal_id, s.starts_at, s.ends_at, s.capacity, s.created_at,
				t.port_code, t.booking_fee_kobo
			FROM terminal_slots s
			JOIN port_terminals t ON t.terminal_id = s.terminal_id
			WHERE s.slot_id = $1`, slotID).
			Scan(&info.SlotID, &info.TerminalID, &info.StartsAt, &info.EndsAt, &info.Capacity, &info.CreatedAt, &info.PortCode, &info.BookingFeeKobo)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("get slot: %w", err)
		}
		return nil
	})
	return info, err
}

// Directory adapts the booking store to the USSD gateway's narrow interface.
type Directory struct {
	store     *Store
	principal Principal
}

func NewDirectory(store *Store, principal Principal) (*Directory, error) {
	if store == nil || principal.ID == "" || principal.Role == "" {
		return nil, errors.New("booking directory requires a store and a principal")
	}
	return &Directory{store: store, principal: principal}, nil
}

// msisdnMatches compares contact MSISDNs on digit sequences so a local
// ("0803...") record matches its international ("+234803...") form.
func msisdnMatches(a, b string) bool {
	digits := func(value string) string {
		var out []rune
		for _, character := range value {
			if character >= '0' && character <= '9' {
				out = append(out, character)
			}
		}
		return string(out)
	}
	digitsA, digitsB := digits(a), digits(b)
	nationalA := strings.TrimLeft(digitsA, "0")
	nationalB := strings.TrimLeft(digitsB, "0")
	if len(nationalA) < 7 || len(nationalB) < 7 {
		return false
	}
	return digitsA == digitsB ||
		strings.HasSuffix(digitsA, nationalB) ||
		strings.HasSuffix(digitsB, nationalA)
}

// BookingStatus is MSISDN-bound: the booking is returned only when its
// contact MSISDN belongs to the querying session; any other booking answers
// ErrNotFound so one phone can never enumerate another phone's bookings.
func (directory *Directory) BookingStatus(ctx context.Context, bookingID, msisdn string) (Booking, error) {
	found, err := directory.store.Get(ctx, bookingID)
	if err != nil {
		return Booking{}, err
	}
	if !msisdnMatches(found.TruckerMSISDN, msisdn) {
		return Booking{}, ErrNotFound
	}
	return found, nil
}

// BookSlotByID creates (idempotently, keyed by requestID) a USSD-channel
// booking priced by the terminal fee and reserves the requested slot. A replay
// of the same session returns the retained booking instead of double-booking.
func (directory *Directory) BookSlotByID(ctx context.Context, slotID, truckPlate, msisdn, requestID string) (Booking, error) {
	slot, err := directory.store.GetSlot(ctx, slotID)
	if err != nil {
		return Booking{}, err
	}
	created, err := directory.store.Create(ctx, CreateRequest{
		RequestID:     requestID,
		TruckPlate:    truckPlate,
		TruckerMSISDN: msisdn,
		TerminalID:    slot.TerminalID,
		Channel:       ChannelUSSD,
		AmountKobo:    slot.BookingFeeKobo,
		ExpiresAt:     slot.EndsAt,
	}, directory.principal)
	if err != nil {
		return Booking{}, err
	}
	if created.Status == StatusSlotReserved {
		if created.SlotID != nil && *created.SlotID == slotID {
			return created, nil
		}
		return Booking{}, ErrIdempotencyConflict
	}
	return directory.store.ReserveSlot(ctx, created.BookingID, slotID, created.Version, directory.principal)
}
