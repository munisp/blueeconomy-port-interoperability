package queue

import (
	"context"
	"errors"
	"strings"

	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
)

// Directory adapts the queue store to the USSD gateway's narrow interface.
type Directory struct {
	store     *Store
	principal Principal
}

func NewDirectory(store *Store, principal Principal) (*Directory, error) {
	if store == nil || principal.ID == "" || principal.Role == "" {
		return nil, errors.New("queue directory requires a store and a principal")
	}
	return &Directory{store: store, principal: principal}, nil
}

// RequestQueueEntry creates (idempotently, keyed by requestID) a USSD-channel
// queue request for the terminal with a pending booking. A replay of the same
// session returns the retained request instead of double-queueing.
func (directory *Directory) RequestQueueEntry(ctx context.Context, terminalID, truckPlate, msisdn, requestID string) (Request, error) {
	return directory.store.Create(ctx, CreateRequest{
		IdempotencyKey: requestID,
		TruckPlate:     truckPlate,
		TruckerMSISDN:  msisdn,
		TerminalID:     terminalID,
		PriorityClass:  ClassStandard,
	}, booking.ChannelUSSD, directory.principal)
}

// QueueStatus returns the position and call-up state of a queue request.
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

// QueueStatus is MSISDN-bound: the request is returned only when its contact
// MSISDN belongs to the querying session; any other request answers
// ErrNotFound so one phone can never enumerate another phone's queue entries.
func (directory *Directory) QueueStatus(ctx context.Context, queueRequestID, msisdn string) (Request, error) {
	found, err := directory.store.Get(ctx, queueRequestID)
	if err != nil {
		return Request{}, err
	}
	if !msisdnMatches(found.TruckerMSISDN, msisdn) {
		return Request{}, ErrNotFound
	}
	return found, nil
}
