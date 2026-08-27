package queue

import (
	"context"
	"errors"

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
func (directory *Directory) QueueStatus(ctx context.Context, queueRequestID string) (Request, error) {
	return directory.store.Get(ctx, queueRequestID)
}
