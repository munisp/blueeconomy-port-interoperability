// Package queue implements the eCallUp 2.0 truck call-up queue: a fail-closed
// per-terminal FIFO with priority classes, DB-enforced call-up capacity and
// grace-window forfeiture, following the Lagos e-call-up precedent. A truck
// requests entry to a terminal queue, receives an atomic position, is called
// up when terminal capacity frees and must arrive within the grace window or
// forfeit its place.
package queue

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type Status string

const (
	StatusRequested              Status = "REQUESTED"
	StatusQueued                 Status = "QUEUED"
	StatusCalledUp               Status = "CALLED_UP"
	StatusEnRoute                Status = "EN_ROUTE"
	StatusArrived                Status = "ARRIVED"
	StatusCancelled              Status = "CANCELLED"
	StatusExpired                Status = "EXPIRED"
	StatusForfeited              Status = "FORFEITED"
	StatusReconciliationRequired Status = "RECONCILIATION_REQUIRED"
)

// PriorityClass orders the queue: PERISHABLE and PRIORITY cargo jump the
// STANDARD queue but never reorder within their own class (FIFO by position).
type PriorityClass string

const (
	ClassStandard   PriorityClass = "STANDARD"
	ClassPerishable PriorityClass = "PERISHABLE"
	ClassPriority   PriorityClass = "PRIORITY"
)

// rank orders priority classes for head-of-queue selection; lower is earlier.
func (class PriorityClass) rank() int {
	switch class {
	case ClassPerishable:
		return 0
	case ClassPriority:
		return 1
	default:
		return 2
	}
}

var (
	ErrNotFound            = errors.New("queue resource not found")
	ErrInvalidTransition   = errors.New("invalid queue state transition")
	ErrOptimisticConflict  = errors.New("queue request changed concurrently")
	ErrIdempotencyConflict = errors.New("request id conflicts with a retained queue request")
	ErrCallUpCapacity      = errors.New("terminal has no remaining call-up capacity")
	ErrGraceWindow         = errors.New("call-up grace window has elapsed")
)

// transitions is the complete fail-closed queue state machine. Any pair not
// listed here is prohibited.
var transitions = map[Status]map[Status]bool{
	StatusRequested: {
		StatusQueued:                 true,
		StatusCancelled:              true,
		StatusReconciliationRequired: true,
	},
	StatusQueued: {
		StatusCalledUp:               true,
		StatusExpired:                true,
		StatusCancelled:              true,
		StatusReconciliationRequired: true,
	},
	StatusCalledUp: {
		StatusEnRoute:                true, // trucker acknowledges the call-up
		StatusArrived:                true, // gate scan may confirm arrival directly
		StatusForfeited:              true, // grace window elapsed
		StatusCancelled:              true,
		StatusReconciliationRequired: true,
	},
	StatusEnRoute: {
		StatusArrived:                true,
		StatusForfeited:              true, // grace window elapsed on the road
		StatusCancelled:              true,
		StatusReconciliationRequired: true,
	},
	StatusReconciliationRequired: {
		StatusQueued:    true, // operator re-queues at the tail of the class
		StatusCancelled: true,
	},
}

func ValidTransition(current, next Status) bool {
	return transitions[current][next]
}

var (
	truckPlatePattern = regexp.MustCompile(`^[A-Z0-9][A-Z0-9-]{2,15}$`)
	msisdnPattern     = regexp.MustCompile(`^\+[0-9]{8,15}$`)
	terminalPattern   = regexp.MustCompile(`^[A-Z][A-Z0-9-]{1,31}$`)
)

type CreateRequest struct {
	IdempotencyKey string        `json:"idempotency_key"`
	TruckPlate     string        `json:"truck_plate"`
	TruckerMSISDN  string        `json:"trucker_msisdn"`
	TerminalID     string        `json:"terminal_id"`
	PriorityClass  PriorityClass `json:"priority_class"`
	// BookingID references an existing booking; when empty a PENDING (DRAFTED)
	// booking priced at the terminal fee is created atomically with the queue
	// request.
	BookingID string `json:"booking_id,omitempty"`
}

type Request struct {
	QueueRequestID       string        `json:"queue_request_id"`
	TenantID             string        `json:"tenant_id"`
	IdempotencyKey       string        `json:"idempotency_key"`
	BookingID            *string       `json:"booking_id,omitempty"`
	TruckPlate           string        `json:"truck_plate"`
	TruckerMSISDN        string        `json:"trucker_msisdn"`
	TerminalID           string        `json:"terminal_id"`
	PriorityClass        PriorityClass `json:"priority_class"`
	Status               Status        `json:"status"`
	Position             *int64        `json:"position,omitempty"`
	CalledUpAt           *time.Time    `json:"called_up_at,omitempty"`
	GraceDeadline        *time.Time    `json:"grace_deadline,omitempty"`
	ArrivedAt            *time.Time    `json:"arrived_at,omitempty"`
	GateID               *string       `json:"gate_id,omitempty"`
	ForfeitReason        *string       `json:"forfeit_reason,omitempty"`
	ReconciliationReason *string       `json:"reconciliation_reason,omitempty"`
	CreatedAt            time.Time     `json:"created_at"`
	UpdatedAt            time.Time     `json:"updated_at"`
	Version              int64         `json:"version"`
}

func (request CreateRequest) Validate() error {
	if len(request.IdempotencyKey) < 8 || len(request.IdempotencyKey) > 128 || request.IdempotencyKey != strings.TrimSpace(request.IdempotencyKey) {
		return errors.New("idempotency_key must be canonical text between 8 and 128 characters")
	}
	if !truckPlatePattern.MatchString(request.TruckPlate) {
		return errors.New("truck_plate must be an uppercase registration plate")
	}
	if !msisdnPattern.MatchString(request.TruckerMSISDN) {
		return errors.New("trucker_msisdn must be E.164")
	}
	if !terminalPattern.MatchString(request.TerminalID) {
		return errors.New("terminal_id is invalid")
	}
	switch request.PriorityClass {
	case ClassStandard, ClassPerishable, ClassPriority:
	default:
		return errors.New("priority_class must be STANDARD, PERISHABLE or PRIORITY")
	}
	if request.BookingID != "" && len(request.BookingID) > 64 {
		return errors.New("booking_id is invalid")
	}
	return nil
}

func validateTerminalID(value string) error {
	if !terminalPattern.MatchString(value) {
		return fmt.Errorf("terminal_id %q is invalid", value)
	}
	return nil
}
