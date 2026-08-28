package nswadapter

import (
	"time"
)

// Delivery lifecycle states persisted in the nsw_delivery ledger.
const (
	StatusPending         = "PENDING"
	StatusDelivered       = "DELIVERED"
	StatusFailedPermanent = "FAILED_PERMANENT"
)

// Delivery is one NSW handoff obligation for a single outbox event.
type Delivery struct {
	DeliveryID    string
	TenantID      string
	Source        string // platform_outbox or port_call_outbox
	EventID       string
	EventType     string
	CallReference string
	ContentType   string
	Payload       string
	PayloadSHA256 string
	Status        string
	Attempts      int
	MaxAttempts   int
	NextAttemptAt time.Time
}

// Backoff computes the delay before the next attempt after the given
// 1-based failed attempt: base doubling each attempt, capped at max.
func Backoff(failedAttempt int, base, max time.Duration) time.Duration {
	if failedAttempt < 1 {
		failedAttempt = 1
	}
	delay := base
	for attempt := 1; attempt < failedAttempt; attempt++ {
		delay *= 2
		if delay >= max {
			return max
		}
	}
	if delay > max {
		return max
	}
	return delay
}

// AttemptOutcome is the new persisted state after one delivery attempt.
type AttemptOutcome struct {
	Status        string
	Attempts      int
	NextAttemptAt time.Time
	DeliveredAt   *time.Time
	LastError     string
}

// settleAttempt applies the delivery state machine to one attempt. A
// successful send delivers immediately; a failure retries with backoff until
// the attempt budget is exhausted, then fails permanently — never silently.
func settleAttempt(now time.Time, delivery Delivery, sendErr error, backoffBase, backoffMax time.Duration) AttemptOutcome {
	now = now.UTC()
	attempts := delivery.Attempts + 1
	if sendErr == nil {
		return AttemptOutcome{
			Status:      StatusDelivered,
			Attempts:    attempts,
			DeliveredAt: &now,
		}
	}
	outcome := AttemptOutcome{
		Attempts:  attempts,
		LastError: sendErr.Error(),
	}
	if attempts >= delivery.MaxAttempts {
		outcome.Status = StatusFailedPermanent
		return outcome
	}
	outcome.Status = StatusPending
	outcome.NextAttemptAt = now.Add(Backoff(attempts, backoffBase, backoffMax))
	return outcome
}
