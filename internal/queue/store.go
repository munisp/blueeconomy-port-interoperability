package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantdb"
)

// Principal is the verified actor behind a queue mutation; it becomes the
// provenance block of every emitted platform event.
type Principal = booking.Principal

// DefaultGraceWindow is the Lagos e-call-up precedent: a called-up truck must
// arrive within 90 minutes or forfeit its place.
const DefaultGraceWindow = 90 * time.Minute

// pendingBookingValidity bounds the DRAFTED booking created alongside a queue
// request that does not reference an existing booking.
const pendingBookingValidity = 12 * time.Hour

type Store struct {
	pool        *pgxpool.Pool
	bookings    *booking.Store
	signer      *events.Signer
	graceWindow time.Duration
}

// NewStore fails closed without a booking store (the pending-booking leg) or a
// negative grace window; a zero grace window selects DefaultGraceWindow. The
// envelope signer is mandatory: call-up events are JWS-signed at emission.
func NewStore(pool *pgxpool.Pool, bookings *booking.Store, signer *events.Signer, graceWindow time.Duration) (*Store, error) {
	if bookings == nil {
		return nil, errors.New("queue store requires a booking store")
	}
	if signer == nil {
		return nil, errors.New("queue store requires an envelope signer")
	}
	if graceWindow < 0 {
		return nil, errors.New("call-up grace window must not be negative")
	}
	if graceWindow == 0 {
		graceWindow = DefaultGraceWindow
	}
	return &Store{pool: pool, bookings: bookings, signer: signer, graceWindow: graceWindow}, nil
}

func Open(ctx context.Context, databaseURL string, bookings *booking.Store, signer *events.Signer, graceWindow time.Duration) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return NewStore(pool, bookings, signer, graceWindow)
}

func (store *Store) Close() {
	store.pool.Close()
}

func (store *Store) Pool() *pgxpool.Pool {
	return store.pool
}

func (store *Store) Exec(ctx context.Context, statement string) (int64, error) {
	result, err := store.pool.Exec(ctx, statement)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (store *Store) withTx(ctx context.Context, work func(pgx.Tx, tenantctx.Claims) error) error {
	return tenantdb.WithTx(ctx, store.pool, work)
}

const queueColumns = `queue_request_id, tenant_id, idempotency_key, booking_id, truck_plate,
	trucker_msisdn, terminal_id, priority_class, status, position, called_up_at,
	grace_deadline, arrived_at, gate_id, forfeit_reason, reconciliation_reason,
	created_at, updated_at, version`

func scanRequest(row pgx.Row) (Request, error) {
	var request Request
	err := row.Scan(&request.QueueRequestID, &request.TenantID, &request.IdempotencyKey, &request.BookingID,
		&request.TruckPlate, &request.TruckerMSISDN, &request.TerminalID, &request.PriorityClass,
		&request.Status, &request.Position, &request.CalledUpAt, &request.GraceDeadline, &request.ArrivedAt,
		&request.GateID, &request.ForfeitReason, &request.ReconciliationReason, &request.CreatedAt,
		&request.UpdatedAt, &request.Version)
	return request, err
}

// emit writes a FHIR-enveloped ports.queue.v1 event into the transactional
// platform outbox inside the caller's transaction.
func emit(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, eventType, correlationID, subjectID string, payload any, extensions map[string]string, principal Principal, occurredAt time.Time, signer *events.Signer) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s payload: %w", eventType, err)
	}
	envelope, err := events.Message(eventType, events.TopicQueue, correlationID, subjectID, payloadJSON, extensions, events.Provenance{
		PrincipalID:   principal.ID,
		PrincipalRole: principal.Role,
	}, occurredAt, signer)
	if err != nil {
		return fmt.Errorf("build %s envelope: %w", eventType, err)
	}
	envelopeJSON, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode %s envelope: %w", eventType, err)
	}
	eventID, err := uuid.Parse(envelope.EventID)
	if err != nil {
		return fmt.Errorf("parse event id: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO platform_outbox (event_id, tenant_id, topic, event_type, idempotency_key, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		eventID, claims.TenantID, events.TopicQueue, eventType, envelope.EventID, envelopeJSON, occurredAt); err != nil {
		return fmt.Errorf("write %s outbox event: %w", eventType, err)
	}
	return nil
}

// lockTerminal takes the terminal row FOR UPDATE, which serializes position
// assignment and call-up promotion for the terminal.
func lockTerminal(ctx context.Context, tx pgx.Tx, terminalID string) (active bool, bookingFeeKobo int64, queueCapacity int, err error) {
	err = tx.QueryRow(ctx, `SELECT active, booking_fee_kobo, queue_capacity FROM port_terminals WHERE terminal_id=$1 FOR UPDATE`, terminalID).
		Scan(&active, &bookingFeeKobo, &queueCapacity)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, 0, 0, ErrNotFound
	}
	if err != nil {
		return false, 0, 0, fmt.Errorf("lock terminal: %w", err)
	}
	return active, bookingFeeKobo, queueCapacity, nil
}

// Create registers a queue request, assigns its atomic per-terminal position
// and moves it REQUESTED -> QUEUED in one transaction. When the request does
// not reference an existing booking, a PENDING (DRAFTED) booking priced at the
// terminal fee is created atomically. Idempotent on (tenant, idempotency_key).
func (store *Store) Create(ctx context.Context, request CreateRequest, channel booking.Channel, principal Principal) (Request, error) {
	if err := request.Validate(); err != nil {
		return Request{}, err
	}
	if channel != booking.ChannelWeb && channel != booking.ChannelUSSD {
		return Request{}, errors.New("queue channel must be WEB or USSD")
	}
	if principal.ID == "" || principal.Role == "" {
		return Request{}, errors.New("queue principal is required")
	}
	var created Request
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		active, bookingFeeKobo, _, err := lockTerminal(ctx, tx, request.TerminalID)
		if err != nil {
			return err
		}
		if !active {
			return errors.New("terminal is not active")
		}
		bookingID := request.BookingID
		if bookingID == "" {
			pending, err := store.bookings.CreateTx(ctx, tx, claims, booking.CreateRequest{
				RequestID:     request.IdempotencyKey,
				TruckPlate:    request.TruckPlate,
				TruckerMSISDN: request.TruckerMSISDN,
				TerminalID:    request.TerminalID,
				Channel:       channel,
				AmountKobo:    bookingFeeKobo,
				ExpiresAt:     time.Now().UTC().Add(pendingBookingValidity),
			}, principal)
			if err != nil {
				return fmt.Errorf("create pending booking: %w", err)
			}
			bookingID = pending.BookingID
		} else {
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM truck_bookings WHERE booking_id=$1)`, bookingID).Scan(&exists); err != nil {
				return fmt.Errorf("verify referenced booking: %w", err)
			}
			if !exists {
				return ErrNotFound
			}
		}
		now := time.Now().UTC()
		row := tx.QueryRow(ctx, `
			INSERT INTO truck_queue_requests (
				queue_request_id, tenant_id, idempotency_key, booking_id, truck_plate, trucker_msisdn,
				terminal_id, priority_class, status, created_at, updated_at, version
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'REQUESTED',$9,$9,1)
			ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
			RETURNING `+queueColumns,
			uuid.New(), claims.TenantID, request.IdempotencyKey, bookingID, request.TruckPlate,
			request.TruckerMSISDN, request.TerminalID, request.PriorityClass, now)
		inserted, err := scanRequest(row)
		if errors.Is(err, pgx.ErrNoRows) {
			existing, lookupErr := scanRequest(tx.QueryRow(ctx, `SELECT `+queueColumns+` FROM truck_queue_requests WHERE tenant_id=$1 AND idempotency_key=$2 FOR UPDATE`, claims.TenantID, request.IdempotencyKey))
			if lookupErr != nil {
				return fmt.Errorf("lookup idempotent queue request: %w", lookupErr)
			}
			if existing.TruckPlate != request.TruckPlate || existing.TruckerMSISDN != request.TruckerMSISDN ||
				existing.TerminalID != request.TerminalID || existing.PriorityClass != request.PriorityClass {
				return ErrIdempotencyConflict
			}
			created = existing
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert queue request: %w", err)
		}
		if err := emit(ctx, tx, claims, "queue.requested", inserted.IdempotencyKey, inserted.QueueRequestID, inserted, map[string]string{
			"terminal-id":    inserted.TerminalID,
			"priority-class": string(inserted.PriorityClass),
		}, principal, now, store.signer); err != nil {
			return err
		}
		// Position assignment runs under the terminal row lock taken above, so
		// concurrent creators serialize and exactly one wins each position.
		var position int64
		if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(position), 0) + 1 FROM truck_queue_requests WHERE terminal_id=$1`, request.TerminalID).Scan(&position); err != nil {
			return fmt.Errorf("assign queue position: %w", err)
		}
		queued, err := store.transitionTx(ctx, tx, claims, inserted, StatusQueued, func(updated *Request) {
			updated.Position = &position
		}, "queue.position_assigned", map[string]string{
			"terminal-id": inserted.TerminalID,
			"position":    fmt.Sprintf("%d", position),
		}, principal)
		if err != nil {
			return err
		}
		created = queued
		return nil
	})
	return created, err
}

func (store *Store) Get(ctx context.Context, queueRequestID string) (Request, error) {
	var request Request
	err := store.withTx(ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		found, err := scanRequest(tx.QueryRow(ctx, `SELECT `+queueColumns+` FROM truck_queue_requests WHERE queue_request_id=$1`, queueRequestID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("get queue request: %w", err)
		}
		request = found
		return nil
	})
	return request, err
}

func (store *Store) getForUpdate(ctx context.Context, tx pgx.Tx, queueRequestID string) (Request, error) {
	request, err := scanRequest(tx.QueryRow(ctx, `SELECT `+queueColumns+` FROM truck_queue_requests WHERE queue_request_id=$1 FOR UPDATE`, queueRequestID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Request{}, ErrNotFound
	}
	if err != nil {
		return Request{}, fmt.Errorf("lock queue request: %w", err)
	}
	return request, nil
}

func (store *Store) transitionTx(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, request Request, next Status, mutate func(*Request), eventType string, extensions map[string]string, principal Principal) (Request, error) {
	if !ValidTransition(request.Status, next) {
		return Request{}, ErrInvalidTransition
	}
	updated := request
	updated.Status = next
	updated.UpdatedAt = time.Now().UTC()
	updated.Version++
	if mutate != nil {
		mutate(&updated)
	}
	result, err := scanRequest(tx.QueryRow(ctx, `
		UPDATE truck_queue_requests
		SET status=$1, position=$2, called_up_at=$3, grace_deadline=$4, arrived_at=$5, gate_id=$6,
			forfeit_reason=$7, reconciliation_reason=$8, updated_at=$9, version=$10
		WHERE queue_request_id=$11 AND status=$12 AND version=$13
		RETURNING `+queueColumns,
		updated.Status, updated.Position, updated.CalledUpAt, updated.GraceDeadline, updated.ArrivedAt,
		updated.GateID, updated.ForfeitReason, updated.ReconciliationReason, updated.UpdatedAt,
		updated.Version, request.QueueRequestID, request.Status, request.Version))
	if errors.Is(err, pgx.ErrNoRows) {
		return Request{}, ErrOptimisticConflict
	}
	if err != nil {
		return Request{}, fmt.Errorf("transition queue request to %s: %w", next, err)
	}
	if err := emit(ctx, tx, claims, eventType, result.IdempotencyKey, result.QueueRequestID, result, extensions, principal, result.UpdatedAt, store.signer); err != nil {
		return Request{}, err
	}
	return result, nil
}

// promoteNextTx promotes the head-of-queue to CALLED_UP when the terminal has
// remaining call-up capacity. Head selection honours priority classes
// (PERISHABLE, then PRIORITY, then STANDARD) and is FIFO by position within a
// class. Returns nil when nobody is queued or capacity is exhausted; the
// terminal row lock and the capacity trigger make over-promotion impossible.
// The caller must already serialize on the terminal or accept its lock here.
func (store *Store) promoteNextTx(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, terminalID string, principal Principal, now time.Time) (*Request, error) {
	if err := validateTerminalID(terminalID); err != nil {
		return nil, err
	}
	_, _, queueCapacity, err := lockTerminal(ctx, tx, terminalID)
	if err != nil {
		return nil, err
	}
	var active int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM truck_queue_requests WHERE terminal_id=$1 AND status IN ('CALLED_UP','EN_ROUTE')`, terminalID).Scan(&active); err != nil {
		return nil, fmt.Errorf("count active call-ups: %w", err)
	}
	if active >= queueCapacity {
		return nil, nil
	}
	head, err := scanRequest(tx.QueryRow(ctx, `
		SELECT `+queueColumns+` FROM truck_queue_requests
		WHERE terminal_id=$1 AND status='QUEUED'
		ORDER BY CASE priority_class WHEN 'PERISHABLE' THEN 0 WHEN 'PRIORITY' THEN 1 ELSE 2 END, position
		LIMIT 1 FOR UPDATE`, terminalID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select head of queue: %w", err)
	}
	now = now.UTC()
	deadline := now.Add(store.graceWindow)
	promoted, err := store.transitionTx(ctx, tx, claims, head, StatusCalledUp, func(updated *Request) {
		updated.CalledUpAt = &now
		updated.GraceDeadline = &deadline
	}, "queue.called_up", map[string]string{
		"terminal-id":    terminalID,
		"grace-deadline": deadline.Format(time.RFC3339),
	}, principal)
	if err != nil {
		return nil, err
	}
	return &promoted, nil
}

// ConfigureTerminal sets the per-terminal call-up capacity (the maximum
// number of CALLED_UP/EN_ROUTE requests the terminal may hold at once).
func (store *Store) ConfigureTerminal(ctx context.Context, terminalID string, queueCapacity int) error {
	if err := validateTerminalID(terminalID); err != nil {
		return err
	}
	if queueCapacity < 1 {
		return errors.New("queue capacity must be at least 1")
	}
	return store.withTx(ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		result, err := tx.Exec(ctx, `UPDATE port_terminals SET queue_capacity=$1 WHERE terminal_id=$2`, queueCapacity, terminalID)
		if err != nil {
			return fmt.Errorf("configure terminal queue capacity: %w", err)
		}
		if result.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// PromoteNext is the public capacity-release hook: after terminal capacity
// frees, the head-of-queue is called up with the configured grace window.
func (store *Store) PromoteNext(ctx context.Context, terminalID string, principal Principal) (*Request, error) {
	var promoted *Request
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		result, err := store.promoteNextTx(ctx, tx, claims, terminalID, principal, time.Now().UTC())
		if err != nil {
			return err
		}
		promoted = result
		return nil
	})
	return promoted, err
}

// CapacityReleased implements booking.CapacityListener: booking slot releases
// (cancel, expire, complete) promote the head-of-queue in the same transaction.
func (store *Store) CapacityReleased(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, terminalID string, principal booking.Principal) error {
	_, err := store.promoteNextTx(ctx, tx, claims, terminalID, principal, time.Now().UTC())
	return err
}

// Arrive confirms a called-up truck at the gate: CALLED_UP or EN_ROUTE moves
// to ARRIVED and the freed call-up capacity immediately promotes the next in
// queue. Arrival after the grace deadline is a fail-closed forfeiture: the
// request is marked FORFEITED, the audit event is emitted and the next truck
// is promoted before ErrGraceWindow is returned.
func (store *Store) Arrive(ctx context.Context, queueRequestID, gateID string, expectedVersion int64, principal Principal) (Request, *Request, error) {
	if len(gateID) < 2 || len(gateID) > 64 {
		return Request{}, nil, errors.New("gate_id must be between 2 and 64 characters")
	}
	var arrived Request
	var promoted *Request
	lateArrival := false
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		request, err := store.getForUpdate(ctx, tx, queueRequestID)
		if err != nil {
			return err
		}
		if request.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		if request.Status != StatusCalledUp && request.Status != StatusEnRoute {
			return ErrInvalidTransition
		}
		now := time.Now().UTC()
		if request.GraceDeadline != nil && now.After(*request.GraceDeadline) {
			// Late arrival is a fail-closed forfeiture: it must commit (audit
			// event + chain promotion), so the sentinel error is returned only
			// after the transaction succeeds.
			reason := "arrived after the call-up grace window"
			if _, err := store.transitionTx(ctx, tx, claims, request, StatusForfeited, func(updated *Request) {
				updated.ForfeitReason = &reason
			}, "queue.forfeited", map[string]string{
				"terminal-id": request.TerminalID,
				"reason":      reason,
			}, principal); err != nil {
				return err
			}
			promoted, err = store.promoteNextTx(ctx, tx, claims, request.TerminalID, principal, now)
			if err != nil {
				return err
			}
			lateArrival = true
			return nil
		}
		result, err := store.transitionTx(ctx, tx, claims, request, StatusArrived, func(updated *Request) {
			updated.ArrivedAt = &now
			updated.GateID = &gateID
		}, "queue.arrived", map[string]string{
			"terminal-id": request.TerminalID,
			"gate-id":     gateID,
		}, principal)
		if err != nil {
			return err
		}
		promoted, err = store.promoteNextTx(ctx, tx, claims, request.TerminalID, principal, now)
		if err != nil {
			return err
		}
		arrived = result
		return nil
	})
	if err == nil && lateArrival {
		return Request{}, promoted, ErrGraceWindow
	}
	return arrived, promoted, err
}

// Depart records the trucker acknowledging a call-up: CALLED_UP -> EN_ROUTE.
func (store *Store) Depart(ctx context.Context, queueRequestID string, expectedVersion int64, principal Principal) (Request, error) {
	var departed Request
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		request, err := store.getForUpdate(ctx, tx, queueRequestID)
		if err != nil {
			return err
		}
		if request.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		result, err := store.transitionTx(ctx, tx, claims, request, StatusEnRoute, nil, "queue.en_route", map[string]string{
			"terminal-id": request.TerminalID,
		}, principal)
		if err != nil {
			return err
		}
		departed = result
		return nil
	})
	return departed, err
}

// Forfeit moves a CALLED_UP or EN_ROUTE request to FORFEITED with an audit
// event and promotes the next in queue (the call-up chain).
func (store *Store) Forfeit(ctx context.Context, queueRequestID, reason string, principal Principal) (Request, *Request, error) {
	if reason == "" || len(reason) > 1024 {
		return Request{}, nil, errors.New("forfeiture reason is required")
	}
	var forfeited Request
	var promoted *Request
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		request, err := store.getForUpdate(ctx, tx, queueRequestID)
		if err != nil {
			return err
		}
		result, promoted2, err := store.forfeitTx(ctx, tx, claims, request, reason, principal, time.Now().UTC())
		if err != nil {
			return err
		}
		forfeited = result
		promoted = promoted2
		return nil
	})
	return forfeited, promoted, err
}

func (store *Store) forfeitTx(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, request Request, reason string, principal Principal, now time.Time) (Request, *Request, error) {
	result, err := store.transitionTx(ctx, tx, claims, request, StatusForfeited, func(updated *Request) {
		updated.ForfeitReason = &reason
	}, "queue.forfeited", map[string]string{
		"terminal-id": request.TerminalID,
		"reason":      reason,
	}, principal)
	if err != nil {
		return Request{}, nil, err
	}
	promoted, err := store.promoteNextTx(ctx, tx, claims, request.TerminalID, principal, now)
	if err != nil {
		return Request{}, nil, err
	}
	return result, promoted, nil
}

// ForfeitExpired sweeps call-ups whose grace window elapsed into FORFEITED,
// emitting one audit event each and chaining the next-in-queue promotion.
// It returns the forfeited count and every newly promoted request so callers
// can start call-up workflows for them.
func (store *Store) ForfeitExpired(ctx context.Context, now time.Time, principal Principal) (int, []Request, error) {
	count := 0
	var promotedAll []Request
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		rows, err := tx.Query(ctx, `SELECT `+queueColumns+` FROM truck_queue_requests
			WHERE status IN ('CALLED_UP','EN_ROUTE') AND grace_deadline < $1 FOR UPDATE SKIP LOCKED`, now.UTC())
		if err != nil {
			return fmt.Errorf("find forfeitable call-ups: %w", err)
		}
		var due []Request
		for rows.Next() {
			request, err := scanRequest(rows)
			if err != nil {
				rows.Close()
				return fmt.Errorf("scan forfeitable call-up: %w", err)
			}
			due = append(due, request)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, request := range due {
			_, promoted, err := store.forfeitTx(ctx, tx, claims, request, "call-up grace window elapsed", principal, now)
			if err != nil {
				return err
			}
			count++
			if promoted != nil {
				promotedAll = append(promotedAll, *promoted)
			}
		}
		return nil
	})
	return count, promotedAll, err
}

// ExpireStale sweeps QUEUED requests created before the cutoff into EXPIRED
// (they never held call-up capacity, so no promotion chain is needed).
func (store *Store) ExpireStale(ctx context.Context, createdBefore time.Time, principal Principal) (int, error) {
	count := 0
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		rows, err := tx.Query(ctx, `SELECT `+queueColumns+` FROM truck_queue_requests
			WHERE status='QUEUED' AND created_at < $1 FOR UPDATE SKIP LOCKED`, createdBefore.UTC())
		if err != nil {
			return fmt.Errorf("find stale queued requests: %w", err)
		}
		var due []Request
		for rows.Next() {
			request, err := scanRequest(rows)
			if err != nil {
				rows.Close()
				return fmt.Errorf("scan stale queued request: %w", err)
			}
			due = append(due, request)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, request := range due {
			if _, err := store.transitionTx(ctx, tx, claims, request, StatusExpired, nil, "queue.expired", map[string]string{
				"terminal-id": request.TerminalID,
			}, principal); err != nil {
				return err
			}
			count++
		}
		return nil
	})
	return count, err
}

// Cancel moves any non-terminal queue request into CANCELLED; a cancelled
// call-up frees capacity and chains the next promotion.
func (store *Store) Cancel(ctx context.Context, queueRequestID string, expectedVersion int64, reason string, principal Principal) (Request, *Request, error) {
	if reason == "" || len(reason) > 1024 {
		return Request{}, nil, errors.New("cancellation reason is required")
	}
	var cancelled Request
	var promoted *Request
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		request, err := store.getForUpdate(ctx, tx, queueRequestID)
		if err != nil {
			return err
		}
		if request.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		wasCalledUp := request.Status == StatusCalledUp || request.Status == StatusEnRoute
		result, err := store.transitionTx(ctx, tx, claims, request, StatusCancelled, func(updated *Request) {
			updated.ReconciliationReason = &reason
		}, "queue.cancelled", map[string]string{"reason": reason}, principal)
		if err != nil {
			return err
		}
		if wasCalledUp {
			promoted, err = store.promoteNextTx(ctx, tx, claims, request.TerminalID, principal, time.Now().UTC())
			if err != nil {
				return err
			}
		}
		cancelled = result
		return nil
	})
	return cancelled, promoted, err
}

// FlagReconciliation surfaces a conflicted live request (e.g. its booking was
// cancelled or expired elsewhere) as RECONCILIATION_REQUIRED — never a silent
// drop. A flagged call-up frees capacity and chains the next promotion.
func (store *Store) FlagReconciliation(ctx context.Context, queueRequestID string, expectedVersion int64, reason string, principal Principal) (Request, error) {
	if reason == "" || len(reason) > 1024 {
		return Request{}, errors.New("reconciliation reason is required")
	}
	var flagged Request
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		request, err := store.getForUpdate(ctx, tx, queueRequestID)
		if err != nil {
			return err
		}
		if request.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		wasCalledUp := request.Status == StatusCalledUp || request.Status == StatusEnRoute
		result, err := store.transitionTx(ctx, tx, claims, request, StatusReconciliationRequired, func(updated *Request) {
			updated.ReconciliationReason = &reason
		}, "queue.reconciliation_required", map[string]string{"reason": reason}, principal)
		if err != nil {
			return err
		}
		if wasCalledUp {
			if _, err := store.promoteNextTx(ctx, tx, claims, request.TerminalID, principal, time.Now().UTC()); err != nil {
				return err
			}
		}
		flagged = result
		return nil
	})
	return flagged, err
}

// Requeue resolves RECONCILIATION_REQUIRED by appending the request at the
// tail of its priority class with a fresh atomic position.
func (store *Store) Requeue(ctx context.Context, queueRequestID string, expectedVersion int64, principal Principal) (Request, error) {
	var requeued Request
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		request, err := store.getForUpdate(ctx, tx, queueRequestID)
		if err != nil {
			return err
		}
		if request.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		if request.Status != StatusReconciliationRequired {
			return ErrInvalidTransition
		}
		if _, _, _, err := lockTerminal(ctx, tx, request.TerminalID); err != nil {
			return err
		}
		var position int64
		if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(position), 0) + 1 FROM truck_queue_requests WHERE terminal_id=$1`, request.TerminalID).Scan(&position); err != nil {
			return fmt.Errorf("assign queue position: %w", err)
		}
		result, err := store.transitionTx(ctx, tx, claims, request, StatusQueued, func(updated *Request) {
			updated.Position = &position
			updated.ReconciliationReason = nil
			updated.CalledUpAt = nil
			updated.GraceDeadline = nil
		}, "queue.position_assigned", map[string]string{
			"terminal-id": request.TerminalID,
			"position":    fmt.Sprintf("%d", position),
			"requeue":     "true",
		}, principal)
		if err != nil {
			return err
		}
		requeued = result
		return nil
	})
	return requeued, err
}

// ListTerminal is the operator queue view: live entries ordered by priority
// class, then FIFO position within the class.
func (store *Store) ListTerminal(ctx context.Context, terminalID string) ([]Request, error) {
	if err := validateTerminalID(terminalID); err != nil {
		return nil, err
	}
	var requests []Request
	err := store.withTx(ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		rows, err := tx.Query(ctx, `
			SELECT `+queueColumns+` FROM truck_queue_requests
			WHERE terminal_id=$1 AND status IN ('QUEUED','CALLED_UP','EN_ROUTE')
			ORDER BY CASE priority_class WHEN 'PERISHABLE' THEN 0 WHEN 'PRIORITY' THEN 1 ELSE 2 END, position`, terminalID)
		if err != nil {
			return fmt.Errorf("list terminal queue: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			request, err := scanRequest(rows)
			if err != nil {
				return fmt.Errorf("scan terminal queue entry: %w", err)
			}
			requests = append(requests, request)
		}
		return rows.Err()
	})
	return requests, err
}

// ListActiveCallUps returns every request currently holding call-up capacity
// (CALLED_UP or EN_ROUTE). The sweeper uses it to idempotently ensure each
// active call-up has a running grace-window workflow.
func (store *Store) ListActiveCallUps(ctx context.Context) ([]Request, error) {
	var requests []Request
	err := store.withTx(ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		rows, err := tx.Query(ctx, `
			SELECT `+queueColumns+` FROM truck_queue_requests
			WHERE status IN ('CALLED_UP','EN_ROUTE')
			ORDER BY terminal_id, grace_deadline`)
		if err != nil {
			return fmt.Errorf("list active call-ups: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			request, err := scanRequest(rows)
			if err != nil {
				return fmt.Errorf("scan active call-up: %w", err)
			}
			requests = append(requests, request)
		}
		return rows.Err()
	})
	return requests, err
}

// ReconcileCallUps is the periodic sweeper: it forfeits elapsed grace windows
// (chaining promotions) and fills any free call-up capacity from the head of
// every terminal queue. It returns every request promoted during the sweep so
// the caller can (idempotently) start call-up workflows for them.
func (store *Store) ReconcileCallUps(ctx context.Context, principal Principal) ([]Request, error) {
	now := time.Now().UTC()
	var promotedAll []Request
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		rows, err := tx.Query(ctx, `SELECT `+queueColumns+` FROM truck_queue_requests
			WHERE status IN ('CALLED_UP','EN_ROUTE') AND grace_deadline < $1 FOR UPDATE SKIP LOCKED`, now)
		if err != nil {
			return fmt.Errorf("find forfeitable call-ups: %w", err)
		}
		var due []Request
		for rows.Next() {
			request, err := scanRequest(rows)
			if err != nil {
				rows.Close()
				return fmt.Errorf("scan forfeitable call-up: %w", err)
			}
			due = append(due, request)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, request := range due {
			_, promoted, err := store.forfeitTx(ctx, tx, claims, request, "call-up grace window elapsed", principal, now)
			if err != nil {
				return err
			}
			if promoted != nil {
				promotedAll = append(promotedAll, *promoted)
			}
		}
		terminals, err := tx.Query(ctx, `SELECT DISTINCT terminal_id FROM truck_queue_requests WHERE status='QUEUED'`)
		if err != nil {
			return fmt.Errorf("find queued terminals: %w", err)
		}
		var terminalIDs []string
		for terminals.Next() {
			var terminalID string
			if err := terminals.Scan(&terminalID); err != nil {
				terminals.Close()
				return fmt.Errorf("scan queued terminal: %w", err)
			}
			terminalIDs = append(terminalIDs, terminalID)
		}
		terminals.Close()
		if err := terminals.Err(); err != nil {
			return err
		}
		for _, terminalID := range terminalIDs {
			for {
				promoted, err := store.promoteNextTx(ctx, tx, claims, terminalID, principal, now)
				if err != nil {
					return err
				}
				if promoted == nil {
					break
				}
				promotedAll = append(promotedAll, *promoted)
			}
		}
		return nil
	})
	return promotedAll, err
}
