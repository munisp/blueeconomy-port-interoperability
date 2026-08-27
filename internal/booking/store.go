package booking

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantdb"
)

// Principal describes the verified actor behind a booking mutation; it becomes
// the provenance block of every emitted platform event.
type Principal struct {
	ID   string
	Role string
}

// CapacityListener is invoked inside the same transaction whenever a booking
// releases terminal slot capacity (cancellation, expiry or completion). The
// queue package implements it to call up the head of the terminal queue
// atomically with the release.
type CapacityListener interface {
	CapacityReleased(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, terminalID string, principal Principal) error
}

type Store struct {
	pool     *pgxpool.Pool
	listener CapacityListener
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// SetCapacityListener wires the capacity-release hook. A nil listener
// disables it; releases then leave queue promotion to the sweeper.
func (store *Store) SetCapacityListener(listener CapacityListener) {
	store.listener = listener
}

// capacityReleased notifies the listener, when wired, that a booking left a
// capacity-holding state. The caller passes the pre-transition booking; only
// SLOT_RESERVED, PAID and GATE_APPROVED occupy terminal slot capacity.
func (store *Store) capacityReleased(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, before Booking, principal Principal) error {
	if store.listener == nil || before.SlotID == nil {
		return nil
	}
	switch before.Status {
	case StatusSlotReserved, StatusPaid, StatusGateApproved:
		return store.listener.CapacityReleased(ctx, tx, claims, before.TerminalID, principal)
	}
	return nil
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return NewStore(pool), nil
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

const bookingColumns = `booking_id, tenant_id, request_id, truck_plate, trucker_msisdn, terminal_id,
	slot_id, channel, status, amount_kobo, currency, payment_receipt_ref, gate_id,
	ledger_commit_hash, reconciliation_reason, created_at, updated_at, expires_at, version`

func scanBooking(row pgx.Row) (Booking, error) {
	var booking Booking
	err := row.Scan(&booking.BookingID, &booking.TenantID, &booking.RequestID, &booking.TruckPlate,
		&booking.TruckerMSISDN, &booking.TerminalID, &booking.SlotID, &booking.Channel, &booking.Status,
		&booking.AmountKobo, &booking.Currency, &booking.PaymentReceiptRef, &booking.GateID,
		&booking.LedgerCommitHash, &booking.ReconciliationReason, &booking.CreatedAt, &booking.UpdatedAt,
		&booking.ExpiresAt, &booking.Version)
	return booking, err
}

// emit writes a FHIR-enveloped event into the transactional platform outbox
// inside the caller's transaction, guaranteeing atomicity with the mutation.
func emit(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, topic, eventType, correlationID, subjectID string, payload any, extensions map[string]string, principal Principal, ledgerCommitHash string, occurredAt time.Time) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s payload: %w", eventType, err)
	}
	envelope, err := events.Message(eventType, topic, correlationID, subjectID, payloadJSON, extensions, events.Provenance{
		PrincipalID:      principal.ID,
		PrincipalRole:    principal.Role,
		LedgerCommitHash: ledgerCommitHash,
	}, occurredAt)
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
		eventID, claims.TenantID, topic, eventType, envelope.EventID, envelopeJSON, occurredAt); err != nil {
		return fmt.Errorf("write %s outbox event: %w", eventType, err)
	}
	return nil
}

func (store *Store) CreateTerminal(ctx context.Context, terminalID, portCode, name string, bookingFeeKobo int64) error {
	if err := ValidateTerminalID(terminalID); err != nil {
		return err
	}
	if err := ValidatePortCode(portCode); err != nil {
		return err
	}
	if len(name) < 2 || len(name) > 256 || name != nameTrim(name) {
		return errors.New("terminal name must be canonical text between 2 and 256 characters")
	}
	if bookingFeeKobo <= 0 {
		return errors.New("terminal booking fee must be positive")
	}
	return store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO port_terminals (terminal_id, tenant_id, port_code, name, booking_fee_kobo)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (terminal_id) DO NOTHING`, terminalID, claims.TenantID, portCode, name, bookingFeeKobo); err != nil {
			return fmt.Errorf("register terminal: %w", err)
		}
		return nil
	})
}

func nameTrim(value string) string {
	start, end := 0, len(value)
	for start < end && value[start] == ' ' {
		start++
	}
	for end > start && value[end-1] == ' ' {
		end--
	}
	return value[start:end]
}

func (store *Store) CreateSlot(ctx context.Context, terminalID string, startsAt, endsAt time.Time, capacity int) (Slot, error) {
	if err := ValidateTerminalID(terminalID); err != nil {
		return Slot{}, err
	}
	if err := ValidateSlotWindow(startsAt, endsAt, capacity); err != nil {
		return Slot{}, err
	}
	var slot Slot
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		var active bool
		if err := tx.QueryRow(ctx, `SELECT active, port_code FROM port_terminals WHERE terminal_id=$1 FOR SHARE`, terminalID).Scan(&active, &slot.PortCode); errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		} else if err != nil {
			return fmt.Errorf("lock terminal for slot: %w", err)
		}
		if !active {
			return errors.New("terminal is not active")
		}
		created, err := scanSlot(tx.QueryRow(ctx, `
			INSERT INTO terminal_slots (slot_id, tenant_id, terminal_id, starts_at, ends_at, capacity)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (tenant_id, terminal_id, starts_at) DO NOTHING
			RETURNING slot_id, terminal_id, starts_at, ends_at, capacity, created_at`,
			uuid.New(), claims.TenantID, terminalID, startsAt.UTC(), endsAt.UTC(), capacity))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrIdempotencyConflict
		}
		if err != nil {
			return fmt.Errorf("create slot: %w", err)
		}
		created.PortCode = slot.PortCode
		slot = created
		return nil
	})
	return slot, err
}

func scanSlot(row pgx.Row) (Slot, error) {
	var slot Slot
	err := row.Scan(&slot.SlotID, &slot.TerminalID, &slot.StartsAt, &slot.EndsAt, &slot.Capacity, &slot.CreatedAt)
	return slot, err
}

// ListSlots returns slots overlapping [from, to) with their live reservation counts.
func (store *Store) ListSlots(ctx context.Context, terminalID string, from, to time.Time) ([]Slot, error) {
	var slots []Slot
	err := store.withTx(ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		rows, err := tx.Query(ctx, `
			SELECT s.slot_id, s.terminal_id, s.starts_at, s.ends_at, s.capacity, s.created_at,
				(SELECT count(*) FROM truck_bookings b WHERE b.slot_id = s.slot_id
					AND b.status IN ('SLOT_RESERVED','PAID','GATE_APPROVED')) AS reserved
			FROM terminal_slots s
			WHERE s.terminal_id = $1 AND s.starts_at < $3 AND s.ends_at > $2
			ORDER BY s.starts_at`, terminalID, from.UTC(), to.UTC())
		if err != nil {
			return fmt.Errorf("list slots: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var slot Slot
			if err := rows.Scan(&slot.SlotID, &slot.TerminalID, &slot.StartsAt, &slot.EndsAt, &slot.Capacity, &slot.CreatedAt, &slot.Reserved); err != nil {
				return fmt.Errorf("scan slot: %w", err)
			}
			slots = append(slots, slot)
		}
		return rows.Err()
	})
	return slots, err
}

// Create registers a per-truck booking. Offline-channel bookings are accepted
// into PENDING_SYNC and must be reconciled on reconnect; every other channel
// starts DRAFTED. Idempotent on (tenant, request_id).
func (store *Store) Create(ctx context.Context, request CreateRequest, principal Principal) (Booking, error) {
	if err := request.Validate(); err != nil {
		return Booking{}, err
	}
	if principal.ID == "" || principal.Role == "" {
		return Booking{}, errors.New("booking principal is required")
	}
	var booking Booking
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		created, err := store.createTx(ctx, tx, claims, request, principal)
		if err != nil {
			return err
		}
		booking = created
		return nil
	})
	return booking, err
}

// CreateTx runs booking creation inside the caller's tenant transaction. The
// queue package uses it to create the pending booking atomically with a queue
// request.
func (store *Store) CreateTx(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, request CreateRequest, principal Principal) (Booking, error) {
	return store.createTx(ctx, tx, claims, request, principal)
}

func (store *Store) createTx(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, request CreateRequest, principal Principal) (Booking, error) {
	var terminalActive bool
	if err := tx.QueryRow(ctx, `SELECT active FROM port_terminals WHERE terminal_id=$1 FOR SHARE`, request.TerminalID).Scan(&terminalActive); errors.Is(err, pgx.ErrNoRows) {
		return Booking{}, ErrNotFound
	} else if err != nil {
		return Booking{}, fmt.Errorf("lock terminal for booking: %w", err)
	}
	if !terminalActive {
		return Booking{}, errors.New("terminal is not active")
	}
	status := StatusDrafted
	if request.Channel == ChannelOffline {
		status = StatusPendingSync
	}
	now := time.Now().UTC()
	booking, err := scanBooking(tx.QueryRow(ctx, `
		INSERT INTO truck_bookings (
			booking_id, tenant_id, request_id, truck_plate, trucker_msisdn, terminal_id,
			channel, status, amount_kobo, currency, created_at, updated_at, expires_at, version
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'NGN',$10,$10,$11,1)
		ON CONFLICT (tenant_id, request_id) DO NOTHING
		RETURNING `+bookingColumns,
		uuid.New(), claims.TenantID, request.RequestID, request.TruckPlate, request.TruckerMSISDN,
		request.TerminalID, request.Channel, status, request.AmountKobo, now, request.ExpiresAt.UTC()))
	if errors.Is(err, pgx.ErrNoRows) {
		existing, lookupErr := scanBooking(tx.QueryRow(ctx, `SELECT `+bookingColumns+` FROM truck_bookings WHERE tenant_id=$1 AND request_id=$2 FOR UPDATE`, claims.TenantID, request.RequestID))
		if lookupErr != nil {
			return Booking{}, fmt.Errorf("lookup idempotent booking: %w", lookupErr)
		}
		if existing.TruckPlate != request.TruckPlate || existing.TruckerMSISDN != request.TruckerMSISDN ||
			existing.TerminalID != request.TerminalID || existing.Channel != request.Channel || existing.AmountKobo != request.AmountKobo {
			return Booking{}, ErrIdempotencyConflict
		}
		return existing, nil
	}
	if err != nil {
		return Booking{}, fmt.Errorf("insert booking: %w", err)
	}
	eventType := "booking.drafted"
	if status == StatusPendingSync {
		eventType = "booking.pending_sync"
	}
	if err := emit(ctx, tx, claims, events.TopicBooking, eventType, booking.RequestID, booking.BookingID, booking, map[string]string{
		"terminal-id": booking.TerminalID,
		"channel":     string(booking.Channel),
	}, principal, "", now); err != nil {
		return Booking{}, err
	}
	return booking, nil
}

func (store *Store) Get(ctx context.Context, bookingID string) (Booking, error) {
	var booking Booking
	err := store.withTx(ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		found, err := scanBooking(tx.QueryRow(ctx, `SELECT `+bookingColumns+` FROM truck_bookings WHERE booking_id=$1`, bookingID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("get booking: %w", err)
		}
		booking = found
		return nil
	})
	return booking, err
}

func (store *Store) getForUpdate(ctx context.Context, tx pgx.Tx, bookingID string) (Booking, error) {
	booking, err := scanBooking(tx.QueryRow(ctx, `SELECT `+bookingColumns+` FROM truck_bookings WHERE booking_id=$1 FOR UPDATE`, bookingID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Booking{}, ErrNotFound
	}
	if err != nil {
		return Booking{}, fmt.Errorf("lock booking: %w", err)
	}
	return booking, nil
}

func (store *Store) transitionTx(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, booking Booking, next Status, mutate func(*Booking), eventType string, extensions map[string]string, principal Principal, ledgerCommitHash string) (Booking, error) {
	if !ValidTransition(booking.Status, next) {
		return Booking{}, ErrInvalidTransition
	}
	updated := booking
	updated.Status = next
	updated.UpdatedAt = time.Now().UTC()
	updated.Version++
	if mutate != nil {
		mutate(&updated)
	}
	result, err := scanBooking(tx.QueryRow(ctx, `
		UPDATE truck_bookings
		SET status=$1, slot_id=$2, payment_receipt_ref=$3, gate_id=$4, ledger_commit_hash=$5,
			reconciliation_reason=$6, updated_at=$7, version=$8
		WHERE booking_id=$9 AND status=$10 AND version=$11
		RETURNING `+bookingColumns,
		updated.Status, updated.SlotID, updated.PaymentReceiptRef, updated.GateID, updated.LedgerCommitHash,
		updated.ReconciliationReason, updated.UpdatedAt, updated.Version, booking.BookingID, booking.Status, booking.Version))
	if errors.Is(err, pgx.ErrNoRows) {
		return Booking{}, ErrOptimisticConflict
	}
	if err != nil {
		return Booking{}, fmt.Errorf("transition booking to %s: %w", next, err)
	}
	if err := emit(ctx, tx, claims, events.TopicBooking, eventType, result.RequestID, result.BookingID, result, extensions, principal, ledgerCommitHash, result.UpdatedAt); err != nil {
		return Booking{}, err
	}
	return result, nil
}

// slotCapacityFree reports whether the slot can take one more active booking.
// The caller must hold FOR UPDATE on the terminal_slots row.
func slotCapacityFree(ctx context.Context, tx pgx.Tx, slotID string) (bool, Slot, error) {
	slot, err := scanSlot(tx.QueryRow(ctx, `SELECT slot_id, terminal_id, starts_at, ends_at, capacity, created_at FROM terminal_slots WHERE slot_id=$1 FOR UPDATE`, slotID))
	if errors.Is(err, pgx.ErrNoRows) {
		return false, Slot{}, ErrNotFound
	}
	if err != nil {
		return false, Slot{}, fmt.Errorf("lock slot: %w", err)
	}
	var active int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM truck_bookings WHERE slot_id=$1 AND status IN ('SLOT_RESERVED','PAID','GATE_APPROVED')`, slotID).Scan(&active); err != nil {
		return false, Slot{}, fmt.Errorf("count slot reservations: %w", err)
	}
	return active < slot.Capacity, slot, nil
}

// ReserveSlot moves DRAFTED -> SLOT_RESERVED with DB-enforced capacity. It is
// also the resolution path out of RECONCILIATION_REQUIRED.
func (store *Store) ReserveSlot(ctx context.Context, bookingID, slotID string, expectedVersion int64, principal Principal) (Booking, error) {
	var reserved Booking
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		booking, err := store.getForUpdate(ctx, tx, bookingID)
		if err != nil {
			return err
		}
		if booking.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		if booking.Status != StatusDrafted && booking.Status != StatusReconciliationRequired {
			return ErrInvalidTransition
		}
		free, slot, err := slotCapacityFree(ctx, tx, slotID)
		if err != nil {
			return err
		}
		if slot.TerminalID != booking.TerminalID {
			return ErrSlotWindow
		}
		if !time.Now().UTC().Before(slot.EndsAt) {
			return ErrSlotWindow
		}
		if !free {
			return ErrSlotUnavailable
		}
		result, err := store.transitionTx(ctx, tx, claims, booking, StatusSlotReserved, func(updated *Booking) {
			updated.SlotID = &slotID
			updated.ReconciliationReason = nil
		}, "booking.slot_reserved", map[string]string{
			"slot-id":     slotID,
			"terminal-id": booking.TerminalID,
		}, principal, "")
		if err != nil {
			return err
		}
		reserved = result
		return nil
	})
	return reserved, err
}

// Reconcile processes an offline PENDING_SYNC booking on reconnect against the
// requested slot. Capacity conflicts move the booking to
// RECONCILIATION_REQUIRED with a recorded reason — never a silent drop.
func (store *Store) Reconcile(ctx context.Context, bookingID, slotID string, expectedVersion int64, principal Principal) (Booking, error) {
	var reconciled Booking
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		booking, err := store.getForUpdate(ctx, tx, bookingID)
		if err != nil {
			return err
		}
		if booking.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		if booking.Status != StatusPendingSync {
			return ErrInvalidTransition
		}
		free, slot, err := slotCapacityFree(ctx, tx, slotID)
		if err != nil {
			return err
		}
		if slot.TerminalID != booking.TerminalID || !time.Now().UTC().Before(slot.EndsAt) || !free {
			reason := "slot capacity exhausted on reconnect"
			if slot.TerminalID != booking.TerminalID {
				reason = "slot terminal mismatch on reconnect"
			} else if !time.Now().UTC().Before(slot.EndsAt) {
				reason = "slot window elapsed on reconnect"
			}
			result, err := store.transitionTx(ctx, tx, claims, booking, StatusReconciliationRequired, func(updated *Booking) {
				updated.ReconciliationReason = &reason
			}, "booking.reconciliation_required", map[string]string{
				"slot-id": slotID,
				"reason":  reason,
			}, principal, "")
			if err != nil {
				return err
			}
			reconciled = result
			return nil
		}
		result, err := store.transitionTx(ctx, tx, claims, booking, StatusSlotReserved, func(updated *Booking) {
			updated.SlotID = &slotID
			updated.ReconciliationReason = nil
		}, "booking.reconciled", map[string]string{
			"slot-id":     slotID,
			"terminal-id": booking.TerminalID,
		}, principal, "")
		if err != nil {
			return err
		}
		reconciled = result
		return nil
	})
	return reconciled, err
}

// CreatePaymentIntent records an idempotent Mojaloop NGN payment intent for a
// SLOT_RESERVED booking. The mojaloopTxRef must come from a real switch
// interaction performed by the caller through the payments gateway.
func (store *Store) CreatePaymentIntent(ctx context.Context, bookingID, requestID, mojaloopTxRef string, expectedVersion int64) (PaymentIntent, error) {
	if len(requestID) < 8 || len(requestID) > 128 {
		return PaymentIntent{}, errors.New("payment request_id must be between 8 and 128 characters")
	}
	if mojaloopTxRef == "" {
		return PaymentIntent{}, ErrPaymentInvalid
	}
	var intent PaymentIntent
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		booking, err := store.getForUpdate(ctx, tx, bookingID)
		if err != nil {
			return err
		}
		if booking.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		if booking.Status != StatusSlotReserved {
			return ErrInvalidTransition
		}
		now := time.Now().UTC()
		row := tx.QueryRow(ctx, `
			INSERT INTO booking_payment_intents (intent_id, tenant_id, booking_id, request_id, amount_kobo, currency, mojaloop_tx_ref, status, created_at)
			VALUES ($1,$2,$3,$4,$5,'NGN',$6,'REQUESTED',$7)
			ON CONFLICT (tenant_id, request_id) DO NOTHING
			RETURNING intent_id, booking_id, request_id, amount_kobo, currency, mojaloop_tx_ref, status, created_at`,
			uuid.New(), claims.TenantID, bookingID, requestID, booking.AmountKobo, mojaloopTxRef, now)
		if err := row.Scan(&intent.IntentID, &intent.BookingID, &intent.RequestID, &intent.AmountKobo, &intent.Currency, &intent.MojaloopTxRef, &intent.Status, &intent.CreatedAt); errors.Is(err, pgx.ErrNoRows) {
			var existingTxRef, existingBooking string
			if lookupErr := tx.QueryRow(ctx, `SELECT booking_id, mojaloop_tx_ref FROM booking_payment_intents WHERE tenant_id=$1 AND request_id=$2`, claims.TenantID, requestID).Scan(&existingBooking, &existingTxRef); lookupErr != nil {
				return fmt.Errorf("lookup idempotent payment intent: %w", lookupErr)
			}
			if existingBooking != bookingID || existingTxRef != mojaloopTxRef {
				return ErrIdempotencyConflict
			}
			return scanPaymentIntent(tx.QueryRow(ctx, `SELECT intent_id, booking_id, request_id, amount_kobo, currency, mojaloop_tx_ref, status, created_at FROM booking_payment_intents WHERE tenant_id=$1 AND request_id=$2`, claims.TenantID, requestID), &intent)
		} else if err != nil {
			return fmt.Errorf("insert payment intent: %w", err)
		}
		return nil
	})
	return intent, err
}

func scanPaymentIntent(row pgx.Row, intent *PaymentIntent) error {
	return row.Scan(&intent.IntentID, &intent.BookingID, &intent.RequestID, &intent.AmountKobo, &intent.Currency, &intent.MojaloopTxRef, &intent.Status, &intent.CreatedAt)
}

// ConfirmPayment moves SLOT_RESERVED -> PAID against a Mojaloop receipt
// reference produced by the payment switch.
func (store *Store) ConfirmPayment(ctx context.Context, bookingID, receiptRef string, expectedVersion int64, principal Principal) (Booking, error) {
	if receiptRef == "" || len(receiptRef) > 128 {
		return Booking{}, ErrPaymentInvalid
	}
	var paid Booking
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		booking, err := store.getForUpdate(ctx, tx, bookingID)
		if err != nil {
			return err
		}
		if booking.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		if booking.Status != StatusSlotReserved {
			return ErrInvalidTransition
		}
		var pendingIntents int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM booking_payment_intents WHERE booking_id=$1 AND status='REQUESTED'`, bookingID).Scan(&pendingIntents); err != nil {
			return fmt.Errorf("check payment intents: %w", err)
		}
		if pendingIntents == 0 {
			return ErrPaymentInvalid
		}
		if _, err := tx.Exec(ctx, `UPDATE booking_payment_intents SET status='COMPLETED' WHERE booking_id=$1 AND status='REQUESTED'`, bookingID); err != nil {
			return fmt.Errorf("complete payment intents: %w", err)
		}
		result, err := store.transitionTx(ctx, tx, claims, booking, StatusPaid, func(updated *Booking) {
			updated.PaymentReceiptRef = &receiptRef
		}, "booking.paid", map[string]string{
			"payment-receipt-ref": receiptRef,
			"amount-kobo":         fmt.Sprintf("%d", booking.AmountKobo),
		}, principal, "")
		if err != nil {
			return err
		}
		paid = result
		return nil
	})
	return paid, err
}

// RecordGateScan is the gate controller check: a scan is approved only when the
// booking is PAID, carries a payment receipt, the assigned slot exists and the
// scan happens inside the slot window. Every scan — approved or denied — is
// persisted for audit, and approvals emit ports.gate.v1.
func (store *Store) RecordGateScan(ctx context.Context, bookingID, gateID, scannedBy string, scannedAt time.Time, principal Principal) (GateScan, Booking, error) {
	if len(gateID) < 2 || len(gateID) > 64 || scannedBy == "" || len(scannedBy) > 256 {
		return GateScan{}, Booking{}, ErrGateDenied
	}
	var scan GateScan
	var updated Booking
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		booking, err := store.getForUpdate(ctx, tx, bookingID)
		if err != nil {
			return err
		}
		scan = GateScan{
			ScanID:    uuid.NewString(),
			BookingID: bookingID,
			GateID:    gateID,
			ScannedBy: scannedBy,
			ScannedAt: scannedAt.UTC(),
			Decision:  "DENIED",
		}
		deny := func(reason string) error {
			scan.DenialReason = &reason
			if _, err := tx.Exec(ctx, `
				INSERT INTO gate_scans (scan_id, tenant_id, booking_id, gate_id, scanned_by, decision, denial_reason, scanned_at)
				VALUES ($1,$2,$3,$4,$5,'DENIED',$6,$7)`,
				scan.ScanID, claims.TenantID, bookingID, gateID, scannedBy, reason, scan.ScannedAt); err != nil {
				return fmt.Errorf("persist denied gate scan: %w", err)
			}
			if err := emit(ctx, tx, claims, events.TopicGate, "gate.scan_denied", booking.RequestID, bookingID, scan, map[string]string{
				"gate-id": gateID,
				"reason":  reason,
			}, principal, "", scan.ScannedAt); err != nil {
				return err
			}
			updated = booking
			return ErrGateDenied
		}
		if booking.Status != StatusPaid {
			return deny("booking is not in PAID state")
		}
		if booking.PaymentReceiptRef == nil {
			return deny("payment receipt is missing")
		}
		if booking.SlotID == nil {
			return deny("no slot is assigned to this booking")
		}
		var startsAt, endsAt time.Time
		if err := tx.QueryRow(ctx, `SELECT starts_at, ends_at FROM terminal_slots WHERE slot_id=$1 FOR SHARE`, *booking.SlotID).Scan(&startsAt, &endsAt); err != nil {
			return deny("assigned slot is unavailable")
		}
		if scan.ScannedAt.Before(startsAt) || scan.ScannedAt.After(endsAt) {
			return deny("scan is outside the booked slot window")
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO gate_scans (scan_id, tenant_id, booking_id, gate_id, scanned_by, decision, denial_reason, scanned_at)
			VALUES ($1,$2,$3,$4,$5,'APPROVED',NULL,$6)`,
			scan.ScanID, claims.TenantID, bookingID, gateID, scannedBy, scan.ScannedAt); err != nil {
			return fmt.Errorf("persist approved gate scan: %w", err)
		}
		scan.Decision = "APPROVED"
		result, err := store.transitionTx(ctx, tx, claims, booking, StatusGateApproved, func(updated *Booking) {
			updated.GateID = &gateID
		}, "booking.gate_approved", map[string]string{
			"gate-id": gateID,
			"scan-id": scan.ScanID,
		}, principal, "")
		if err != nil {
			return err
		}
		if err := emit(ctx, tx, claims, events.TopicGate, "gate.scan_approved", booking.RequestID, bookingID, scan, map[string]string{
			"gate-id":    gateID,
			"booking-id": bookingID,
		}, principal, "", scan.ScannedAt); err != nil {
			return err
		}
		updated = result
		return nil
	})
	return scan, updated, err
}

// Complete moves GATE_APPROVED -> COMPLETED and records the TigerBeetle ledger
// commit hash for the audit trail.
func (store *Store) Complete(ctx context.Context, bookingID string, expectedVersion int64, ledgerCommitHash string, principal Principal) (Booking, error) {
	if ledgerCommitHash == "" {
		return Booking{}, errors.New("ledger commit hash is required to complete a booking")
	}
	var completed Booking
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		booking, err := store.getForUpdate(ctx, tx, bookingID)
		if err != nil {
			return err
		}
		if booking.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		result, err := store.transitionTx(ctx, tx, claims, booking, StatusCompleted, func(updated *Booking) {
			updated.LedgerCommitHash = &ledgerCommitHash
		}, "booking.completed", map[string]string{
			"ledger-commit-hash": ledgerCommitHash,
		}, principal, ledgerCommitHash)
		if err != nil {
			return err
		}
		if err := store.capacityReleased(ctx, tx, claims, booking, principal); err != nil {
			return err
		}
		completed = result
		return nil
	})
	return completed, err
}

// Cancel moves any non-terminal booking into CANCELLED.
func (store *Store) Cancel(ctx context.Context, bookingID string, expectedVersion int64, reason string, principal Principal) (Booking, error) {
	if reason == "" || len(reason) > 1024 {
		return Booking{}, errors.New("cancellation reason is required")
	}
	var cancelled Booking
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		booking, err := store.getForUpdate(ctx, tx, bookingID)
		if err != nil {
			return err
		}
		if booking.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		result, err := store.transitionTx(ctx, tx, claims, booking, StatusCancelled, func(updated *Booking) {
			updated.ReconciliationReason = &reason
		}, "booking.cancelled", map[string]string{"reason": reason}, principal, "")
		if err != nil {
			return err
		}
		if err := store.capacityReleased(ctx, tx, claims, booking, principal); err != nil {
			return err
		}
		cancelled = result
		return nil
	})
	return cancelled, err
}

// ExpireDue sweeps reserved/paid bookings whose validity elapsed into EXPIRED.
func (store *Store) ExpireDue(ctx context.Context, now time.Time, principal Principal) (int, error) {
	count := 0
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		rows, err := tx.Query(ctx, `SELECT `+bookingColumns+` FROM truck_bookings
			WHERE status IN ('SLOT_RESERVED','PAID') AND expires_at < $1 FOR UPDATE SKIP LOCKED`, now.UTC())
		if err != nil {
			return fmt.Errorf("find expirable bookings: %w", err)
		}
		var due []Booking
		for rows.Next() {
			booking, err := scanBooking(rows)
			if err != nil {
				rows.Close()
				return fmt.Errorf("scan expirable booking: %w", err)
			}
			due = append(due, booking)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, booking := range due {
			if _, err := store.transitionTx(ctx, tx, claims, booking, StatusExpired, nil, "booking.expired", nil, principal, ""); err != nil {
				return err
			}
			if err := store.capacityReleased(ctx, tx, claims, booking, principal); err != nil {
				return err
			}
			count++
		}
		return nil
	})
	return count, err
}
