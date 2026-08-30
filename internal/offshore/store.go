package offshore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tariff"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantdb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Principal is the verified actor behind an offshore mutation; it becomes
// the provenance block of every emitted platform event.
type Principal struct {
	ID   string
	Role string
}

// tracer returns the offshore tracer. With telemetry disabled the global
// provider is a no-op: spans are non-recording and the workflow semantics
// are unchanged.
func tracer() trace.Tracer {
	return otel.Tracer("github.com/munisp/blueeconomy-port-interoperability/internal/offshore")
}

// Store is the tenant-scoped offshore terminal-call repository. Every
// method runs inside tenantdb.WithTx (RLS isolation); lifecycle events are
// JWS-signed into the platform outbox in the same transaction as the
// mutation (exactly-once emission).
type Store struct {
	pool   *pgxpool.Pool
	signer *events.Signer
}

// NewStore builds the offshore store. The envelope signer is mandatory —
// an unsigned event pipeline fails closed at the emit site.
func NewStore(pool *pgxpool.Pool, signer *events.Signer) *Store {
	return &Store{pool: pool, signer: signer}
}

func Open(ctx context.Context, databaseURL string, signer *events.Signer) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return NewStore(pool, signer), nil
}

func (store *Store) Close() { store.pool.Close() }

// Pool exposes the pool for test harnesses and infrastructure adapters.
func (store *Store) Pool() *pgxpool.Pool { return store.pool }

func (store *Store) withTx(ctx context.Context, work func(pgx.Tx, tenantctx.Claims) error) error {
	return tenantdb.WithTx(ctx, store.pool, work)
}

// emit writes a FHIR-enveloped, JWS-signed event into the platform outbox
// inside the caller's transaction.
func emit(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, eventType, correlationID, subjectID string, payload any, extensions map[string]string, principal Principal, occurredAt time.Time, signer *events.Signer) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s payload: %w", eventType, err)
	}
	envelope, err := events.Message(eventType, events.TopicOffshore, correlationID, subjectID, payloadJSON, extensions, events.Provenance{
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
		eventID, claims.TenantID, events.TopicOffshore, eventType, envelope.EventID, envelopeJSON, occurredAt); err != nil {
		return fmt.Errorf("write %s outbox event: %w", eventType, err)
	}
	return nil
}

const callColumns = `call_id, vessel_imo, vessel_name, terminal_code, terminal_kind, buoy_id, agency_code,
	gross_tonnage, cargo_tonnes, mooring_window_start, mooring_window_end, nominated_by,
	status, mooring_master, created_at, updated_at, version`

func scanCall(row pgx.Row) (Call, error) {
	var call Call
	err := row.Scan(&call.CallID, &call.VesselIMO, &call.VesselName, &call.TerminalCode, &call.TerminalKind,
		&call.BuoyID, &call.AgencyCode, &call.GrossTonnage, &call.CargoTonnes,
		&call.MooringWindowStart, &call.MooringWindowEnd, &call.NominatedBy,
		&call.Status, &call.MooringMaster, &call.CreatedAt, &call.UpdatedAt, &call.Version)
	return call, err
}

// Create registers an offshore terminal call idempotently: an exact replay
// returns the retained call, a conflicting reuse of the key fails closed.
func (store *Store) Create(ctx context.Context, idempotencyKey string, request CreateRequest, principal Principal) (Call, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return Call{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if err := request.Validate(); err != nil {
		return Call{}, err
	}
	if principal.ID == "" || principal.Role == "" {
		return Call{}, errors.New("a verified principal is required")
	}
	ctx, span := tracer().Start(ctx, "offshore.nominate", trace.WithAttributes(
		attribute.String("offshore.call_id", request.CallID),
		attribute.String("offshore.terminal_code", request.TerminalCode),
		attribute.String("agency", request.AgencyCode),
	))
	defer span.End()
	var call Call
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		span.SetAttributes(attribute.String("tenant.id", claims.TenantID))
		created, err := store.createTx(ctx, tx, claims, idempotencyKey, request, principal)
		if err != nil {
			return err
		}
		call = created
		return nil
	})
	if err != nil {
		span.RecordError(err)
	}
	return call, err
}

func (store *Store) createTx(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, idempotencyKey string, request CreateRequest, principal Principal) (Call, error) {
	createdAt := time.Now().UTC()
	call, err := scanCall(tx.QueryRow(ctx, `
		INSERT INTO offshore_terminal_calls (
			call_id, tenant_id, idempotency_key, vessel_imo, vessel_name, terminal_code, terminal_kind,
			buoy_id, agency_code, gross_tonnage, cargo_tonnes, mooring_window_start, mooring_window_end,
			nominated_by, status, created_at, updated_at, version
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $16, 1)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+callColumns,
		request.CallID, claims.TenantID, idempotencyKey, request.VesselIMO, request.VesselName,
		request.TerminalCode, request.TerminalKind, request.BuoyID, request.AgencyCode,
		request.GrossTonnage, request.CargoTonnes, request.MooringWindowStart.UTC(), request.MooringWindowEnd.UTC(),
		request.NominatedBy, StatusNominated, createdAt))
	if errors.Is(err, pgx.ErrNoRows) {
		retained, lookupErr := store.getForUpdate(ctx, tx, `idempotency_key`, idempotencyKey)
		if lookupErr != nil {
			return Call{}, fmt.Errorf("lookup idempotent offshore call: %w", lookupErr)
		}
		if !retained.Matches(request) {
			return Call{}, ErrIdempotencyConflict
		}
		return retained, nil
	}
	if err != nil {
		return Call{}, fmt.Errorf("insert offshore call: %w", err)
	}
	if err := emit(ctx, tx, claims, "offshore.call_nominated", idempotencyKey, call.CallID, call, map[string]string{
		"terminal-code": call.TerminalCode,
		"terminal-kind": string(call.TerminalKind),
		"buoy-id":       call.BuoyID,
		"agency":        call.AgencyCode,
	}, principal, createdAt, store.signer); err != nil {
		return Call{}, err
	}
	return call, nil
}

func (store *Store) getForUpdate(ctx context.Context, tx pgx.Tx, keyColumn, keyValue string) (Call, error) {
	var column string
	switch keyColumn {
	case "call_id", "idempotency_key":
		column = keyColumn
	default:
		return Call{}, errors.New("unsupported lookup column")
	}
	call, err := scanCall(tx.QueryRow(ctx, `SELECT `+callColumns+` FROM offshore_terminal_calls WHERE `+column+` = $1 FOR UPDATE`, keyValue))
	if errors.Is(err, pgx.ErrNoRows) {
		return Call{}, ErrNotFound
	}
	if err != nil {
		return Call{}, fmt.Errorf("load offshore call: %w", err)
	}
	return call, nil
}

// Get returns a call by id.
func (store *Store) Get(ctx context.Context, callID string) (Call, error) {
	if !callIDPattern.MatchString(callID) {
		return Call{}, ErrNotFound
	}
	var call Call
	err := store.withTx(ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		found, err := scanCall(tx.QueryRow(ctx, `SELECT `+callColumns+` FROM offshore_terminal_calls WHERE call_id = $1`, callID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("get offshore call: %w", err)
		}
		call = found
		return nil
	})
	return call, err
}

// Transition advances the mooring-master workflow with optimistic
// concurrency. Mooring requires the named mooring master on the MOORED
// transition; every transition is signed into the outbox atomically.
func (store *Store) Transition(ctx context.Context, callID string, expectedVersion int64, next Status, mooringMaster string, principal Principal) (Call, error) {
	if !callIDPattern.MatchString(callID) || expectedVersion < 1 {
		return Call{}, ErrNotFound
	}
	if next == StatusMoored && !canonical(mooringMaster, 2, 256) {
		return Call{}, fmt.Errorf("%w: mooring to MOORED requires the named mooring master", ErrInvalidTransition)
	}
	if next != StatusMoored && mooringMaster != "" {
		return Call{}, fmt.Errorf("%w: mooring master applies only to the MOORED transition", ErrInvalidTransition)
	}
	if principal.ID == "" || principal.Role == "" {
		return Call{}, errors.New("a verified principal is required")
	}
	ctx, span := tracer().Start(ctx, "offshore.mooring_transition", trace.WithAttributes(
		attribute.String("offshore.call_id", callID),
		attribute.String("offshore.transition.to", string(next)),
	))
	defer span.End()
	var updated Call
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		current, err := store.getForUpdate(ctx, tx, "call_id", callID)
		if err != nil {
			return err
		}
		span.SetAttributes(
			attribute.String("tenant.id", claims.TenantID),
			attribute.String("agency", current.AgencyCode),
			attribute.String("offshore.transition.from", string(current.Status)),
		)
		if current.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		if !ValidTransition(current.Status, next) {
			return ErrInvalidTransition
		}
		updatedAt := time.Now().UTC()
		master := current.MooringMaster
		if next == StatusMoored {
			master = &mooringMaster
		}
		updated, err = scanCall(tx.QueryRow(ctx, `
			UPDATE offshore_terminal_calls
			SET status = $1, mooring_master = $2, updated_at = $3, version = version + 1
			WHERE call_id = $4 AND status = $5 AND version = $6
			RETURNING `+callColumns, next, master, updatedAt, callID, current.Status, expectedVersion))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrOptimisticConflict
		}
		if err != nil {
			return fmt.Errorf("transition offshore call: %w", err)
		}
		return emit(ctx, tx, claims, "offshore.call_status_changed", fmt.Sprintf("%s:%d", callID, updated.Version), callID, updated, map[string]string{
			"from-status": string(current.Status),
			"to-status":   string(next),
			"agency":      current.AgencyCode,
		}, principal, updatedAt, store.signer)
	})
	if err != nil {
		span.RecordError(err)
	}
	return updated, err
}

// EventRequest records one append-only operational event.
type EventRequest struct {
	EventType  EventType `json:"event_type"`
	RecordedBy string    `json:"recorded_by"`
	Remarks    string    `json:"remarks"`
	// Custody-transfer metering fields; required for CUSTODY_METER_READING,
	// prohibited otherwise. Decimal literals, at most 3 fractional digits.
	MeterID        *string `json:"meter_id,omitempty"`
	MeterOpeningM3 *string `json:"meter_opening_m3,omitempty"`
	MeterClosingM3 *string `json:"meter_closing_m3,omitempty"`
}

var meterReadingPattern = regexp.MustCompile(`^[0-9]{1,15}(\.[0-9]{1,3})?$`)

func (request EventRequest) validate() error {
	switch request.EventType {
	case EventHoseConnection, EventLoadingArmStart, EventLoadingArmStop, EventCustodyMeterReading, EventMooringNote:
	default:
		return fmt.Errorf("%w: unknown event type %q", ErrEventRejected, request.EventType)
	}
	if !canonical(request.RecordedBy, 2, 256) {
		return fmt.Errorf("%w: recorded_by must be canonical text", ErrEventRejected)
	}
	if len(request.Remarks) > 1024 || strings.TrimSpace(request.Remarks) != request.Remarks {
		return fmt.Errorf("%w: remarks must be canonical text of at most 1024 characters", ErrEventRejected)
	}
	if request.EventType == EventCustodyMeterReading {
		if request.MeterID == nil || !canonical(*request.MeterID, 1, 64) ||
			request.MeterOpeningM3 == nil || !meterReadingPattern.MatchString(*request.MeterOpeningM3) ||
			request.MeterClosingM3 == nil || !meterReadingPattern.MatchString(*request.MeterClosingM3) {
			return fmt.Errorf("%w: custody meter readings require meter id and opening/closing readings", ErrEventRejected)
		}
		opening, okOpening := new(big.Float).SetString(*request.MeterOpeningM3)
		closing, okClosing := new(big.Float).SetString(*request.MeterClosingM3)
		if !okOpening || !okClosing || closing.Cmp(opening) < 0 {
			return fmt.Errorf("%w: closing reading must be at or above the opening reading", ErrEventRejected)
		}
		return nil
	}
	if request.MeterID != nil || request.MeterOpeningM3 != nil || request.MeterClosingM3 != nil {
		return fmt.Errorf("%w: metering fields apply only to custody meter readings", ErrEventRejected)
	}
	return nil
}

// RecordEvent appends an operational event after validating it against the
// current workflow state. Rejected events are errors — nothing is silently
// dropped.
func (store *Store) RecordEvent(ctx context.Context, callID string, request EventRequest, principal Principal) (OperationalEvent, error) {
	if !callIDPattern.MatchString(callID) {
		return OperationalEvent{}, ErrNotFound
	}
	if err := request.validate(); err != nil {
		return OperationalEvent{}, err
	}
	if principal.ID == "" || principal.Role == "" {
		return OperationalEvent{}, errors.New("a verified principal is required")
	}
	var event OperationalEvent
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		call, err := store.getForUpdate(ctx, tx, "call_id", callID)
		if err != nil {
			return err
		}
		if !EventAllowed(call.Status, request.EventType) {
			return fmt.Errorf("%w: %s is not recordable while %s", ErrEventRejected, request.EventType, call.Status)
		}
		recordedAt := time.Now().UTC()
		eventID := uuid.New()
		err = tx.QueryRow(ctx, `
			INSERT INTO offshore_call_events (event_id, tenant_id, call_id, event_type, recorded_by, recorded_at, remarks, meter_id, meter_opening_m3, meter_closing_m3)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::numeric, $10::numeric)
			RETURNING event_id, call_id, event_type, recorded_by, recorded_at, remarks, meter_id, meter_opening_m3::text, meter_closing_m3::text`,
			eventID, claims.TenantID, callID, request.EventType, request.RecordedBy, recordedAt, request.Remarks,
			request.MeterID, request.MeterOpeningM3, request.MeterClosingM3).
			Scan(&event.EventID, &event.CallID, &event.EventType, &event.RecordedBy, &event.RecordedAt, &event.Remarks,
				&event.MeterID, &event.MeterOpeningM3, &event.MeterClosingM3)
		if err != nil {
			return fmt.Errorf("insert offshore call event: %w", err)
		}
		return emit(ctx, tx, claims, "offshore.call_event_recorded", event.EventID, callID, event, map[string]string{
			"event-type": string(request.EventType),
			"agency":     call.AgencyCode,
		}, principal, recordedAt, store.signer)
	})
	return event, err
}

// ListEvents returns the append-only event trail of a call in order.
func (store *Store) ListEvents(ctx context.Context, callID string) ([]OperationalEvent, error) {
	if !callIDPattern.MatchString(callID) {
		return nil, ErrNotFound
	}
	var eventsOut []OperationalEvent
	err := store.withTx(ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		rows, err := tx.Query(ctx, `
			SELECT event_id, call_id, event_type, recorded_by, recorded_at, remarks, meter_id, meter_opening_m3::text, meter_closing_m3::text
			FROM offshore_call_events WHERE call_id = $1 ORDER BY event_seq`, callID)
		if err != nil {
			return fmt.Errorf("list offshore call events: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var event OperationalEvent
			if err := rows.Scan(&event.EventID, &event.CallID, &event.EventType, &event.RecordedBy, &event.RecordedAt, &event.Remarks,
				&event.MeterID, &event.MeterOpeningM3, &event.MeterClosingM3); err != nil {
				return fmt.Errorf("scan offshore call event: %w", err)
			}
			eventsOut = append(eventsOut, event)
		}
		return rows.Err()
	})
	return eventsOut, err
}

// Assess computes the terminal fee against the versioned tariff schedule
// effective at asOf and records the revenue assessment exactly once under
// the idempotency key: an identical replay returns the retained assessment
// (no duplicate emission); a conflicting reuse fails closed.
func (store *Store) Assess(ctx context.Context, callID, idempotencyKey string, asOf time.Time, principal Principal) (tariff.Assessment, error) {
	if !callIDPattern.MatchString(callID) {
		return tariff.Assessment{}, ErrNotFound
	}
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return tariff.Assessment{}, errors.New("assessment idempotency key must be non-empty and at most 256 characters")
	}
	if asOf.IsZero() {
		return tariff.Assessment{}, errors.New("assessment as-of instant is required")
	}
	if principal.ID == "" || principal.Role == "" {
		return tariff.Assessment{}, errors.New("a verified principal is required")
	}
	ctx, span := tracer().Start(ctx, "offshore.dues_assessment", trace.WithAttributes(
		attribute.String("offshore.call_id", callID),
	))
	defer span.End()
	var assessment tariff.Assessment
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		call, err := store.getForUpdate(ctx, tx, "call_id", callID)
		if err != nil {
			return err
		}
		span.SetAttributes(attribute.String("tenant.id", claims.TenantID), attribute.String("agency", call.AgencyCode))
		schedule, err := tariff.LoadEffective(ctx, tx, tariff.DomainOffshoreTerminal, asOf)
		if err != nil {
			return err
		}
		facts := tariff.Facts{GrossTonnage: call.GrossTonnage, CargoTonnes: call.CargoTonnes}
		items, total, err := tariff.Compute(schedule, facts, asOf)
		if err != nil {
			return err
		}
		span.SetAttributes(
			attribute.String("tariff.schedule_id", schedule.ScheduleID),
			attribute.Int64("assessment.total_minor", total),
		)
		recorded, err := tariff.RecordAssessmentTx(ctx, tx, claims, store.signer, tariff.AssessmentParams{
			IdempotencyKey: idempotencyKey,
			Domain:         tariff.DomainOffshoreTerminal,
			CallReference:  callID,
			Schedule:       schedule,
			AsOf:           asOf,
			Facts:          facts,
			LineItems:      items,
			TotalMinor:     total,
			Principal:      tariff.Principal{ID: principal.ID, Role: principal.Role},
		})
		if err != nil {
			return err
		}
		assessment = recorded
		return nil
	})
	if err != nil {
		span.RecordError(err)
	}
	return assessment, err
}
