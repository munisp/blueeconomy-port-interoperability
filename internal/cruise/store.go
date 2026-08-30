package cruise

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
	"github.com/munisp/blueeconomy-port-interoperability/internal/tariff"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantdb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Principal is the verified actor behind a cruise mutation; it becomes the
// provenance block of every emitted platform event.
type Principal struct {
	ID   string
	Role string
}

// tracer returns the cruise tracer. With telemetry disabled the global
// provider is a no-op: spans are non-recording and workflow semantics are
// unchanged.
func tracer() trace.Tracer {
	return otel.Tracer("github.com/munisp/blueeconomy-port-interoperability/internal/cruise")
}

// Store is the tenant-scoped cruise call repository. Every method runs
// inside tenantdb.WithTx (RLS isolation); lifecycle events are JWS-signed
// into the platform outbox in the same transaction as the mutation.
type Store struct {
	pool   *pgxpool.Pool
	signer *events.Signer
}

// NewStore builds the cruise store. The envelope signer is mandatory — an
// unsigned event pipeline fails closed at the emit site.
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

// emit writes a FHIR-enveloped, JWS-signed event into the platform outbox
// inside the caller's transaction.
func emit(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, eventType, correlationID, subjectID string, payload any, extensions map[string]string, principal Principal, occurredAt time.Time, signer *events.Signer) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s payload: %w", eventType, err)
	}
	envelope, err := events.Message(eventType, events.TopicCruise, correlationID, subjectID, payloadJSON, extensions, events.Provenance{
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
		eventID, claims.TenantID, events.TopicCruise, eventType, envelope.EventID, envelopeJSON, occurredAt); err != nil {
		return fmt.Errorf("write %s outbox event: %w", eventType, err)
	}
	return nil
}

const callColumns = `call_id, port_call_id, cruise_line, vessel_name, pax_count, created_by, pax_band, status, created_at, updated_at, version`

func scanCall(row pgx.Row) (Call, error) {
	var call Call
	err := row.Scan(&call.CallID, &call.PortCallID, &call.CruiseLine, &call.VesselName, &call.PaxCount,
		&call.CreatedBy, &call.PaxBand, &call.Status, &call.CreatedAt, &call.UpdatedAt, &call.Version)
	return call, err
}

// Create registers a cruise call over an existing port call, idempotently.
// The referenced port call must be visible to the tenant (RLS) — a cruise
// call never extends another tenant's port call.
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
	ctx, span := tracer().Start(ctx, "cruise.register_call", trace.WithAttributes(
		attribute.String("cruise.call_id", request.CallID),
		attribute.String("cruise.pax_band", string(BandFor(request.PaxCount))),
	))
	defer span.End()
	var call Call
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		span.SetAttributes(attribute.String("tenant.id", claims.TenantID))
		var visible string
		if err := tx.QueryRow(ctx, `SELECT call_id FROM port_calls WHERE call_id = $1 FOR SHARE`, request.PortCallID).Scan(&visible); errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: port call %s", ErrNotFound, request.PortCallID)
		} else if err != nil {
			return fmt.Errorf("verify port call: %w", err)
		}
		createdAt := time.Now().UTC()
		created, err := scanCall(tx.QueryRow(ctx, `
			INSERT INTO cruise_calls (call_id, tenant_id, idempotency_key, port_call_id, cruise_line, vessel_name, pax_count, pax_band, status, created_by, created_at, updated_at, version)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11, 1)
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING `+callColumns,
			request.CallID, claims.TenantID, idempotencyKey, request.PortCallID, request.CruiseLine,
			request.VesselName, request.PaxCount, BandFor(request.PaxCount), StatusPlanned, request.CreatedBy, createdAt))
		if errors.Is(err, pgx.ErrNoRows) {
			retained, lookupErr := scanCall(tx.QueryRow(ctx, `SELECT `+callColumns+` FROM cruise_calls WHERE idempotency_key = $1 FOR UPDATE`, idempotencyKey))
			if lookupErr != nil {
				return fmt.Errorf("lookup idempotent cruise call: %w", lookupErr)
			}
			if !retained.Matches(request) {
				return ErrIdempotencyConflict
			}
			call = retained
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert cruise call: %w", err)
		}
		call = created
		return emit(ctx, tx, claims, "cruise.call_registered", idempotencyKey, call.CallID, call, map[string]string{
			"pax-band":     string(call.PaxBand),
			"port-call-id": call.PortCallID,
		}, principal, createdAt, store.signer)
	})
	if err != nil {
		span.RecordError(err)
	}
	return call, err
}

// Get returns a cruise call by id.
func (store *Store) Get(ctx context.Context, callID string) (Call, error) {
	if !callIDPattern.MatchString(callID) {
		return Call{}, ErrNotFound
	}
	var call Call
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, _ tenantctx.Claims) error {
		found, err := scanCall(tx.QueryRow(ctx, `SELECT `+callColumns+` FROM cruise_calls WHERE call_id = $1`, callID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("get cruise call: %w", err)
		}
		call = found
		return nil
	})
	return call, err
}

func (store *Store) getForUpdate(ctx context.Context, tx pgx.Tx, callID string) (Call, error) {
	call, err := scanCall(tx.QueryRow(ctx, `SELECT `+callColumns+` FROM cruise_calls WHERE call_id = $1 FOR UPDATE`, callID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Call{}, ErrNotFound
	}
	if err != nil {
		return Call{}, fmt.Errorf("load cruise call: %w", err)
	}
	return call, nil
}

// Transition advances the cruise call workflow with optimistic concurrency.
func (store *Store) Transition(ctx context.Context, callID string, expectedVersion int64, next Status, principal Principal) (Call, error) {
	if !callIDPattern.MatchString(callID) || expectedVersion < 1 {
		return Call{}, ErrNotFound
	}
	if principal.ID == "" || principal.Role == "" {
		return Call{}, errors.New("a verified principal is required")
	}
	ctx, span := tracer().Start(ctx, "cruise.call_transition", trace.WithAttributes(
		attribute.String("cruise.call_id", callID),
		attribute.String("cruise.transition.to", string(next)),
	))
	defer span.End()
	var updated Call
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		current, err := store.getForUpdate(ctx, tx, callID)
		if err != nil {
			return err
		}
		span.SetAttributes(attribute.String("tenant.id", claims.TenantID))
		if current.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		if !ValidTransition(current.Status, next) {
			return ErrInvalidTransition
		}
		updatedAt := time.Now().UTC()
		updated, err = scanCall(tx.QueryRow(ctx, `
			UPDATE cruise_calls SET status = $1, updated_at = $2, version = version + 1
			WHERE call_id = $3 AND status = $4 AND version = $5
			RETURNING `+callColumns, next, updatedAt, callID, current.Status, expectedVersion))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrOptimisticConflict
		}
		if err != nil {
			return fmt.Errorf("transition cruise call: %w", err)
		}
		return emit(ctx, tx, claims, "cruise.call_status_changed", fmt.Sprintf("%s:%d", callID, updated.Version), callID, updated, map[string]string{
			"from-status": string(current.Status),
			"to-status":   string(next),
		}, principal, updatedAt, store.signer)
	})
	if err != nil {
		span.RecordError(err)
	}
	return updated, err
}

// UpdatePaxCount records the final-manifest passenger count. The band is
// recomputed deterministically; dues are then re-assessable under a new
// idempotency key (the pre-arrival assessment is never mutated — revenue
// recomputation is a new assessment event).
func (store *Store) UpdatePaxCount(ctx context.Context, callID string, expectedVersion int64, paxCount int, principal Principal) (Call, error) {
	if !callIDPattern.MatchString(callID) || expectedVersion < 1 {
		return Call{}, ErrNotFound
	}
	if paxCount <= 0 {
		return Call{}, errors.New("pax_count must be positive")
	}
	if principal.ID == "" || principal.Role == "" {
		return Call{}, errors.New("a verified principal is required")
	}
	var updated Call
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		current, err := store.getForUpdate(ctx, tx, callID)
		if err != nil {
			return err
		}
		if current.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		if current.Status == StatusCompleted || current.Status == StatusCancelled {
			return ErrInvalidTransition
		}
		updatedAt := time.Now().UTC()
		updated, err = scanCall(tx.QueryRow(ctx, `
			UPDATE cruise_calls SET pax_count = $1, pax_band = $2, updated_at = $3, version = version + 1
			WHERE call_id = $4 AND version = $5
			RETURNING `+callColumns, paxCount, BandFor(paxCount), updatedAt, callID, expectedVersion))
		if err != nil {
			return fmt.Errorf("update pax count: %w", err)
		}
		return emit(ctx, tx, claims, "cruise.pax_count_updated", fmt.Sprintf("%s:%d", callID, updated.Version), callID, updated, map[string]string{
			"pax-count": fmt.Sprint(paxCount),
			"pax-band":  string(updated.PaxBand),
		}, principal, updatedAt, store.signer)
	})
	return updated, err
}

// AddExcursion registers an excursion manifest idempotently.
func (store *Store) AddExcursion(ctx context.Context, callID, idempotencyKey, name, operator string, paxCount int, principal Principal) (Excursion, error) {
	if !callIDPattern.MatchString(callID) {
		return Excursion{}, ErrNotFound
	}
	if idempotencyKey == "" || len(idempotencyKey) > 256 || !canonical(name, 2, 256) || !canonical(operator, 2, 256) || paxCount <= 0 {
		return Excursion{}, ErrExcursionInvalid
	}
	if principal.ID == "" || principal.Role == "" {
		return Excursion{}, errors.New("a verified principal is required")
	}
	var excursion Excursion
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		call, err := store.getForUpdate(ctx, tx, callID)
		if err != nil {
			return err
		}
		if call.Status == StatusCompleted || call.Status == StatusCancelled {
			return ErrInvalidTransition
		}
		registeredAt := time.Now().UTC()
		err = tx.QueryRow(ctx, `
			INSERT INTO cruise_excursions (excursion_id, tenant_id, call_id, idempotency_key, name, operator, pax_count, status, registered_by, registered_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, 'SCHEDULED', $8, $9)
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING excursion_id, call_id, name, operator, pax_count, status, registered_by, registered_at`,
			uuid.New(), claims.TenantID, callID, idempotencyKey, name, operator, paxCount, principal.ID, registeredAt).
			Scan(&excursion.ExcursionID, &excursion.CallID, &excursion.Name, &excursion.Operator, &excursion.PaxCount, &excursion.Status, &excursion.RegisteredBy, &excursion.RegisteredAt)
		if errors.Is(err, pgx.ErrNoRows) {
			err = tx.QueryRow(ctx, `
				SELECT excursion_id, call_id, name, operator, pax_count, status, registered_by, registered_at
				FROM cruise_excursions WHERE idempotency_key = $1 FOR UPDATE`, idempotencyKey).
				Scan(&excursion.ExcursionID, &excursion.CallID, &excursion.Name, &excursion.Operator, &excursion.PaxCount, &excursion.Status, &excursion.RegisteredBy, &excursion.RegisteredAt)
			if err != nil {
				return fmt.Errorf("lookup idempotent excursion: %w", err)
			}
			if excursion.CallID != callID || excursion.Name != name || excursion.Operator != operator || excursion.PaxCount != paxCount {
				return ErrIdempotencyConflict
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert excursion: %w", err)
		}
		return emit(ctx, tx, claims, "cruise.excursion_registered", idempotencyKey, callID, excursion, map[string]string{
			"excursion-id": excursion.ExcursionID,
		}, principal, registeredAt, store.signer)
	})
	return excursion, err
}

// AllocateTender reserves a tender berth window. Overlapping windows on the
// same berth are rejected by the exclusion constraint — one berth serves
// one call at a time.
func (store *Store) AllocateTender(ctx context.Context, callID, idempotencyKey, terminalCode, berthCode string, windowStart, windowEnd time.Time, principal Principal) (TenderAllocation, error) {
	if !callIDPattern.MatchString(callID) {
		return TenderAllocation{}, ErrNotFound
	}
	if idempotencyKey == "" || len(idempotencyKey) > 256 ||
		!terminalPattern.MatchString(terminalCode) || !berthPattern.MatchString(berthCode) ||
		windowStart.IsZero() || !windowEnd.After(windowStart) {
		return TenderAllocation{}, ErrAllocationInvalid
	}
	if principal.ID == "" || principal.Role == "" {
		return TenderAllocation{}, errors.New("a verified principal is required")
	}
	var allocation TenderAllocation
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		call, err := store.getForUpdate(ctx, tx, callID)
		if err != nil {
			return err
		}
		if call.Status == StatusCompleted || call.Status == StatusCancelled {
			return ErrInvalidTransition
		}
		allocatedAt := time.Now().UTC()
		err = tx.QueryRow(ctx, `
			INSERT INTO cruise_tender_allocations (allocation_id, tenant_id, call_id, idempotency_key, terminal_code, berth_code, window_start, window_end, allocated_by, allocated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING allocation_id, call_id, terminal_code, berth_code, window_start, window_end, allocated_by, allocated_at`,
			uuid.New(), claims.TenantID, callID, idempotencyKey, terminalCode, berthCode,
			windowStart.UTC(), windowEnd.UTC(), principal.ID, allocatedAt).
			Scan(&allocation.AllocationID, &allocation.CallID, &allocation.TerminalCode, &allocation.BerthCode,
				&allocation.WindowStart, &allocation.WindowEnd, &allocation.AllocatedBy, &allocation.AllocatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			err = tx.QueryRow(ctx, `
				SELECT allocation_id, call_id, terminal_code, berth_code, window_start, window_end, allocated_by, allocated_at
				FROM cruise_tender_allocations WHERE idempotency_key = $1 FOR UPDATE`, idempotencyKey).
				Scan(&allocation.AllocationID, &allocation.CallID, &allocation.TerminalCode, &allocation.BerthCode,
					&allocation.WindowStart, &allocation.WindowEnd, &allocation.AllocatedBy, &allocation.AllocatedAt)
			if err != nil {
				return fmt.Errorf("lookup idempotent tender allocation: %w", err)
			}
			if allocation.CallID != callID || allocation.TerminalCode != terminalCode || allocation.BerthCode != berthCode ||
				!allocation.WindowStart.Equal(windowStart.UTC().Truncate(time.Microsecond)) || !allocation.WindowEnd.Equal(windowEnd.UTC().Truncate(time.Microsecond)) {
				return ErrIdempotencyConflict
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert tender allocation (overlap on the berth is rejected): %w", err)
		}
		return emit(ctx, tx, claims, "cruise.tender_allocated", idempotencyKey, callID, allocation, map[string]string{
			"terminal-code": terminalCode,
			"berth-code":    berthCode,
		}, principal, allocatedAt, store.signer)
	})
	return allocation, err
}

// AssessDues computes the cruise dues (per-passenger charges) against the
// versioned CRUISE_DUES tariff schedule effective at asOf and records the
// revenue assessment exactly once under the idempotency key. Re-assessment
// on the final manifest uses a new idempotency key — prior assessments are
// immutable.
func (store *Store) AssessDues(ctx context.Context, callID, idempotencyKey string, asOf time.Time, principal Principal) (tariff.Assessment, error) {
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
	ctx, span := tracer().Start(ctx, "cruise.dues_assessment", trace.WithAttributes(
		attribute.String("cruise.call_id", callID),
	))
	defer span.End()
	var assessment tariff.Assessment
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		call, err := store.getForUpdate(ctx, tx, callID)
		if err != nil {
			return err
		}
		span.SetAttributes(attribute.String("tenant.id", claims.TenantID))
		schedule, err := tariff.LoadEffective(ctx, tx, tariff.DomainCruiseDues, asOf)
		if err != nil {
			return err
		}
		facts := tariff.Facts{Passengers: int64(call.PaxCount)}
		items, total, err := tariff.Compute(schedule, facts, asOf)
		if err != nil {
			return err
		}
		span.SetAttributes(
			attribute.String("tariff.schedule_id", schedule.ScheduleID),
			attribute.String("cruise.pax_band", string(call.PaxBand)),
			attribute.Int64("assessment.total_minor", total),
		)
		recorded, err := tariff.RecordAssessmentTx(ctx, tx, claims, store.signer, tariff.AssessmentParams{
			IdempotencyKey: idempotencyKey,
			Domain:         tariff.DomainCruiseDues,
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
