package portcall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
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

func (store *Store) Exec(ctx context.Context, statement string) (int64, error) {
	result, err := store.pool.Exec(ctx, statement)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (store *Store) Create(ctx context.Context, idempotencyKey string, request CreateRequest) (PortCall, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return PortCall{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if err := request.Validate(); err != nil {
		return PortCall{}, err
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return PortCall{}, fmt.Errorf("begin create: %w", err)
	}
	defer tx.Rollback(ctx)

	createdAt := time.Now().UTC()
	var call PortCall
	inserted := false
	err = tx.QueryRow(ctx, `
		INSERT INTO port_calls (
			call_id, vessel_imo, port_code, declaration_reference, submitted_by,
			status, idempotency_key, created_at, updated_at, version
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, 1)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING call_id, vessel_imo, port_code, declaration_reference, submitted_by,
			status, created_at, updated_at, version`,
		request.CallID, request.VesselIMO, request.PortCode, request.DeclarationRef,
		request.SubmittedBy, StatusDraft, idempotencyKey, createdAt,
	).Scan(&call.CallID, &call.VesselIMO, &call.PortCode, &call.DeclarationRef, &call.SubmittedBy,
		&call.Status, &call.CreatedAt, &call.UpdatedAt, &call.Version)
	if err == nil {
		inserted = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return PortCall{}, fmt.Errorf("insert port call: %w", err)
	}
	if !inserted {
		err = tx.QueryRow(ctx, `
			SELECT call_id, vessel_imo, port_code, declaration_reference, submitted_by,
				status, created_at, updated_at, version
			FROM port_calls WHERE idempotency_key = $1 FOR UPDATE`, idempotencyKey).
			Scan(&call.CallID, &call.VesselIMO, &call.PortCode, &call.DeclarationRef, &call.SubmittedBy,
				&call.Status, &call.CreatedAt, &call.UpdatedAt, &call.Version)
		if errors.Is(err, pgx.ErrNoRows) {
			return PortCall{}, fmt.Errorf("idempotency lookup: %w", ErrNotFound)
		}
		if err != nil {
			return PortCall{}, fmt.Errorf("lookup idempotent port call: %w", err)
		}
		if !call.Matches(request) {
			return PortCall{}, ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return PortCall{}, fmt.Errorf("commit idempotent replay: %w", err)
		}
		return call, nil
	}

	payload, err := json.Marshal(call)
	if err != nil {
		return PortCall{}, fmt.Errorf("encode created event: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO port_call_outbox (event_id, call_id, event_type, payload, created_at)
		VALUES ($1, $2, 'port_call.created', $3, $4)`, uuid.New(), call.CallID, payload, createdAt); err != nil {
		return PortCall{}, fmt.Errorf("write created event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return PortCall{}, fmt.Errorf("commit create: %w", err)
	}
	return call, nil
}

func (store *Store) Get(ctx context.Context, callID string) (PortCall, error) {
	var call PortCall
	err := store.pool.QueryRow(ctx, `
		SELECT call_id, vessel_imo, port_code, declaration_reference, submitted_by,
			status, created_at, updated_at, version
		FROM port_calls WHERE call_id = $1`, callID).
		Scan(&call.CallID, &call.VesselIMO, &call.PortCode, &call.DeclarationRef, &call.SubmittedBy,
			&call.Status, &call.CreatedAt, &call.UpdatedAt, &call.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return PortCall{}, ErrNotFound
	}
	if err != nil {
		return PortCall{}, fmt.Errorf("get port call: %w", err)
	}
	return call, nil
}

func (store *Store) Transition(ctx context.Context, callID string, expectedVersion int64, next Status) (PortCall, error) {
	current, err := store.Get(ctx, callID)
	if err != nil {
		return PortCall{}, err
	}
	if current.Version != expectedVersion || !ValidTransition(current.Status, next) {
		if current.Version != expectedVersion {
			return PortCall{}, ErrOptimisticConflict
		}
		return PortCall{}, ErrInvalidTransition
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return PortCall{}, fmt.Errorf("begin transition: %w", err)
	}
	defer tx.Rollback(ctx)
	updatedAt := time.Now().UTC()
	var updated PortCall
	err = tx.QueryRow(ctx, `
		UPDATE port_calls
		SET status = $1, updated_at = $2, version = version + 1
		WHERE call_id = $3 AND status = $4 AND version = $5
		RETURNING call_id, vessel_imo, port_code, declaration_reference, submitted_by,
			status, created_at, updated_at, version`,
		next, updatedAt, callID, current.Status, expectedVersion).
		Scan(&updated.CallID, &updated.VesselIMO, &updated.PortCode, &updated.DeclarationRef,
			&updated.SubmittedBy, &updated.Status, &updated.CreatedAt, &updated.UpdatedAt, &updated.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return PortCall{}, ErrOptimisticConflict
	}
	if err != nil {
		return PortCall{}, fmt.Errorf("transition port call: %w", err)
	}
	payload, err := json.Marshal(updated)
	if err != nil {
		return PortCall{}, fmt.Errorf("encode status event: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO port_call_outbox (event_id, call_id, event_type, payload, created_at)
		VALUES ($1, $2, 'port_call.status_changed', $3, $4)`, uuid.New(), updated.CallID, payload, updatedAt); err != nil {
		return PortCall{}, fmt.Errorf("write status event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return PortCall{}, fmt.Errorf("commit transition: %w", err)
	}
	return updated, nil
}
