package ais

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store persists validated position reports. Ingestion is idempotent on
// the (mmsi, message timestamp) identity: replayed feed batches upsert and
// never double-count.
type Store interface {
	UpsertPositions(ctx context.Context, reports []PositionReport) (int, error)
	Latest(ctx context.Context, mmsi string, limit int) ([]PositionReport, error)
	Count(ctx context.Context) (int64, error)
}

// MemoryStore is the database-free Store used by unit tests and by the
// fail-closed constructor paths; production wiring uses PgStore.
type MemoryStore struct {
	mu       sync.Mutex
	reports  map[string]PositionReport
	inserted int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{reports: map[string]PositionReport{}}
}

func (store *MemoryStore) UpsertPositions(_ context.Context, reports []PositionReport) (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	fresh := 0
	for _, report := range reports {
		key := report.dedupeKey()
		if _, exists := store.reports[key]; !exists {
			fresh++
		}
		store.reports[key] = report
	}
	store.inserted += fresh
	return fresh, nil
}

func (store *MemoryStore) Latest(_ context.Context, mmsi string, limit int) ([]PositionReport, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var out []PositionReport
	for _, report := range store.reports {
		if mmsi == "" || report.MMSI == mmsi {
			out = append(out, report)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.After(out[j].Timestamp) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (store *MemoryStore) Count(_ context.Context) (int64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return int64(len(store.reports)), nil
}

// PgStore is the PostgreSQL-backed production Store. AIS positions are
// platform-level (not tenant-scoped) reference data, so queries run
// without the tenant transaction wrapper.
type PgStore struct {
	pool *pgxpool.Pool
}

func NewPgStore(pool *pgxpool.Pool) (*PgStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("AIS store requires a database pool")
	}
	return &PgStore{pool: pool}, nil
}

func (store *PgStore) UpsertPositions(ctx context.Context, reports []PositionReport) (int, error) {
	fresh := 0
	for _, report := range reports {
		tag, err := store.pool.Exec(ctx, `
			INSERT INTO pcs_ais_positions (
				mmsi, imo, latitude, longitude, speed_knots, course_degrees, heading, message_ts, ingested_at
			) VALUES ($1, NULLIF($2, ''), $3, $4, $5, $6, $7, $8, now())
			ON CONFLICT (mmsi, message_ts) DO NOTHING`,
			report.MMSI, report.IMO, report.Latitude, report.Longitude,
			report.SpeedKnots, report.CourseDegrees, report.Heading, report.Timestamp.UTC())
		if err != nil {
			return fresh, fmt.Errorf("upsert AIS position: %w", err)
		}
		fresh += int(tag.RowsAffected())
	}
	return fresh, nil
}

func (store *PgStore) Latest(ctx context.Context, mmsi string, limit int) ([]PositionReport, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	query := `
		SELECT mmsi, COALESCE(imo, ''), latitude, longitude, speed_knots, course_degrees, heading, message_ts
		FROM pcs_ais_positions`
	args := []any{}
	if mmsi != "" {
		query += ` WHERE mmsi = $1`
		args = append(args, mmsi)
		query += ` ORDER BY message_ts DESC LIMIT $2`
	} else {
		query += ` ORDER BY message_ts DESC LIMIT $1`
	}
	if mmsi != "" {
		args = append(args, limit)
	} else {
		args = append(args, limit)
	}
	rows, err := store.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list AIS positions: %w", err)
	}
	defer rows.Close()
	var out []PositionReport
	for rows.Next() {
		var report PositionReport
		if err := rows.Scan(&report.MMSI, &report.IMO, &report.Latitude, &report.Longitude,
			&report.SpeedKnots, &report.CourseDegrees, &report.Heading, &report.Timestamp); err != nil {
			return nil, fmt.Errorf("scan AIS position: %w", err)
		}
		out = append(out, report)
	}
	return out, rows.Err()
}

func (store *PgStore) Count(ctx context.Context) (int64, error) {
	var count int64
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM pcs_ais_positions`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count AIS positions: %w", err)
	}
	return count, nil
}
