package telemetry

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ApplyPoolEnv overrides pgx pool sizing from the environment. Every knob is
// optional: unset (or <= 0) values keep the pgx defaults, so behavior is
// unchanged for existing deployments.
//
//	PORTIO_DB_POOL_MAX_CONNS          max open connections (default: pgx default, max(4, NumCPU))
//	PORTIO_DB_POOL_MIN_CONNS          min idle connections (default: 0)
//	PORTIO_DB_POOL_MAX_CONN_IDLE_SEC  max connection idle seconds (default: 1800)
//	PORTIO_DB_POOL_MAX_CONN_LIFE_SEC  max connection lifetime seconds (default: 3600)
func ApplyPoolEnv(config *pgxpool.Config) error {
	if raw := os.Getenv("PORTIO_DB_POOL_MAX_CONNS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return fmt.Errorf("PORTIO_DB_POOL_MAX_CONNS must be a positive integer, got %q", raw)
		}
		config.MaxConns = int32(value)
	}
	if raw := os.Getenv("PORTIO_DB_POOL_MIN_CONNS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			return fmt.Errorf("PORTIO_DB_POOL_MIN_CONNS must be a non-negative integer, got %q", raw)
		}
		config.MinConns = int32(value)
	}
	if raw := os.Getenv("PORTIO_DB_POOL_MAX_CONN_IDLE_SEC"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return fmt.Errorf("PORTIO_DB_POOL_MAX_CONN_IDLE_SEC must be a positive integer, got %q", raw)
		}
		config.MaxConnIdleTime = time.Duration(value) * time.Second
	}
	if raw := os.Getenv("PORTIO_DB_POOL_MAX_CONN_LIFE_SEC"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return fmt.Errorf("PORTIO_DB_POOL_MAX_CONN_LIFE_SEC must be a positive integer, got %q", raw)
		}
		config.MaxConnLifetime = time.Duration(value) * time.Second
	}
	return nil
}

// NewPGXPool opens a pgx pool with the otelpgx query tracer attached, so
// every query, batch, prepared statement, copy and acquire emits a span under
// the active trace. When telemetry is disabled the spans are no-ops and the
// pool behaves exactly as before (fail-open telemetry, fail-closed database).
func NewPGXPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	config.ConnConfig.Tracer = otelpgx.NewTracer()
	if err := ApplyPoolEnv(config); err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	return pool, nil
}
