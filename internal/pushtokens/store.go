// Package pushtokens maintains mobile push-notification device tokens
// (POST /v1/push-tokens, POST /v1/push-tokens/revoke). Registration is
// scoped to the verified gateway subject (tenant + user + device); the
// request body can never impersonate another user. Revoke is explicit and
// auditable (status + revoked_at, never a hard delete). No maker-checker:
// a user managing their own device tokens carries no dual-control
// obligation.
package pushtokens

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/munisp/blueeconomy-port-interoperability/internal/telemetry"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantdb"
)

// Store persists push tokens. The pool is mandatory — construction fails
// closed without it.
type Store struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

func NewStore(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, errors.New("push-token store requires a database pool")
	}
	return &Store{pool: pool, now: func() time.Time { return time.Now().UTC() }}, nil
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	if err := telemetry.ApplyPoolEnv(poolConfig); err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return NewStore(pool)
}

func (store *Store) Close() { store.pool.Close() }

// Pool exposes the pool for test harnesses (schema reset/migration).
func (store *Store) Pool() *pgxpool.Pool { return store.pool }

// RegisterRequest is one device-token registration from the mobile client.
type RegisterRequest struct {
	DeviceID string `json:"deviceId"`
	Token    string `json:"token"`
	Platform string `json:"platform"`
}

func (request RegisterRequest) Validate() error {
	if strings.TrimSpace(request.DeviceID) == "" {
		return errors.New("deviceId is required")
	}
	if len(request.DeviceID) > 256 {
		return errors.New("deviceId is too long")
	}
	if len(request.Token) < 8 || len(request.Token) > 4096 {
		return errors.New("token must be between 8 and 4096 characters")
	}
	switch request.Platform {
	case "android", "ios", "web":
	default:
		return errors.New("platform must be android, ios or web")
	}
	return nil
}

// Token is the stored registration view returned to the caller.
type Token struct {
	UserID       string     `json:"userId"`
	DeviceID     string     `json:"deviceId"`
	Token        string     `json:"token"`
	Platform     string     `json:"platform"`
	Status       string     `json:"status"`
	RegisteredAt time.Time  `json:"registeredAt"`
	UpdatedAt    time.Time  `json:"updatedAt"`
	RevokedAt    *time.Time `json:"revokedAt,omitempty"`
}

// ErrNotFound marks a revoke against an unknown or already-revoked device.
var ErrNotFound = errors.New("push token not found")

// Register upserts the (tenant, user, device) registration. The user id is
// the verified subject from the tenant claims. A provider token moving
// between devices/users revokes the previous ACTIVE holder in the same
// transaction (at most one ACTIVE row per token).
func (store *Store) Register(ctx context.Context, request RegisterRequest) (Token, error) {
	if err := request.Validate(); err != nil {
		return Token{}, err
	}
	var stored Token
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		now := store.now()
		if _, err := tx.Exec(ctx, `
			UPDATE push_tokens
			SET status = 'REVOKED', revoked_at = $3, updated_at = $3
			WHERE tenant_id = $1 AND token = $2 AND status = 'ACTIVE'
			  AND NOT (user_id = $4 AND device_id = $5)`,
			claims.TenantID, request.Token, now, claims.Subject, request.DeviceID); err != nil {
			return fmt.Errorf("revoke previous token holder: %w", err)
		}
		return tx.QueryRow(ctx, `
			INSERT INTO push_tokens
				(tenant_id, user_id, device_id, token, platform, status,
				 registered_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, 'ACTIVE', $6, $6)
			ON CONFLICT (tenant_id, user_id, device_id) DO UPDATE
			SET token = EXCLUDED.token, platform = EXCLUDED.platform,
			    status = 'ACTIVE', updated_at = EXCLUDED.updated_at,
			    revoked_at = NULL
			RETURNING user_id, device_id, token, platform, status,
			          registered_at, updated_at, revoked_at`,
			claims.TenantID, claims.Subject, request.DeviceID, request.Token,
			request.Platform, now).
			Scan(&stored.UserID, &stored.DeviceID, &stored.Token, &stored.Platform,
				&stored.Status, &stored.RegisteredAt, &stored.UpdatedAt, &stored.RevokedAt)
	})
	if err != nil {
		return Token{}, err
	}
	return stored, nil
}

// Revoke marks the caller's device token REVOKED. Unknown or already
// revoked devices fail closed with ErrNotFound (no silent success).
func (store *Store) Revoke(ctx context.Context, deviceID string) (Token, error) {
	if strings.TrimSpace(deviceID) == "" {
		return Token{}, errors.New("deviceId is required")
	}
	var stored Token
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		now := store.now()
		return tx.QueryRow(ctx, `
			UPDATE push_tokens
			SET status = 'REVOKED', revoked_at = $4, updated_at = $4
			WHERE tenant_id = $1 AND user_id = $2 AND device_id = $3
			  AND status = 'ACTIVE'
			RETURNING user_id, device_id, token, platform, status,
			          registered_at, updated_at, revoked_at`,
			claims.TenantID, claims.Subject, deviceID, now).
			Scan(&stored.UserID, &stored.DeviceID, &stored.Token, &stored.Platform,
				&stored.Status, &stored.RegisteredAt, &stored.UpdatedAt, &stored.RevokedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Token{}, ErrNotFound
	}
	if err != nil {
		return Token{}, err
	}
	return stored, nil
}
