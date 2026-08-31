package tariff

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/munisp/blueeconomy-port-interoperability/internal/telemetry"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantdb"
)

// Principal is the verified actor behind a tariff mutation; it becomes the
// provenance block of emitted revenue events.
type Principal struct {
	ID   string
	Role string
}

// Store registers schedules and records revenue assessments. Every method
// runs inside tenantdb.WithTx so RLS isolates tenants; assessment emission
// shares the caller's transaction for atomicity with the domain mutation.
type Store struct {
	pool   *pgxpool.Pool
	signer *events.Signer
}

// NewStore builds the tariff store. The envelope signer is mandatory:
// revenue assessment events are JWS-signed at emission and an unsigned
// pipeline fails closed.
func NewStore(pool *pgxpool.Pool, signer *events.Signer) *Store {
	return &Store{pool: pool, signer: signer}
}

func Open(ctx context.Context, databaseURL string, signer *events.Signer) (*Store, error) {
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
	return NewStore(pool, signer), nil
}

func (store *Store) Close() { store.pool.Close() }

// Pool exposes the pool for infrastructure adapters (never for business
// operations, which must stay tenant-scoped).
func (store *Store) Pool() *pgxpool.Pool { return store.pool }

// RegisterSchedule persists a schedule and its rules atomically. Schedules
// are immutable: an exact replay of the same registration is a no-op, a
// conflicting reuse of the schedule id fails closed.
func (store *Store) RegisterSchedule(ctx context.Context, schedule Schedule) error {
	if err := schedule.Validate(); err != nil {
		return err
	}
	return tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		return RegisterScheduleTx(ctx, tx, claims, schedule)
	})
}

// RegisterScheduleTx is RegisterSchedule inside the caller's transaction.
func RegisterScheduleTx(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, schedule Schedule) error {
	registeredAt := schedule.RegisteredAt
	if registeredAt.IsZero() {
		registeredAt = time.Now().UTC()
	}
	inserted, err := tx.Exec(ctx, `
		INSERT INTO tariff_schedules (schedule_id, tenant_id, domain, name, currency, effective_from, effective_to, legal_anchor, registered_by, registered_at, active)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (schedule_id) DO NOTHING`,
		schedule.ScheduleID, claims.TenantID, schedule.Domain, schedule.Name, schedule.Currency,
		schedule.EffectiveFrom.UTC(), schedule.EffectiveTo, schedule.LegalAnchor, schedule.RegisteredBy, registeredAt.UTC(), schedule.Active)
	if err != nil {
		return fmt.Errorf("insert tariff schedule: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		var domain Domain
		var currency, anchor string
		var from time.Time
		var to *time.Time
		if err := tx.QueryRow(ctx, `SELECT domain, currency, legal_anchor, effective_from, effective_to FROM tariff_schedules WHERE schedule_id = $1 FOR SHARE`, schedule.ScheduleID).
			Scan(&domain, &currency, &anchor, &from, &to); err != nil {
			return fmt.Errorf("load retained tariff schedule: %w", err)
		}
		sameWindow := from.Equal(schedule.EffectiveFrom.UTC()) && (to == nil) == (schedule.EffectiveTo == nil) && (to == nil || to.Equal(schedule.EffectiveTo.UTC()))
		if domain != schedule.Domain || currency != schedule.Currency || anchor != schedule.LegalAnchor || !sameWindow {
			return fmt.Errorf("%w: schedule %s is immutable; register a new schedule id", ErrScheduleInvalid, schedule.ScheduleID)
		}
		return nil
	}
	for _, rule := range schedule.Rules {
		ruleID := rule.RuleID
		if ruleID == "" {
			ruleID = uuid.NewString()
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO tariff_rules (rule_id, tenant_id, schedule_id, component_code, unit, amount_minor, band_min, band_max, legal_anchor)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			ruleID, claims.TenantID, schedule.ScheduleID, rule.ComponentCode, rule.Unit, rule.AmountMinor, rule.BandMin, rule.BandMax, rule.LegalAnchor); err != nil {
			return fmt.Errorf("insert tariff rule %s: %w", rule.ComponentCode, err)
		}
	}
	return nil
}

// LoadEffective returns the newest active schedule of the domain whose window
// covers asOf, with its rules. No covering schedule is ErrNotEffective —
// assessments fail closed rather than pricing against stale rates.
func LoadEffective(ctx context.Context, tx pgx.Tx, domain Domain, asOf time.Time) (Schedule, error) {
	if domain != DomainOffshoreTerminal && domain != DomainCruiseDues {
		return Schedule{}, fmt.Errorf("%w: domain", ErrScheduleInvalid)
	}
	var schedule Schedule
	err := tx.QueryRow(ctx, `
		SELECT schedule_id, domain, name, currency, effective_from, effective_to, legal_anchor, registered_by, registered_at, active
		FROM tariff_schedules
		WHERE domain = $1 AND active AND effective_from <= $2 AND (effective_to IS NULL OR effective_to > $2)
		ORDER BY effective_from DESC, schedule_id DESC
		LIMIT 1`, domain, asOf.UTC()).
		Scan(&schedule.ScheduleID, &schedule.Domain, &schedule.Name, &schedule.Currency,
			&schedule.EffectiveFrom, &schedule.EffectiveTo, &schedule.LegalAnchor,
			&schedule.RegisteredBy, &schedule.RegisteredAt, &schedule.Active)
	if errors.Is(err, pgx.ErrNoRows) {
		return Schedule{}, ErrNotEffective
	}
	if err != nil {
		return Schedule{}, fmt.Errorf("load effective tariff schedule: %w", err)
	}
	rows, err := tx.Query(ctx, `
		SELECT rule_id, component_code, unit, amount_minor, band_min, band_max, legal_anchor
		FROM tariff_rules WHERE schedule_id = $1 ORDER BY component_code, band_min`, schedule.ScheduleID)
	if err != nil {
		return Schedule{}, fmt.Errorf("load tariff rules: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var rule Rule
		if err := rows.Scan(&rule.RuleID, &rule.ComponentCode, &rule.Unit, &rule.AmountMinor, &rule.BandMin, &rule.BandMax, &rule.LegalAnchor); err != nil {
			return Schedule{}, fmt.Errorf("scan tariff rule: %w", err)
		}
		schedule.Rules = append(schedule.Rules, rule)
	}
	if rows.Err() != nil {
		return Schedule{}, fmt.Errorf("iterate tariff rules: %w", rows.Err())
	}
	if len(schedule.Rules) == 0 {
		return Schedule{}, fmt.Errorf("%w: schedule %s has no rules", ErrScheduleInvalid, schedule.ScheduleID)
	}
	return schedule, nil
}

// LoadSchedule returns a pinned schedule by id (used at assessment replay).
func LoadSchedule(ctx context.Context, tx pgx.Tx, scheduleID string) (Schedule, error) {
	var schedule Schedule
	err := tx.QueryRow(ctx, `
		SELECT schedule_id, domain, name, currency, effective_from, effective_to, legal_anchor, registered_by, registered_at, active
		FROM tariff_schedules WHERE schedule_id = $1`, scheduleID).
		Scan(&schedule.ScheduleID, &schedule.Domain, &schedule.Name, &schedule.Currency,
			&schedule.EffectiveFrom, &schedule.EffectiveTo, &schedule.LegalAnchor,
			&schedule.RegisteredBy, &schedule.RegisteredAt, &schedule.Active)
	if errors.Is(err, pgx.ErrNoRows) {
		return Schedule{}, ErrNotFound
	}
	if err != nil {
		return Schedule{}, fmt.Errorf("load tariff schedule: %w", err)
	}
	rows, err := tx.Query(ctx, `
		SELECT rule_id, component_code, unit, amount_minor, band_min, band_max, legal_anchor
		FROM tariff_rules WHERE schedule_id = $1 ORDER BY component_code, band_min`, scheduleID)
	if err != nil {
		return Schedule{}, fmt.Errorf("load tariff rules: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var rule Rule
		if err := rows.Scan(&rule.RuleID, &rule.ComponentCode, &rule.Unit, &rule.AmountMinor, &rule.BandMin, &rule.BandMax, &rule.LegalAnchor); err != nil {
			return Schedule{}, fmt.Errorf("scan tariff rule: %w", err)
		}
		schedule.Rules = append(schedule.Rules, rule)
	}
	return schedule, rows.Err()
}

// Assessment is a recorded, immutable revenue assessment pinned to a
// schedule version.
type Assessment struct {
	AssessmentID   string          `json:"assessment_id"`
	Domain         Domain          `json:"domain"`
	CallReference  string          `json:"call_reference"`
	ScheduleID     string          `json:"schedule_id"`
	AsOf           time.Time       `json:"as_of"`
	Facts          Facts           `json:"facts"`
	LineItems      []LineItem      `json:"line_items"`
	TotalMinor     int64           `json:"total_minor"`
	Currency       string          `json:"currency"`
	AssessedBy     string          `json:"assessed_by"`
	AssessedAt     time.Time       `json:"assessed_at"`
	IdempotencyKey string          `json:"idempotency_key"`
	factsJSON      json.RawMessage `json:"-"`
}

// AssessmentParams carries everything RecordAssessmentTx needs; the caller
// has already computed items/total via Compute against a pinned schedule.
type AssessmentParams struct {
	IdempotencyKey string
	Domain         Domain
	CallReference  string
	Schedule       Schedule
	AsOf           time.Time
	Facts          Facts
	LineItems      []LineItem
	TotalMinor     int64
	Principal      Principal
}

// RecordAssessmentTx records an assessment and emits the signed revenue
// event to finance.revenue-assessments.v1 inside the caller's transaction.
// Exactly-once: the idempotency key is unique; an identical replay returns
// the retained assessment without re-emitting, and a conflicting reuse of
// the key fails closed. Emission happens only on the first insert, in the
// same transaction as the assessment row.
func RecordAssessmentTx(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, signer *events.Signer, params AssessmentParams) (Assessment, error) {
	if signer == nil {
		return Assessment{}, errors.New("an envelope signer is required (fail closed)")
	}
	if params.IdempotencyKey == "" || len(params.IdempotencyKey) > 256 {
		return Assessment{}, errors.New("assessment idempotency key must be non-empty and at most 256 characters")
	}
	if params.Domain != DomainOffshoreTerminal && params.Domain != DomainCruiseDues {
		return Assessment{}, fmt.Errorf("%w: domain", ErrScheduleInvalid)
	}
	if !canonicalText(params.CallReference, 2, 64) || !canonicalText(params.Principal.ID, 2, 256) || params.Principal.Role == "" {
		return Assessment{}, fmt.Errorf("%w: call reference and principal are required", ErrAssessmentReplay)
	}
	if params.TotalMinor < 0 || len(params.LineItems) == 0 {
		return Assessment{}, errors.New("assessment requires a non-negative total and at least one line item")
	}
	// timestamptz stores microseconds; canonicalize so an exact replay of the
	// as-of instant compares equal after the database round-trip.
	params.AsOf = params.AsOf.UTC().Truncate(time.Microsecond)
	factsJSON, err := json.Marshal(params.Facts)
	if err != nil {
		return Assessment{}, fmt.Errorf("encode assessment facts: %w", err)
	}
	itemsJSON, err := json.Marshal(params.LineItems)
	if err != nil {
		return Assessment{}, fmt.Errorf("encode assessment line items: %w", err)
	}
	assessedAt := time.Now().UTC()
	var assessment Assessment
	assessment.Facts = params.Facts
	assessment.LineItems = params.LineItems
	inserted := true
	err = tx.QueryRow(ctx, `
		INSERT INTO revenue_assessments (assessment_id, tenant_id, idempotency_key, domain, call_reference, schedule_id, as_of, facts, line_items, total_minor, currency, assessed_by, assessed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING assessment_id, domain, call_reference, schedule_id, as_of, facts, line_items, total_minor, currency, assessed_by, assessed_at, idempotency_key`,
		uuid.New(), claims.TenantID, params.IdempotencyKey, params.Domain, params.CallReference, params.Schedule.ScheduleID,
		params.AsOf.UTC(), factsJSON, itemsJSON, params.TotalMinor, params.Schedule.Currency, params.Principal.ID, assessedAt).
		Scan(&assessment.AssessmentID, &assessment.Domain, &assessment.CallReference, &assessment.ScheduleID, &assessment.AsOf,
			&assessment.factsJSON, &itemsJSON, &assessment.TotalMinor, &assessment.Currency, &assessment.AssessedBy, &assessment.AssessedAt, &assessment.IdempotencyKey)
	if errors.Is(err, pgx.ErrNoRows) {
		inserted = false
	} else if err != nil {
		return Assessment{}, fmt.Errorf("insert revenue assessment: %w", err)
	}
	if !inserted {
		var retainedFacts, retainedItems json.RawMessage
		err = tx.QueryRow(ctx, `
			SELECT assessment_id, domain, call_reference, schedule_id, as_of, facts, line_items, total_minor, currency, assessed_by, assessed_at, idempotency_key
			FROM revenue_assessments WHERE idempotency_key = $1 FOR UPDATE`, params.IdempotencyKey).
			Scan(&assessment.AssessmentID, &assessment.Domain, &assessment.CallReference, &assessment.ScheduleID, &assessment.AsOf,
				&retainedFacts, &retainedItems, &assessment.TotalMinor, &assessment.Currency, &assessment.AssessedBy, &assessment.AssessedAt, &assessment.IdempotencyKey)
		if err != nil {
			return Assessment{}, fmt.Errorf("load retained assessment: %w", err)
		}
		same := assessment.Domain == params.Domain && assessment.CallReference == params.CallReference &&
			assessment.ScheduleID == params.Schedule.ScheduleID && assessment.AsOf.Equal(params.AsOf.UTC()) &&
			assessment.TotalMinor == params.TotalMinor && assessment.AssessedBy == params.Principal.ID
		if !same {
			return Assessment{}, ErrAssessmentReplay
		}
		assessment.Facts = params.Facts
		assessment.LineItems = params.LineItems
		return assessment, nil
	}

	assessment.factsJSON = nil
	payloadJSON, err := json.Marshal(assessment)
	if err != nil {
		return Assessment{}, fmt.Errorf("encode revenue assessment payload: %w", err)
	}
	envelope, err := events.Message("revenue.assessment_issued", events.TopicRevenueAssessments,
		params.IdempotencyKey, params.CallReference, payloadJSON, map[string]string{
			"domain":         string(params.Domain),
			"schedule-id":    params.Schedule.ScheduleID,
			"currency":       params.Schedule.Currency,
			"total-minor":    fmt.Sprint(params.TotalMinor),
			"call-reference": params.CallReference,
		}, events.Provenance{PrincipalID: params.Principal.ID, PrincipalRole: params.Principal.Role}, assessedAt, signer)
	if err != nil {
		return Assessment{}, fmt.Errorf("build revenue assessment envelope: %w", err)
	}
	envelopeJSON, err := json.Marshal(envelope)
	if err != nil {
		return Assessment{}, fmt.Errorf("encode revenue assessment envelope: %w", err)
	}
	eventID, err := uuid.Parse(envelope.EventID)
	if err != nil {
		return Assessment{}, fmt.Errorf("parse event id: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO platform_outbox (event_id, tenant_id, topic, event_type, idempotency_key, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		eventID, claims.TenantID, events.TopicRevenueAssessments, "revenue.assessment_issued", envelope.EventID, envelopeJSON, assessedAt); err != nil {
		return Assessment{}, fmt.Errorf("write revenue assessment outbox event: %w", err)
	}
	return assessment, nil
}
