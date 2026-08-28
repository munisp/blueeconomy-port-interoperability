package declarations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// Store is the tenant-scoped declaration repository. Every method runs inside
// tenantdb.WithTx so the RLS policies isolate tenants; lifecycle events are
// written to the platform outbox in the same transaction as the mutation.
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

func (store *Store) Pool() *pgxpool.Pool {
	return store.pool
}

func (store *Store) withTx(ctx context.Context, work func(pgx.Tx, tenantctx.Claims) error) error {
	return tenantdb.WithTx(ctx, store.pool, work)
}

const declarationColumns = `declaration_id, tenant_id, request_id, declaration_ref, ucr, revision,
	supersedes_id, trader_id, declaration_type, status, risk_lane, risk_score, scoring_model,
	scoring_error, hs_code, goods_description, country_of_origin, country_of_destination,
	port_of_entry, gross_weight_kg, net_weight_kg, number_of_packages, consignee_id, operator_id,
	is_aeo, invoice_amount_minor, freight_amount_minor, insurance_amount_minor, invoice_currency,
	tariff_bps, vat_bps, levy_bps, excise_bps, duty_minor, vat_minor, levy_minor, excise_minor,
	total_duty_minor, rejection_reason, submitted_at, cleared_at, created_at, updated_at, version`

func scanDeclaration(row pgx.Row) (Declaration, error) {
	var declaration Declaration
	err := row.Scan(&declaration.DeclarationID, &declaration.TenantID, &declaration.RequestID,
		&declaration.DeclarationRef, &declaration.UCR, &declaration.Revision, &declaration.SupersedesID,
		&declaration.TraderID, &declaration.DeclarationType, &declaration.Status, &declaration.RiskLane,
		&declaration.RiskScore, &declaration.ScoringModel, &declaration.ScoringError, &declaration.HSCode,
		&declaration.GoodsDescription, &declaration.CountryOfOrigin, &declaration.CountryOfDestination,
		&declaration.PortOfEntry, &declaration.GrossWeightKg, &declaration.NetWeightKg,
		&declaration.NumberOfPackages, &declaration.ConsigneeID, &declaration.OperatorID, &declaration.IsAEO,
		&declaration.InvoiceAmountMinor, &declaration.FreightAmountMinor, &declaration.InsuranceAmountMinor,
		&declaration.InvoiceCurrency, &declaration.TariffBPS, &declaration.VatBPS, &declaration.LevyBPS,
		&declaration.ExciseBPS, &declaration.DutyMinor, &declaration.VatMinor, &declaration.LevyMinor,
		&declaration.ExciseMinor, &declaration.TotalDutyMinor, &declaration.RejectionReason,
		&declaration.SubmittedAt, &declaration.ClearedAt, &declaration.CreatedAt, &declaration.UpdatedAt,
		&declaration.Version)
	return declaration, err
}

// emit writes a FHIR-enveloped lifecycle event into the transactional
// platform outbox inside the caller's transaction. The envelope subject id is
// the declaration ref so downstream consumers (including the NSW adapter)
// reference the business identifier.
func emit(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, eventType string, declaration Declaration, extensions map[string]string, principal Principal, occurredAt time.Time) error {
	payloadJSON, err := json.Marshal(declaration)
	if err != nil {
		return fmt.Errorf("encode %s payload: %w", eventType, err)
	}
	envelope, err := events.Message(eventType, events.TopicDeclarations, declaration.RequestID, declaration.DeclarationRef,
		payloadJSON, extensions, events.Provenance{
			PrincipalID:   principal.ID,
			PrincipalRole: principal.Role,
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
		eventID, claims.TenantID, events.TopicDeclarations, eventType, envelope.EventID, envelopeJSON, occurredAt); err != nil {
		return fmt.Errorf("write %s outbox event: %w", eventType, err)
	}
	return nil
}

func requirePrincipal(principal Principal) error {
	if principal.ID == "" || principal.Role == "" {
		return errors.New("declaration principal is required")
	}
	return nil
}

// Create registers a DRAFT declaration (revision 1). Idempotent on
// (tenant, request_id): a replayed create with identical content returns the
// retained declaration; divergent content is an idempotency conflict.
func (store *Store) Create(ctx context.Context, request CreateRequest, principal Principal) (Declaration, error) {
	if err := request.Validate(); err != nil {
		return Declaration{}, err
	}
	if err := requirePrincipal(principal); err != nil {
		return Declaration{}, err
	}
	hsCode, err := NormalizeHSCode(request.HSCode)
	if err != nil {
		return Declaration{}, err
	}
	var declaration Declaration
	err = store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		now := time.Now().UTC()
		created, err := scanDeclaration(tx.QueryRow(ctx, `
			INSERT INTO customs_declarations (
				declaration_id, tenant_id, request_id, declaration_ref, ucr, revision, trader_id,
				declaration_type, status, hs_code, goods_description, country_of_origin,
				country_of_destination, port_of_entry, gross_weight_kg, net_weight_kg,
				number_of_packages, consignee_id, operator_id, is_aeo, invoice_amount_minor,
				freight_amount_minor, insurance_amount_minor, invoice_currency, tariff_bps, vat_bps,
				levy_bps, excise_bps, created_at, updated_at, version
			) VALUES ($1,$2,$3,$4,$5,1,$6,$7,'DRAFT',$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$27,1)
			ON CONFLICT (tenant_id, request_id) DO NOTHING
			RETURNING `+declarationColumns,
			uuid.New(), claims.TenantID, request.RequestID, request.DeclarationRef, nilIfEmpty(request.UCR),
			principal.ID, string(request.DeclarationType), hsCode, request.GoodsDescription,
			request.CountryOfOrigin, nilIfEmpty(request.CountryOfDestination), request.PortOfEntry,
			request.GrossWeightKg, request.NetWeightKg, request.NumberOfPackages, request.ConsigneeID,
			request.OperatorID, request.IsAEO, request.InvoiceAmountMinor, request.FreightAmountMinor,
			request.InsuranceAmountMinor, request.InvoiceCurrency, request.TariffBPS, request.VatBPS,
			request.LevyBPS, request.ExciseBPS, now))
		if errors.Is(err, pgx.ErrNoRows) {
			existing, lookupErr := store.getByRequestID(ctx, tx, claims.TenantID, request.RequestID)
			if lookupErr != nil {
				return fmt.Errorf("lookup idempotent declaration: %w", lookupErr)
			}
			if existing.DeclarationRef != request.DeclarationRef || existing.HSCode != hsCode ||
				existing.GrossWeightKg != request.GrossWeightKg || existing.ConsigneeID != request.ConsigneeID ||
				existing.OperatorID != request.OperatorID || existing.InvoiceAmountMinor != request.InvoiceAmountMinor {
				return ErrIdempotencyConflict
			}
			declaration = existing
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert declaration: %w", err)
		}
		declaration = created
		return nil
	})
	return declaration, err
}

func nilIfEmpty(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func (store *Store) getByRequestID(ctx context.Context, tx pgx.Tx, tenantID, requestID string) (Declaration, error) {
	return scanDeclaration(tx.QueryRow(ctx, `SELECT `+declarationColumns+`
		FROM customs_declarations WHERE tenant_id=$1 AND request_id=$2`, tenantID, requestID))
}

// Get loads one declaration revision by id.
func (store *Store) Get(ctx context.Context, declarationID string) (Declaration, error) {
	var declaration Declaration
	err := store.withTx(ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		found, err := scanDeclaration(tx.QueryRow(ctx, `SELECT `+declarationColumns+`
			FROM customs_declarations WHERE declaration_id=$1`, declarationID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("get declaration: %w", err)
		}
		declaration = found
		return nil
	})
	return declaration, err
}

func (store *Store) getForUpdate(ctx context.Context, tx pgx.Tx, declarationID string) (Declaration, error) {
	declaration, err := scanDeclaration(tx.QueryRow(ctx, `SELECT `+declarationColumns+`
		FROM customs_declarations WHERE declaration_id=$1 FOR UPDATE`, declarationID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Declaration{}, ErrNotFound
	}
	if err != nil {
		return Declaration{}, fmt.Errorf("lock declaration: %w", err)
	}
	return declaration, nil
}

// HeadByRef loads the live (non-superseded) revision of a declaration ref.
// It is the lookup the customs LocalValidator cross-checks bookings against.
func (store *Store) HeadByRef(ctx context.Context, declarationRef string) (Declaration, error) {
	var declaration Declaration
	err := store.withTx(ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		found, err := scanDeclaration(tx.QueryRow(ctx, `SELECT `+declarationColumns+`
			FROM customs_declarations WHERE declaration_ref=$1 AND status <> 'SUPERSEDED'
			ORDER BY revision DESC LIMIT 1`, declarationRef))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("get declaration by ref: %w", err)
		}
		declaration = found
		return nil
	})
	return declaration, err
}

// List returns declaration revisions for a trader (or the whole tenant when
// traderID is empty), newest first. The limit is bounded fail-closed.
func (store *Store) List(ctx context.Context, traderID string, status Status, limit, offset int) ([]Declaration, error) {
	if limit < 1 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	var declarations []Declaration
	err := store.withTx(ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		rows, err := tx.Query(ctx, `SELECT `+declarationColumns+`
			FROM customs_declarations
			WHERE ($1 = '' OR trader_id = $1) AND ($2 = '' OR status = $2)
			ORDER BY created_at DESC, declaration_id
			LIMIT $3 OFFSET $4`, traderID, string(status), limit, offset)
		if err != nil {
			return fmt.Errorf("list declarations: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			declaration, err := scanDeclaration(rows)
			if err != nil {
				return fmt.Errorf("scan declaration: %w", err)
			}
			declarations = append(declarations, declaration)
		}
		return rows.Err()
	})
	return declarations, err
}

// permitsBlockSubmit reports whether any linked OGA permit blocks submission:
// a permit that is not APPROVED, or an APPROVED permit whose expiry has
// passed, fails the declaration closed.
func permitsBlockSubmit(ctx context.Context, tx pgx.Tx, declarationID string, now time.Time) error {
	rows, err := tx.Query(ctx, `SELECT agency_code, status, expires_at FROM declaration_permits WHERE declaration_id=$1`, declarationID)
	if err != nil {
		return fmt.Errorf("load declaration permits: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var agencyCode, status string
		var expiresAt *time.Time
		if err := rows.Scan(&agencyCode, &status, &expiresAt); err != nil {
			return fmt.Errorf("scan declaration permit: %w", err)
		}
		if status != PermitApproved {
			return fmt.Errorf("%w: %s permit is %s", ErrPermitInvalid, agencyCode, status)
		}
		if expiresAt != nil && !expiresAt.After(now) {
			return fmt.Errorf("%w: %s permit has expired", ErrPermitInvalid, agencyCode)
		}
	}
	return rows.Err()
}

// Submit moves DRAFT -> SUBMITTED: HS-code and permit rules are re-enforced,
// the CIF duty assessment is computed and persisted, and
// trade.declaration.submitted.v1 is emitted in the same transaction.
func (store *Store) Submit(ctx context.Context, declarationID string, expectedVersion int64, principal Principal) (Declaration, error) {
	if err := requirePrincipal(principal); err != nil {
		return Declaration{}, err
	}
	var submitted Declaration
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		declaration, err := store.getForUpdate(ctx, tx, declarationID)
		if err != nil {
			return err
		}
		if declaration.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		if declaration.Status != StatusDraft {
			return ErrInvalidTransition
		}
		if _, err := NormalizeHSCode(declaration.HSCode); err != nil {
			return fmt.Errorf("%w: %v", ErrDeclarationInvalid, err)
		}
		now := time.Now().UTC()
		if err := permitsBlockSubmit(ctx, tx, declarationID, now); err != nil {
			return err
		}
		breakdown, err := CalculateDuty(DutyInput{
			InvoiceAmountMinor:   declaration.InvoiceAmountMinor,
			FreightAmountMinor:   declaration.FreightAmountMinor,
			InsuranceAmountMinor: declaration.InsuranceAmountMinor,
			TariffBPS:            declaration.TariffBPS,
			VatBPS:               declaration.VatBPS,
			LevyBPS:              declaration.LevyBPS,
			ExciseBPS:            declaration.ExciseBPS,
		})
		if err != nil {
			return fmt.Errorf("%w: %v", ErrDeclarationInvalid, err)
		}
		updated, err := scanDeclaration(tx.QueryRow(ctx, `
			UPDATE customs_declarations
			SET status='SUBMITTED', duty_minor=$1, vat_minor=$2, levy_minor=$3, excise_minor=$4,
				total_duty_minor=$5, submitted_at=$6, updated_at=$6, version=version+1
			WHERE declaration_id=$7 AND status='DRAFT' AND version=$8
			RETURNING `+declarationColumns,
			breakdown.DutyMinor, breakdown.VatMinor, breakdown.LevyMinor, breakdown.ExciseMinor,
			breakdown.TotalDutyMinor, now, declarationID, expectedVersion))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrOptimisticConflict
		}
		if err != nil {
			return fmt.Errorf("submit declaration: %w", err)
		}
		if err := emit(ctx, tx, claims, EventSubmitted, updated, map[string]string{
			"declaration-ref":  updated.DeclarationRef,
			"declaration-type": string(updated.DeclarationType),
			"total-duty-minor": fmt.Sprintf("%d", breakdown.TotalDutyMinor),
		}, principal, now); err != nil {
			return err
		}
		submitted = updated
		return nil
	})
	return submitted, err
}

// AssessRisk drives SUBMITTED -> RISK_ASSESSED -> lane, and GREEN lane
// continues to CLEARED with an atomically issued clearance certificate
// (auto-clearance eligible per the lane rules). The scorer call happens
// outside any transaction; any scorer failure — unreachable or an invalid
// verdict — parks the declaration in the terminal SCORING_UNAVAILABLE state
// and never assigns a lane.
func (store *Store) AssessRisk(ctx context.Context, declarationID string, expectedVersion int64, scorer Scorer, highValueThresholdMinor int64, principal Principal) (Declaration, error) {
	if scorer == nil {
		return Declaration{}, errors.New("risk scorer is not configured")
	}
	if err := requirePrincipal(principal); err != nil {
		return Declaration{}, err
	}
	var snapshot Declaration
	err := store.withTx(ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		declaration, err := store.getForUpdate(ctx, tx, declarationID)
		if err != nil {
			return err
		}
		if declaration.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		if declaration.Status != StatusSubmitted {
			return ErrInvalidTransition
		}
		snapshot = declaration
		return nil
	})
	if err != nil {
		return Declaration{}, err
	}
	verdict, scoreErr := scorer.Score(ctx, ScoreRequest{
		DeclarationRef:       snapshot.DeclarationRef,
		DeclarationType:      string(snapshot.DeclarationType),
		HSCode:               snapshot.HSCode,
		GoodsDescription:     snapshot.GoodsDescription,
		CountryOfOrigin:      snapshot.CountryOfOrigin,
		CountryOfDestination: deref(snapshot.CountryOfDestination),
		PortOfEntry:          snapshot.PortOfEntry,
		GrossWeightKg:        snapshot.GrossWeightKg,
		NumberOfPackages:     snapshot.NumberOfPackages,
		InvoiceAmountMinor:   snapshot.InvoiceAmountMinor,
		InvoiceCurrency:      snapshot.InvoiceCurrency,
		ConsigneeID:          snapshot.ConsigneeID,
		OperatorID:           snapshot.OperatorID,
		TraderID:             snapshot.TraderID,
		IsAEO:                snapshot.IsAEO,
	})
	var assessed Declaration
	err = store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		declaration, err := store.getForUpdate(ctx, tx, declarationID)
		if err != nil {
			return err
		}
		if declaration.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		if declaration.Status != StatusSubmitted {
			return ErrInvalidTransition
		}
		now := time.Now().UTC()
		if scoreErr != nil {
			// Fail closed: terminal SCORING_UNAVAILABLE, never auto-laned.
			scoringError := scoreErr.Error()
			unavailable, err := scanDeclaration(tx.QueryRow(ctx, `
				UPDATE customs_declarations
				SET status='SCORING_UNAVAILABLE', scoring_error=$1, updated_at=$2, version=version+1
				WHERE declaration_id=$3 AND status='SUBMITTED' AND version=$4
				RETURNING `+declarationColumns, scoringError, now, declarationID, expectedVersion))
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrOptimisticConflict
			}
			if err != nil {
				return fmt.Errorf("park declaration as scoring-unavailable: %w", err)
			}
			if err := emit(ctx, tx, claims, EventScoringUnavailable, unavailable, map[string]string{
				"declaration-ref": unavailable.DeclarationRef,
			}, principal, now); err != nil {
				return err
			}
			assessed = unavailable
			return nil
		}
		assessment, err := AssignRiskLane(RiskInput{
			Score:                   verdict.Score,
			IsAEO:                   declaration.IsAEO,
			IsSanctioned:            verdict.Sanctioned,
			InvoiceAmountMinor:      declaration.InvoiceAmountMinor,
			HighValueThresholdMinor: highValueThresholdMinor,
			CountryOfOrigin:         declaration.CountryOfOrigin,
			HSCode:                  declaration.HSCode,
		})
		if err != nil {
			return fmt.Errorf("assign risk lane: %w", err)
		}
		lane := assessment.Lane
		finalStatus := LaneStatus(lane)
		var clearedAt *time.Time
		if lane == LaneGreen {
			finalStatus = StatusCleared
			clearedAt = &now
		}
		updated, err := scanDeclaration(tx.QueryRow(ctx, `
			UPDATE customs_declarations
			SET status=$1, risk_lane=$2, risk_score=$3, scoring_model=$4, cleared_at=$5,
				updated_at=$6, version=version+1
			WHERE declaration_id=$7 AND status='SUBMITTED' AND version=$8
			RETURNING `+declarationColumns,
			string(finalStatus), string(lane), assessment.AdjustedScore, verdict.ModelVersion,
			clearedAt, now, declarationID, expectedVersion))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrOptimisticConflict
		}
		if err != nil {
			return fmt.Errorf("lane declaration: %w", err)
		}
		reasonsJSON, err := json.Marshal(assessment.Reasons)
		if err != nil {
			return fmt.Errorf("encode lane reasons: %w", err)
		}
		if err := emit(ctx, tx, claims, EventRiskAssessed, updated, map[string]string{
			"declaration-ref": updated.DeclarationRef,
			"risk-lane":       string(lane),
			"risk-score":      fmt.Sprintf("%d", assessment.AdjustedScore),
			"lane-reasons":    string(reasonsJSON),
		}, principal, now); err != nil {
			return err
		}
		if finalStatus == StatusCleared {
			if err := issueClearance(ctx, tx, claims, updated, principal.ID, now); err != nil {
				return err
			}
			if err := emit(ctx, tx, claims, EventCleared, updated, map[string]string{
				"declaration-ref": updated.DeclarationRef,
				"risk-lane":       string(lane),
			}, principal, now); err != nil {
				return err
			}
		}
		assessed = updated
		return nil
	})
	return assessed, err
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// issueClearance writes the clearance certificate atomically with the CLEARED
// transition. The certificate payload digest is tamper-evident.
func issueClearance(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, declaration Declaration, issuedBy string, now time.Time) error {
	certificateNumber := fmt.Sprintf("CC-%s-R%d", declaration.DeclarationRef, declaration.Revision)
	payload, err := json.Marshal(map[string]any{
		"certificate_number": certificateNumber,
		"declaration_id":     declaration.DeclarationID,
		"declaration_ref":    declaration.DeclarationRef,
		"revision":           declaration.Revision,
		"hs_code":            declaration.HSCode,
		"consignee_id":       declaration.ConsigneeID,
		"operator_id":        declaration.OperatorID,
		"gross_weight_kg":    declaration.GrossWeightKg,
		"total_duty_minor":   declaration.TotalDutyMinor,
		"invoice_currency":   declaration.InvoiceCurrency,
		"issued_by":          issuedBy,
		"issued_at":          now,
	})
	if err != nil {
		return fmt.Errorf("encode clearance certificate: %w", err)
	}
	digest := sha256.Sum256(payload)
	if _, err := tx.Exec(ctx, `
		INSERT INTO declaration_clearance_certificates (
			certificate_id, tenant_id, declaration_id, certificate_number, issued_by, payload_sha256, issued_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		uuid.New(), claims.TenantID, declaration.DeclarationID, certificateNumber, issuedBy,
		"sha256:"+hex.EncodeToString(digest[:]), now); err != nil {
		return fmt.Errorf("issue clearance certificate: %w", err)
	}
	return nil
}

// ClearanceCertificate returns the release document for a CLEARED
// declaration; anything else is fail-closed ErrNotCleared.
func (store *Store) ClearanceCertificate(ctx context.Context, declarationID string) (ClearanceCertificate, Declaration, error) {
	var certificate ClearanceCertificate
	var declaration Declaration
	err := store.withTx(ctx, func(tx pgx.Tx, _ tenantctx.Claims) error {
		found, err := store.getForUpdate(ctx, tx, declarationID)
		if err != nil {
			return err
		}
		if found.Status != StatusCleared {
			return ErrNotCleared
		}
		err = tx.QueryRow(ctx, `
			SELECT certificate_id, declaration_id, certificate_number, issued_by, payload_sha256, issued_at
			FROM declaration_clearance_certificates WHERE declaration_id=$1`, declarationID).
			Scan(&certificate.CertificateID, &certificate.DeclarationID, &certificate.CertificateNumber,
				&certificate.IssuedBy, &certificate.PayloadSHA256, &certificate.IssuedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("clearance certificate missing for cleared declaration %s", declarationID)
		}
		if err != nil {
			return fmt.Errorf("load clearance certificate: %w", err)
		}
		declaration = found
		return nil
	})
	return certificate, declaration, err
}

// Amend supersedes the live revision and writes a new DRAFT revision under
// the same declaration ref. Only pre-terminal revisions are amendable
// (DRAFT, SUBMITTED, REJECTED, SCORING_UNAVAILABLE); a lane-assessed or
// cleared declaration is immutable. Idempotent on the amendment request_id.
func (store *Store) Amend(ctx context.Context, declarationID string, request CreateRequest, expectedVersion int64, principal Principal) (Declaration, error) {
	if err := request.Validate(); err != nil {
		return Declaration{}, err
	}
	if err := requirePrincipal(principal); err != nil {
		return Declaration{}, err
	}
	hsCode, err := NormalizeHSCode(request.HSCode)
	if err != nil {
		return Declaration{}, err
	}
	var amended Declaration
	err = store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		head, err := store.getForUpdate(ctx, tx, declarationID)
		if err != nil {
			return err
		}
		if head.DeclarationRef != request.DeclarationRef {
			return fmt.Errorf("%w: amendments keep the declaration ref", ErrDeclarationInvalid)
		}
		// Idempotent replay: the amendment revision already exists.
		if head.RequestID != request.RequestID {
			if existing, lookupErr := store.getByRequestID(ctx, tx, claims.TenantID, request.RequestID); lookupErr == nil {
				if existing.SupersedesID != nil && *existing.SupersedesID == declarationID {
					amended = existing
					return nil
				}
				return ErrIdempotencyConflict
			}
		}
		if head.Version != expectedVersion {
			return ErrOptimisticConflict
		}
		if !Amendable(head.Status) {
			return ErrInvalidTransition
		}
		now := time.Now().UTC()
		if _, err := tx.Exec(ctx, `
			UPDATE customs_declarations SET status='SUPERSEDED', updated_at=$1, version=version+1
			WHERE declaration_id=$2 AND status=$3 AND version=$4`,
			now, declarationID, head.Status, expectedVersion); err != nil {
			return fmt.Errorf("supersede declaration: %w", err)
		}
		created, err := scanDeclaration(tx.QueryRow(ctx, `
			INSERT INTO customs_declarations (
				declaration_id, tenant_id, request_id, declaration_ref, ucr, revision, supersedes_id,
				trader_id, declaration_type, status, hs_code, goods_description, country_of_origin,
				country_of_destination, port_of_entry, gross_weight_kg, net_weight_kg,
				number_of_packages, consignee_id, operator_id, is_aeo, invoice_amount_minor,
				freight_amount_minor, insurance_amount_minor, invoice_currency, tariff_bps, vat_bps,
				levy_bps, excise_bps, created_at, updated_at, version
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'DRAFT',$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$29,1)
			RETURNING `+declarationColumns,
			uuid.New(), claims.TenantID, request.RequestID, request.DeclarationRef, nilIfEmpty(request.UCR),
			head.Revision+1, declarationID, principal.ID, string(request.DeclarationType), hsCode,
			request.GoodsDescription, request.CountryOfOrigin, nilIfEmpty(request.CountryOfDestination),
			request.PortOfEntry, request.GrossWeightKg, request.NetWeightKg, request.NumberOfPackages,
			request.ConsigneeID, request.OperatorID, request.IsAEO, request.InvoiceAmountMinor,
			request.FreightAmountMinor, request.InsuranceAmountMinor, request.InvoiceCurrency,
			request.TariffBPS, request.VatBPS, request.LevyBPS, request.ExciseBPS, now))
		if err != nil {
			return fmt.Errorf("write amendment revision: %w", err)
		}
		if err := emit(ctx, tx, claims, EventAmended, created, map[string]string{
			"declaration-ref": created.DeclarationRef,
			"revision":        fmt.Sprintf("%d", created.Revision),
			"supersedes":      declarationID,
		}, principal, now); err != nil {
			return err
		}
		amended = created
		return nil
	})
	return amended, err
}

// RegisterPermit links an OGA permit to a declaration revision. It is the
// model port of the donor's multi-agency permit routing; agency decisions
// arrive via DecidePermit.
func (store *Store) RegisterPermit(ctx context.Context, declarationID, agencyCode, agencyName string, permitType *string, slaDeadline *time.Time) (Permit, error) {
	if len(agencyCode) < 2 || len(agencyCode) > 32 || len(agencyName) < 2 || len(agencyName) > 128 {
		return Permit{}, errors.New("permit agency code and name are required")
	}
	var permit Permit
	err := store.withTx(ctx, func(tx pgx.Tx, claims tenantctx.Claims) error {
		now := time.Now().UTC()
		err := tx.QueryRow(ctx, `
			INSERT INTO declaration_permits (
				permit_id, tenant_id, declaration_id, agency_code, agency_name, permit_type,
				status, sla_deadline, created_at, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,'PENDING',$7,$8,$8)
			ON CONFLICT (tenant_id, declaration_id, agency_code, permit_type) DO NOTHING
			RETURNING permit_id, declaration_id, agency_code, agency_name, permit_type, permit_number,
				status, sla_deadline, expires_at, responded_at, created_at, updated_at`,
			uuid.New(), claims.TenantID, declarationID, agencyCode, agencyName, permitType, slaDeadline, now).
			Scan(&permit.PermitID, &permit.DeclarationID, &permit.AgencyCode, &permit.AgencyName,
				&permit.PermitType, &permit.PermitNumber, &permit.Status, &permit.SLADeadline,
				&permit.ExpiresAt, &permit.RespondedAt, &permit.CreatedAt, &permit.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrIdempotencyConflict
		}
		if err != nil {
			return fmt.Errorf("register declaration permit: %w", err)
		}
		return nil
	})
	return permit, err
}
