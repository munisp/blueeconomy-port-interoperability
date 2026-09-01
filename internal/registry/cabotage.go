package registry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantdb"
)

// CabotageRule is the configurable Nigerian Coastal Trade eligibility
-- policy (Coastal and Inland Shipping (Cabotage) Act 2003): a cabotage
// vessel must fly the required flag, meet the minimum national beneficial
-- ownership percentage and, when configured, have been built domestically.
// A ministerial waiver may substitute for an unmet criterion only when the
// rule permits waivers.
type CabotageRule struct {
	RuleID                  string    `json:"ruleId"`
	RequiredFlag            string    `json:"requiredFlag"`
	MinNationalOwnershipPct int       `json:"minNationalOwnershipPct"`
	RequireDomesticBuild    bool      `json:"requireDomesticBuild"`
	WaiverAllowed           bool      `json:"waiverAllowed"`
	Status                  string    `json:"status"` // ACTIVE | RETIRED
	EffectiveFrom           time.Time `json:"effectiveFrom"`
	CreatedBy               string    `json:"createdBy"`
	CreatedAt               time.Time `json:"createdAt"`
}

// CabotageFacts are the vessel/ownership facts evaluated against a rule.
type CabotageFacts struct {
	FlagState            string `json:"flagState"`
	BuildCountry         string `json:"buildCountry"`
	NationalOwnershipPct int    `json:"nationalOwnershipPct"`
}

// Eligibility is the deterministic evaluation of facts against a rule.
// Eligible is true only when every required criterion is met; otherwise the
-- unmet criteria are reported and a waiver may still rescue the application
// when the rule allows it.
type Eligibility struct {
	FlagCriterionMet      bool     `json:"flagCriterionMet"`
	OwnershipCriterionMet bool     `json:"ownershipCriterionMet"`
	BuildCriterionMet     bool     `json:"buildCriterionMet"`
	Eligible              bool     `json:"eligible"`
	Waiverable            bool     `json:"waiverable"`
	Unmet                 []string `json:"unmet"`
}

// EvaluateCabotage applies a rule to vessel facts. Pure and total: no I/O,
-- nil rule fails closed (not eligible, not waiverable).
func EvaluateCabotage(rule *CabotageRule, facts CabotageFacts) Eligibility {
	if rule == nil {
		return Eligibility{Unmet: []string{"no ACTIVE cabotage rule"}}
	}
	result := Eligibility{
		FlagCriterionMet:      facts.FlagState == rule.RequiredFlag,
		OwnershipCriterionMet: facts.NationalOwnershipPct >= rule.MinNationalOwnershipPct,
		BuildCriterionMet:     true,
	}
	if rule.RequireDomesticBuild {
		result.BuildCriterionMet = facts.BuildCountry == rule.RequiredFlag
	}
	if !result.FlagCriterionMet {
		result.Unmet = append(result.Unmet, "flag")
	}
	if !result.OwnershipCriterionMet {
		result.Unmet = append(result.Unmet, "ownership")
	}
	if !result.BuildCriterionMet {
		result.Unmet = append(result.Unmet, "build")
	}
	result.Eligible = len(result.Unmet) == 0
	result.Waiverable = !result.Eligible && rule.WaiverAllowed
	return result
}

// PermitStatus is the cabotage permit workflow state.
type PermitStatus string

const (
	PermitApplication PermitStatus = "APPLICATION"
	PermitApproved    PermitStatus = "APPROVED"
	PermitRejected    PermitStatus = "REJECTED"
	PermitRevoked     PermitStatus = "REVOKED"
)

// CabotagePermit is a coastal-trade permit application/grant.
type CabotagePermit struct {
	PermitID              string       `json:"permitId"`
	VesselID              string       `json:"vesselId"`
	RuleID                string       `json:"ruleId"`
	FlagCriterionMet      bool         `json:"flagCriterionMet"`
	OwnershipCriterionMet bool         `json:"ownershipCriterionMet"`
	BuildCriterionMet     bool         `json:"buildCriterionMet"`
	WaiverReference       string       `json:"waiverReference,omitempty"`
	NationalOwnershipPct  int          `json:"nationalOwnershipPct"`
	TradeRoute            string       `json:"tradeRoute"`
	Status                PermitStatus `json:"status"`
	AppliedBy             string       `json:"appliedBy"`
	DecidedBy             string       `json:"decidedBy,omitempty"`
	CreatedAt             time.Time    `json:"createdAt"`
	UpdatedAt             time.Time    `json:"updatedAt"`
	Version               int          `json:"version"`
}

// ApplyPermitRequest opens a cabotage permit application for a vessel.
type ApplyPermitRequest struct {
	PermitID             string `json:"permitId"`
	VesselID             string `json:"vesselId"`
	NationalOwnershipPct int    `json:"nationalOwnershipPct"`
	TradeRoute           string `json:"tradeRoute"`
	WaiverReference      string `json:"waiverReference,omitempty"`
}

// Violation is a cabotage violation flag linked to the vessel registry.
type Violation struct {
	ViolationID   string     `json:"violationId"`
	VesselID      string     `json:"vesselId"`
	PermitID      string     `json:"permitId,omitempty"`
	ViolationType string     `json:"violationType"`
	Detail        string     `json:"detail"`
	Status        string     `json:"status"` // OPEN | RESOLVED
	FlaggedBy     string     `json:"flaggedBy"`
	FlaggedAt     time.Time  `json:"flaggedAt"`
	ResolvedAt    *time.Time `json:"resolvedAt,omitempty"`
}

// UpsertCabotageRule installs a new ACTIVE rule, retiring the currently
-- active rule in the same transaction so exactly one rule governs at any
// time (the partial unique index enforces it). Rule change is governance,
// so it requires a verified principal and emits registry.cabotage.v1.
func (store *Store) UpsertCabotageRule(ctx context.Context, idempotencyKey string, rule CabotageRule, principal Principal) (CabotageRule, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return CabotageRule{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if !identifier.MatchString(rule.RuleID) {
		return CabotageRule{}, errors.New("ruleId must be 1-64 characters of [A-Za-z0-9._:-]")
	}
	if !countryCode.MatchString(rule.RequiredFlag) {
		return CabotageRule{}, errors.New("requiredFlag must be an ISO 3166-1 alpha-2 code")
	}
	if rule.MinNationalOwnershipPct < 0 || rule.MinNationalOwnershipPct > 100 {
		return CabotageRule{}, errors.New("minNationalOwnershipPct must be 0-100")
	}
	if !principal.valid() {
		return CabotageRule{}, errors.New("a verified principal is required")
	}
	var installed CabotageRule
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		now := time.Now().UTC()
		if _, err := tx.Exec(ctx, `UPDATE registry_cabotage_rules SET status = 'RETIRED' WHERE status = 'ACTIVE'`); err != nil {
			return fmt.Errorf("retire active cabotage rule: %w", err)
		}
		installed = rule
		installed.Status = "ACTIVE"
		installed.EffectiveFrom = now
		installed.CreatedBy = principal.ID
		installed.CreatedAt = now
		if _, err := tx.Exec(ctx, `
			INSERT INTO registry_cabotage_rules
				(tenant_id, rule_id, required_flag, min_national_ownership_pct, require_domestic_build, waiver_allowed, status, effective_from, created_by, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, 'ACTIVE', $7, $8, $7)
			ON CONFLICT (tenant_id, rule_id) DO UPDATE
			SET required_flag = EXCLUDED.required_flag,
			    min_national_ownership_pct = EXCLUDED.min_national_ownership_pct,
			    require_domestic_build = EXCLUDED.require_domestic_build,
			    waiver_allowed = EXCLUDED.waiver_allowed,
			    status = 'ACTIVE', effective_from = EXCLUDED.effective_from`,
			claims.TenantID, rule.RuleID, rule.RequiredFlag, rule.MinNationalOwnershipPct,
			rule.RequireDomesticBuild, rule.WaiverAllowed, now, principal.ID); err != nil {
			return fmt.Errorf("install cabotage rule: %w", err)
		}
		return emit(ctx, tx, claims, events.TopicRegistryCabotage, "registry.cabotage.rule-installed", idempotencyKey, rule.RuleID, map[string]string{
			"ruleId":       rule.RuleID,
			"requiredFlag": rule.RequiredFlag,
		}, map[string]string{
			"rule": rule.RuleID,
		}, principal, now, store.signer)
	})
	return installed, err
}

func activeRule(ctx context.Context, tx pgx.Tx) (*CabotageRule, error) {
	var rule CabotageRule
	err := tx.QueryRow(ctx, `
		SELECT rule_id, required_flag, min_national_ownership_pct, require_domestic_build, waiver_allowed, status, effective_from, created_by, created_at
		FROM registry_cabotage_rules WHERE status = 'ACTIVE'`).Scan(
		&rule.RuleID, &rule.RequiredFlag, &rule.MinNationalOwnershipPct, &rule.RequireDomesticBuild,
		&rule.WaiverAllowed, &rule.Status, &rule.EffectiveFrom, &rule.CreatedBy, &rule.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read active cabotage rule: %w", err)
	}
	return &rule, nil
}

const permitColumns = `permit_id, vessel_id, rule_id, flag_criterion_met, ownership_criterion_met, build_criterion_met,
	COALESCE(waiver_reference, ''), national_ownership_pct, trade_route, status, applied_by, COALESCE(decided_by, ''),
	created_at, updated_at, version`

func scanPermit(row pgx.Row) (CabotagePermit, error) {
	var permit CabotagePermit
	err := row.Scan(&permit.PermitID, &permit.VesselID, &permit.RuleID, &permit.FlagCriterionMet,
		&permit.OwnershipCriterionMet, &permit.BuildCriterionMet, &permit.WaiverReference,
		&permit.NationalOwnershipPct, &permit.TradeRoute, &permit.Status, &permit.AppliedBy,
		&permit.DecidedBy, &permit.CreatedAt, &permit.UpdatedAt, &permit.Version)
	return permit, err
}

// ApplyPermit opens a cabotage permit application. Eligibility is evaluated
-- against the ACTIVE rule at application time and snapshotted onto the
// permit; when no ACTIVE rule exists the application fails closed.
func (store *Store) ApplyPermit(ctx context.Context, idempotencyKey string, request ApplyPermitRequest, principal Principal) (CabotagePermit, Eligibility, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return CabotagePermit{}, Eligibility{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if !identifier.MatchString(request.PermitID) {
		return CabotagePermit{}, Eligibility{}, errors.New("permitId must be 1-64 characters of [A-Za-z0-9._:-]")
	}
	if strings.TrimSpace(request.TradeRoute) == "" || len(request.TradeRoute) > 256 {
		return CabotagePermit{}, Eligibility{}, errors.New("tradeRoute must be 1-256 characters")
	}
	if request.NationalOwnershipPct < 0 || request.NationalOwnershipPct > 100 {
		return CabotagePermit{}, Eligibility{}, errors.New("nationalOwnershipPct must be 0-100")
	}
	if request.WaiverReference != "" && len(request.WaiverReference) < 4 {
		return CabotagePermit{}, Eligibility{}, errors.New("waiverReference must be at least 4 characters when present")
	}
	if !principal.valid() {
		return CabotagePermit{}, Eligibility{}, errors.New("a verified principal is required")
	}
	var (
		permit CabotagePermit
		result Eligibility
	)
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		vessel, err := scanVessel(tx.QueryRow(ctx,
			`SELECT `+vesselColumns+` FROM registry_vessels WHERE vessel_id = $1 FOR SHARE`, request.VesselID))
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: vessel %s", ErrNotFound, request.VesselID)
		}
		if err != nil {
			return fmt.Errorf("lock vessel: %w", err)
		}
		if vessel.Status != VesselCertificateIssued {
			return fmt.Errorf("%w: vessel %s is %s; only CERTIFICATE_ISSUED vessels may trade coastal", ErrConflict, request.VesselID, vessel.Status)
		}
		rule, err := activeRule(ctx, tx)
		if err != nil {
			return err
		}
		if rule == nil {
			return fmt.Errorf("%w: no ACTIVE cabotage rule is installed for the tenant", ErrConflict)
		}
		result = EvaluateCabotage(rule, CabotageFacts{
			FlagState:            vessel.FlagState,
			BuildCountry:         vessel.BuildCountry,
			NationalOwnershipPct: request.NationalOwnershipPct,
		})
		// An application with unmet criteria requires a waiver reference up
		// front, and the rule must permit waivers at all.
		if !result.Eligible {
			if !result.Waiverable {
				return fmt.Errorf("%w: vessel %s fails cabotage criteria (%s) and the active rule permits no waiver",
					ErrConflict, request.VesselID, strings.Join(result.Unmet, ","))
			}
			if request.WaiverReference == "" {
				return fmt.Errorf("%w: vessel %s fails cabotage criteria (%s); a ministerial waiver reference is required",
					ErrConflict, request.VesselID, strings.Join(result.Unmet, ","))
			}
		}
		now := time.Now().UTC()
		created, err := scanPermit(tx.QueryRow(ctx, `
			INSERT INTO registry_cabotage_permits
				(tenant_id, permit_id, idempotency_key, vessel_id, rule_id, flag_criterion_met, ownership_criterion_met,
				 build_criterion_met, waiver_reference, national_ownership_pct, trade_route, status, applied_by, created_at, updated_at, version)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), $10, $11, 'APPLICATION', $12, $13, $13, 1)
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING `+permitColumns,
			claims.TenantID, request.PermitID, idempotencyKey, request.VesselID, rule.RuleID,
			result.FlagCriterionMet, result.OwnershipCriterionMet, result.BuildCriterionMet,
			request.WaiverReference, request.NationalOwnershipPct, request.TradeRoute, principal.ID, now))
		if errors.Is(err, pgx.ErrNoRows) {
			existing, lookupErr := scanPermit(tx.QueryRow(ctx,
				`SELECT `+permitColumns+` FROM registry_cabotage_permits WHERE idempotency_key = $1`, idempotencyKey))
			if lookupErr != nil {
				return fmt.Errorf("resolve idempotent permit application: %w", lookupErr)
			}
			permit = existing
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert cabotage permit: %w", err)
		}
		if err := emit(ctx, tx, claims, events.TopicRegistryCabotage, "registry.cabotage.permit-applied", idempotencyKey, created.PermitID, map[string]string{
			"permitId": created.PermitID,
			"vesselId": created.VesselID,
			"ruleId":   created.RuleID,
			"eligible": fmt.Sprintf("%t", result.Eligible),
		}, map[string]string{
			"vessel": created.VesselID,
			"permit": created.PermitID,
		}, principal, now, store.signer); err != nil {
			return err
		}
		permit = created
		return nil
	})
	return permit, result, err
}

// DecidePermit is the checker step: approve or reject an APPLICATION. The
-- deciding officer must differ from the applicant (maker-checker); approval
// of a permit with unmet criteria requires the waiver reference recorded at
// application time (enforced by the CHECK constraint too).
func (store *Store) DecidePermit(ctx context.Context, idempotencyKey, permitID string, approve bool, principal Principal) (CabotagePermit, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return CabotagePermit{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if !principal.valid() {
		return CabotagePermit{}, errors.New("a verified principal is required")
	}
	var permit CabotagePermit
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		current, err := scanPermit(tx.QueryRow(ctx,
			`SELECT `+permitColumns+` FROM registry_cabotage_permits WHERE permit_id = $1 FOR UPDATE`, permitID))
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: permit %s", ErrNotFound, permitID)
		}
		if err != nil {
			return fmt.Errorf("lock permit: %w", err)
		}
		if current.Status != PermitApplication {
			return fmt.Errorf("%w: permit %s is %s, not APPLICATION", ErrConflict, permitID, current.Status)
		}
		if principal.ID == current.AppliedBy {
			return fmt.Errorf("%w: permit %s decided by its applicant", ErrMakerChecker, permitID)
		}
		target := PermitRejected
		if approve {
			allMet := current.FlagCriterionMet && current.OwnershipCriterionMet && current.BuildCriterionMet
			if !allMet && current.WaiverReference == "" {
				return fmt.Errorf("%w: permit %s has unmet criteria and no waiver reference", ErrConflict, permitID)
			}
			target = PermitApproved
		}
		now := time.Now().UTC()
		updated, err := scanPermit(tx.QueryRow(ctx, `
			UPDATE registry_cabotage_permits
			SET status = $3, decided_by = $4, updated_at = $5, version = version + 1
			WHERE permit_id = $1 AND version = $2
			RETURNING `+permitColumns, permitID, current.Version, string(target), principal.ID, now))
		if err != nil {
			return fmt.Errorf("decide permit: %w", err)
		}
		if err := emit(ctx, tx, claims, events.TopicRegistryCabotage, "registry.cabotage.permit-decided", idempotencyKey, permitID, map[string]string{
			"permitId": permitID,
			"vesselId": current.VesselID,
			"decision": string(target),
		}, map[string]string{
			"permit": permitID,
			"status": string(target),
		}, principal, now, store.signer); err != nil {
			return err
		}
		permit = updated
		return nil
	})
	return permit, err
}

// GetPermit returns one permit visible to the tenant.
func (store *Store) GetPermit(ctx context.Context, permitID string) (CabotagePermit, error) {
	var permit CabotagePermit
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, _ tenantctx.Claims) error {
		found, err := scanPermit(tx.QueryRow(ctx,
			`SELECT `+permitColumns+` FROM registry_cabotage_permits WHERE permit_id = $1`, permitID))
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: permit %s", ErrNotFound, permitID)
		}
		if err != nil {
			return fmt.Errorf("read permit: %w", err)
		}
		permit = found
		return nil
	})
	return permit, err
}

// FlagViolation raises a cabotage violation flag against a vessel (and,
-- when given, the permit). Open violations are visible to enforcement and
// the vessel registry in the same tenant scope.
func (store *Store) FlagViolation(ctx context.Context, idempotencyKey string, violation Violation, principal Principal) (Violation, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return Violation{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if !identifier.MatchString(violation.ViolationID) {
		return Violation{}, errors.New("violationId must be 1-64 characters of [A-Za-z0-9._:-]")
	}
	switch violation.ViolationType {
	case "NO_PERMIT", "EXPIRED_PERMIT", "ROUTE_OUTSIDE_PERMIT", "CRITERION_LAPSED":
	default:
		return Violation{}, fmt.Errorf("violationType %q is not admitted", violation.ViolationType)
	}
	if strings.TrimSpace(violation.Detail) == "" || len(violation.Detail) > 1024 {
		return Violation{}, errors.New("detail must be 1-1024 characters")
	}
	if violation.PermitID == "" && violation.ViolationType != "NO_PERMIT" {
		return Violation{}, errors.New("permitId is required unless the violation is NO_PERMIT")
	}
	if !principal.valid() {
		return Violation{}, errors.New("a verified principal is required")
	}
	var flagged Violation
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		var exists string
		if err := tx.QueryRow(ctx, `SELECT vessel_id FROM registry_vessels WHERE vessel_id = $1 FOR SHARE`, violation.VesselID).Scan(&exists); errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: vessel %s", ErrNotFound, violation.VesselID)
		} else if err != nil {
			return fmt.Errorf("verify vessel: %w", err)
		}
		if violation.PermitID != "" {
			if err := tx.QueryRow(ctx, `SELECT permit_id FROM registry_cabotage_permits WHERE permit_id = $1 AND vessel_id = $2 FOR SHARE`, violation.PermitID, violation.VesselID).Scan(&exists); errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: permit %s on vessel %s", ErrNotFound, violation.PermitID, violation.VesselID)
			} else if err != nil {
				return fmt.Errorf("verify permit: %w", err)
			}
		}
		now := time.Now().UTC()
		flagged = violation
		flagged.Status = "OPEN"
		flagged.FlaggedBy = principal.ID
		flagged.FlaggedAt = now
		if _, err := tx.Exec(ctx, `
			INSERT INTO registry_cabotage_violations
				(tenant_id, violation_id, vessel_id, permit_id, violation_type, detail, status, flagged_by, flagged_at)
			VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, 'OPEN', $7, $8)`,
			claims.TenantID, violation.ViolationID, violation.VesselID, violation.PermitID,
			violation.ViolationType, violation.Detail, principal.ID, now); err != nil {
			return fmt.Errorf("flag cabotage violation: %w", err)
		}
		return emit(ctx, tx, claims, events.TopicRegistryCabotage, "registry.cabotage.violation-flagged", idempotencyKey, violation.ViolationID, map[string]string{
			"violationId":   violation.ViolationID,
			"vesselId":      violation.VesselID,
			"violationType": violation.ViolationType,
		}, map[string]string{
			"vessel": violation.VesselID,
		}, principal, now, store.signer)
	})
	return flagged, err
}

// ResolveViolation closes an OPEN violation; the resolver must differ from
-- the flagging officer (maker-checker on enforcement closure).
func (store *Store) ResolveViolation(ctx context.Context, idempotencyKey, violationID string, principal Principal) (Violation, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return Violation{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if !principal.valid() {
		return Violation{}, errors.New("a verified principal is required")
	}
	var resolved Violation
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		var (
			vesselID, permitID, violationType, detail, flaggedBy string
			flaggedAt                                            time.Time
		)
		err := tx.QueryRow(ctx, `
			SELECT vessel_id, COALESCE(permit_id, ''), violation_type, detail, flagged_by, flagged_at
			FROM registry_cabotage_violations WHERE violation_id = $1 AND status = 'OPEN' FOR UPDATE`,
			violationID).Scan(&vesselID, &permitID, &violationType, &detail, &flaggedBy, &flaggedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: open violation %s", ErrNotFound, violationID)
		}
		if err != nil {
			return fmt.Errorf("lock violation: %w", err)
		}
		if principal.ID == flaggedBy {
			return fmt.Errorf("%w: violation %s resolved by its flagging officer", ErrMakerChecker, violationID)
		}
		now := time.Now().UTC()
		if _, err := tx.Exec(ctx, `
			UPDATE registry_cabotage_violations SET status = 'RESOLVED', resolved_at = $2 WHERE violation_id = $1`,
			violationID, now); err != nil {
			return fmt.Errorf("resolve violation: %w", err)
		}
		resolved = Violation{
			ViolationID: violationID, VesselID: vesselID, PermitID: permitID,
			ViolationType: violationType, Detail: detail, Status: "RESOLVED",
			FlaggedBy: flaggedBy, FlaggedAt: flaggedAt, ResolvedAt: &now,
		}
		return emit(ctx, tx, claims, events.TopicRegistryCabotage, "registry.cabotage.violation-resolved", idempotencyKey, violationID, map[string]string{
			"violationId": violationID,
			"vesselId":    vesselID,
		}, map[string]string{
			"vessel": vesselID,
		}, principal, now, store.signer)
	})
	return resolved, err
}
