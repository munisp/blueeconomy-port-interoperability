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

// FisheriesPermitType classifies a fisheries licence: artisanal and
// industrial permits bind a registered vessel; aquaculture permits bind a
// farm site reference (lease / cage registry id).
type FisheriesPermitType string

const (
	FisheriesArtisanal   FisheriesPermitType = "ARTISANAL"
	FisheriesIndustrial  FisheriesPermitType = "INDUSTRIAL"
	FisheriesAquaculture FisheriesPermitType = "AQUACULTURE"
)

func (kind FisheriesPermitType) admitted() bool {
	switch kind {
	case FisheriesArtisanal, FisheriesIndustrial, FisheriesAquaculture:
		return true
	default:
		return false
	}
}

// FisheriesPermitStatus is the licence workflow state.
type FisheriesPermitStatus string

const (
	FisheriesApplication FisheriesPermitStatus = "APPLICATION"
	FisheriesGranted     FisheriesPermitStatus = "GRANTED"
	FisheriesSuspended   FisheriesPermitStatus = "SUSPENDED"
	FisheriesRevoked     FisheriesPermitStatus = "REVOKED"
)

// FisheriesPermit is a fisheries licence application/grant. The permit
// number is the public identifier used by the minimal-disclosure
// verification endpoint.
type FisheriesPermit struct {
	PermitID      string                `json:"permitId"`
	PermitNumber  string                `json:"permitNumber"`
	PermitType    FisheriesPermitType   `json:"permitType"`
	VesselID      string                `json:"vesselId,omitempty"`
	OwnerName     string                `json:"ownerName"`
	SiteReference string                `json:"siteReference,omitempty"`
	ValidFrom     time.Time             `json:"validFrom"`
	ValidTo       time.Time             `json:"validTo"`
	Status        FisheriesPermitStatus `json:"status"`
	AppliedBy     string                `json:"appliedBy"`
	DecidedBy     string                `json:"decidedBy,omitempty"`
	CreatedAt     time.Time             `json:"createdAt"`
	UpdatedAt     time.Time             `json:"updatedAt"`
	Version       int                   `json:"version"`
}

// ApplyFisheriesPermitRequest opens a fisheries permit application.
type ApplyFisheriesPermitRequest struct {
	PermitID      string              `json:"permitId"`
	PermitNumber  string              `json:"permitNumber"`
	PermitType    FisheriesPermitType `json:"permitType"`
	VesselID      string              `json:"vesselId,omitempty"`
	OwnerName     string              `json:"ownerName"`
	SiteReference string              `json:"siteReference,omitempty"`
	ValidFrom     time.Time           `json:"validFrom"`
	ValidTo       time.Time           `json:"validTo"`
}

// FisheriesAuditEntry is one append-only lifecycle record for a permit.
type FisheriesAuditEntry struct {
	AuditID    int64     `json:"auditId"`
	PermitID   string    `json:"permitId"`
	Action     string    `json:"action"`
	Actor      string    `json:"actor"`
	Detail     string    `json:"detail"`
	OccurredAt time.Time `json:"occurredAt"`
}

// FisheriesVerification is the minimal-disclosure outcome of the public
// permit verification endpoint: validity, type and expiry only — owner,
// vessel and site attributes are never disclosed here.
type FisheriesVerification struct {
	PermitNumber string              `json:"permitNumber"`
	PermitType   FisheriesPermitType `json:"permitType"`
	Valid        bool                `json:"valid"`
	ValidTo      time.Time           `json:"validTo"`
	CheckedAt    time.Time           `json:"checkedAt"`
}

func validFisheriesTransition(current, target FisheriesPermitStatus) bool {
	switch current {
	case FisheriesGranted:
		return target == FisheriesSuspended || target == FisheriesRevoked
	case FisheriesSuspended:
		return target == FisheriesGranted || target == FisheriesRevoked
	default:
		return false
	}
}

// validateFisheriesRequest enforces the request invariants fail-closed
// before any database work: the DB CHECK constraints mirror them.
func validateFisheriesRequest(request ApplyFisheriesPermitRequest) error {
	if !identifier.MatchString(request.PermitID) {
		return errors.New("permitId must be 1-64 characters of [A-Za-z0-9._:-]")
	}
	if len(request.PermitNumber) < 4 || len(request.PermitNumber) > 64 {
		return errors.New("permitNumber must be 4-64 characters")
	}
	if !request.PermitType.admitted() {
		return fmt.Errorf("permitType %q is not admitted (ARTISANAL, INDUSTRIAL, AQUACULTURE)", request.PermitType)
	}
	if strings.TrimSpace(request.OwnerName) == "" || len(request.OwnerName) > 256 {
		return errors.New("ownerName must be 1-256 characters")
	}
	if request.PermitType == FisheriesAquaculture {
		if strings.TrimSpace(request.SiteReference) == "" || len(request.SiteReference) > 256 {
			return errors.New("siteReference must be 1-256 characters for AQUACULTURE permits")
		}
	} else {
		if !identifier.MatchString(request.VesselID) {
			return errors.New("vesselId must be 1-64 characters of [A-Za-z0-9._:-] for ARTISANAL/INDUSTRIAL permits")
		}
	}
	if request.ValidTo.IsZero() || !request.ValidTo.After(request.ValidFrom) {
		return errors.New("validTo must be after validFrom")
	}
	return nil
}

const fisheriesColumns = `permit_id, permit_number, permit_type, COALESCE(vessel_id, ''), owner_name,
	COALESCE(site_reference, ''), valid_from, valid_to, status, applied_by, COALESCE(decided_by, ''),
	created_at, updated_at, version`

func scanFisheriesPermit(row pgx.Row) (FisheriesPermit, error) {
	var permit FisheriesPermit
	err := row.Scan(&permit.PermitID, &permit.PermitNumber, &permit.PermitType, &permit.VesselID,
		&permit.OwnerName, &permit.SiteReference, &permit.ValidFrom, &permit.ValidTo, &permit.Status,
		&permit.AppliedBy, &permit.DecidedBy, &permit.CreatedAt, &permit.UpdatedAt, &permit.Version)
	return permit, err
}

func insertFisheriesAudit(ctx context.Context, tx pgx.Tx, tenantID, permitID, action, actor, detail string, occurredAt time.Time) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO registry_fisheries_permit_audit (tenant_id, permit_id, action, actor, detail, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		tenantID, permitID, action, actor, detail, occurredAt); err != nil {
		return fmt.Errorf("append fisheries audit %s: %w", action, err)
	}
	return nil
}

// ApplyFisheriesPermit opens a fisheries permit application. Vessel-backed
// permits require the vessel to exist in the ship registry; the recorded
// owner is snapshotted from the application, and the current registry owner
// must match it (vessel+owner linkage), failing closed otherwise.
func (store *Store) ApplyFisheriesPermit(ctx context.Context, idempotencyKey string, request ApplyFisheriesPermitRequest, principal Principal) (FisheriesPermit, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return FisheriesPermit{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if err := validateFisheriesRequest(request); err != nil {
		return FisheriesPermit{}, err
	}
	if !principal.valid() {
		return FisheriesPermit{}, errors.New("a verified principal is required")
	}
	var permit FisheriesPermit
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		if request.PermitType != FisheriesAquaculture {
			vessel, err := scanVessel(tx.QueryRow(ctx,
				`SELECT `+vesselColumns+` FROM registry_vessels WHERE vessel_id = $1 FOR SHARE`, request.VesselID))
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: vessel %s", ErrNotFound, request.VesselID)
			}
			if err != nil {
				return fmt.Errorf("lock vessel: %w", err)
			}
			if vessel.Status != VesselCertificateIssued {
				return fmt.Errorf("%w: vessel %s is %s; only CERTIFICATE_ISSUED vessels may hold a fisheries permit", ErrConflict, request.VesselID, vessel.Status)
			}
			if !strings.EqualFold(strings.TrimSpace(vessel.OwnerName), strings.TrimSpace(request.OwnerName)) {
				return fmt.Errorf("%w: ownerName does not match the registered owner of vessel %s", ErrConflict, request.VesselID)
			}
		}
		now := time.Now().UTC()
		created, err := scanFisheriesPermit(tx.QueryRow(ctx, `
			INSERT INTO registry_fisheries_permits
				(tenant_id, permit_id, idempotency_key, permit_number, permit_type, vessel_id, owner_name,
				 site_reference, valid_from, valid_to, status, applied_by, created_at, updated_at, version)
			VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, NULLIF($8, ''), $9, $10, 'APPLICATION', $11, $12, $12, 1)
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING `+fisheriesColumns,
			claims.TenantID, request.PermitID, idempotencyKey, request.PermitNumber, string(request.PermitType),
			request.VesselID, request.OwnerName, request.SiteReference, request.ValidFrom, request.ValidTo,
			principal.ID, now))
		if errors.Is(err, pgx.ErrNoRows) {
			existing, lookupErr := scanFisheriesPermit(tx.QueryRow(ctx,
				`SELECT `+fisheriesColumns+` FROM registry_fisheries_permits WHERE idempotency_key = $1`, idempotencyKey))
			if lookupErr != nil {
				return fmt.Errorf("resolve idempotent fisheries application: %w", lookupErr)
			}
			permit = existing
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert fisheries permit: %w", err)
		}
		if err := insertFisheriesAudit(ctx, tx, claims.TenantID, created.PermitID, "APPLIED", principal.ID, "application opened", now); err != nil {
			return err
		}
		if err := emit(ctx, tx, claims, events.TopicRegistryFisheries, "registry.fisheries.permit-applied", idempotencyKey, created.PermitID, map[string]string{
			"permitId":     created.PermitID,
			"permitNumber": created.PermitNumber,
			"permitType":   string(created.PermitType),
			"vesselId":     created.VesselID,
		}, map[string]string{
			"permit": created.PermitID,
		}, principal, now, store.signer); err != nil {
			return err
		}
		permit = created
		return nil
	})
	return permit, err
}

// DecideFisheriesPermit is the checker step: grant or reject an
// APPLICATION. The deciding officer must differ from the applicant
// (maker-checker, enforced by the CHECK constraint too).
func (store *Store) DecideFisheriesPermit(ctx context.Context, idempotencyKey, permitID string, grant bool, principal Principal) (FisheriesPermit, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return FisheriesPermit{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if !principal.valid() {
		return FisheriesPermit{}, errors.New("a verified principal is required")
	}
	var permit FisheriesPermit
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		current, err := scanFisheriesPermit(tx.QueryRow(ctx,
			`SELECT `+fisheriesColumns+` FROM registry_fisheries_permits WHERE permit_id = $1 FOR UPDATE`, permitID))
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: fisheries permit %s", ErrNotFound, permitID)
		}
		if err != nil {
			return fmt.Errorf("lock fisheries permit: %w", err)
		}
		if current.Status != FisheriesApplication {
			return fmt.Errorf("%w: fisheries permit %s is %s, not APPLICATION", ErrConflict, permitID, current.Status)
		}
		if principal.ID == current.AppliedBy {
			return fmt.Errorf("%w: fisheries permit %s decided by its applicant", ErrMakerChecker, permitID)
		}
		now := time.Now().UTC()
		if !grant {
			// Rejection is terminal; REJECTED sits outside the open-permit
			// partial index so the vessel's open slot frees up.
			rejected, err := scanFisheriesPermit(tx.QueryRow(ctx, `
				UPDATE registry_fisheries_permits
				SET status = 'REJECTED', decided_by = $3, updated_at = $4, version = version + 1
				WHERE permit_id = $1 AND version = $2
				RETURNING `+fisheriesColumns, permitID, current.Version, principal.ID, now))
			if err != nil {
				return fmt.Errorf("reject fisheries permit: %w", err)
			}
			if err := insertFisheriesAudit(ctx, tx, claims.TenantID, permitID, "REJECTED", principal.ID, "application rejected", now); err != nil {
				return err
			}
			if err := emit(ctx, tx, claims, events.TopicRegistryFisheries, "registry.fisheries.permit-rejected", idempotencyKey, permitID, map[string]string{
				"permitId": permitID,
			}, map[string]string{
				"permit": permitID,
				"status": "REJECTED",
			}, principal, now, store.signer); err != nil {
				return err
			}
			permit = rejected
			return nil
		}
		updated, err := scanFisheriesPermit(tx.QueryRow(ctx, `
			UPDATE registry_fisheries_permits
			SET status = 'GRANTED', decided_by = $3, updated_at = $4, version = version + 1
			WHERE permit_id = $1 AND version = $2
			RETURNING `+fisheriesColumns, permitID, current.Version, principal.ID, now))
		if err != nil {
			return fmt.Errorf("grant fisheries permit: %w", err)
		}
		if err := insertFisheriesAudit(ctx, tx, claims.TenantID, permitID, "GRANTED", principal.ID, "permit granted", now); err != nil {
			return err
		}
		if err := emit(ctx, tx, claims, events.TopicRegistryFisheries, "registry.fisheries.permit-granted", idempotencyKey, permitID, map[string]string{
			"permitId":     permitID,
			"permitNumber": updated.PermitNumber,
		}, map[string]string{
			"permit": permitID,
			"status": string(FisheriesGranted),
		}, principal, now, store.signer); err != nil {
			return err
		}
		permit = updated
		return nil
	})
	return permit, err
}

// TransitionFisheriesPermit moves a GRANTED/SUSPENDED permit through the
// suspension/revocation lifecycle. The acting officer must differ from the
// applicant (maker-checker on enforcement actions); revocation is terminal.
func (store *Store) TransitionFisheriesPermit(ctx context.Context, idempotencyKey, permitID string, target FisheriesPermitStatus, reason string, principal Principal) (FisheriesPermit, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return FisheriesPermit{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	switch target {
	case FisheriesGranted, FisheriesSuspended, FisheriesRevoked:
	default:
		return FisheriesPermit{}, fmt.Errorf("target status %q is not an admitted transition", target)
	}
	if strings.TrimSpace(reason) == "" || len(reason) > 1024 {
		return FisheriesPermit{}, errors.New("reason must be 1-1024 characters")
	}
	if !principal.valid() {
		return FisheriesPermit{}, errors.New("a verified principal is required")
	}
	var permit FisheriesPermit
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		current, err := scanFisheriesPermit(tx.QueryRow(ctx,
			`SELECT `+fisheriesColumns+` FROM registry_fisheries_permits WHERE permit_id = $1 FOR UPDATE`, permitID))
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: fisheries permit %s", ErrNotFound, permitID)
		}
		if err != nil {
			return fmt.Errorf("lock fisheries permit: %w", err)
		}
		if !validFisheriesTransition(current.Status, target) {
			return fmt.Errorf("%w: fisheries permit %s cannot move %s -> %s", ErrConflict, permitID, current.Status, target)
		}
		if principal.ID == current.AppliedBy {
			return fmt.Errorf("%w: fisheries permit %s transitioned by its applicant", ErrMakerChecker, permitID)
		}
		now := time.Now().UTC()
		updated, err := scanFisheriesPermit(tx.QueryRow(ctx, `
			UPDATE registry_fisheries_permits
			SET status = $3, updated_at = $4, version = version + 1
			WHERE permit_id = $1 AND version = $2
			RETURNING `+fisheriesColumns, permitID, current.Version, string(target), now))
		if err != nil {
			return fmt.Errorf("transition fisheries permit: %w", err)
		}
		action := map[FisheriesPermitStatus]string{
			FisheriesSuspended: "SUSPENDED",
			FisheriesRevoked:   "REVOKED",
			FisheriesGranted:   "REINSTATED",
		}[target]
		if err := insertFisheriesAudit(ctx, tx, claims.TenantID, permitID, action, principal.ID, reason, now); err != nil {
			return err
		}
		if err := emit(ctx, tx, claims, events.TopicRegistryFisheries, "registry.fisheries.permit-transitioned", idempotencyKey, permitID, map[string]string{
			"permitId": permitID,
			"from":     string(current.Status),
			"to":       string(target),
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

// GetFisheriesPermit returns one permit visible to the tenant.
func (store *Store) GetFisheriesPermit(ctx context.Context, permitID string) (FisheriesPermit, error) {
	var permit FisheriesPermit
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, _ tenantctx.Claims) error {
		found, err := scanFisheriesPermit(tx.QueryRow(ctx,
			`SELECT `+fisheriesColumns+` FROM registry_fisheries_permits WHERE permit_id = $1`, permitID))
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: fisheries permit %s", ErrNotFound, permitID)
		}
		if err != nil {
			return fmt.Errorf("read fisheries permit: %w", err)
		}
		permit = found
		return nil
	})
	return permit, err
}

// FisheriesAuditTrail returns the append-only lifecycle history of a
// permit in insertion order.
func (store *Store) FisheriesAuditTrail(ctx context.Context, permitID string) ([]FisheriesAuditEntry, error) {
	var trail []FisheriesAuditEntry
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, _ tenantctx.Claims) error {
		rows, err := tx.Query(ctx, `
			SELECT audit_id, permit_id, action, actor, detail, occurred_at
			FROM registry_fisheries_permit_audit WHERE permit_id = $1 ORDER BY audit_id`, permitID)
		if err != nil {
			return fmt.Errorf("read fisheries audit trail: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var entry FisheriesAuditEntry
			if err := rows.Scan(&entry.AuditID, &entry.PermitID, &entry.Action, &entry.Actor, &entry.Detail, &entry.OccurredAt); err != nil {
				return fmt.Errorf("scan fisheries audit entry: %w", err)
			}
			trail = append(trail, entry)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	if len(trail) == 0 {
		return nil, fmt.Errorf("%w: fisheries permit %s audit trail", ErrNotFound, permitID)
	}
	return trail, nil
}

// VerifyFisheriesPermit is the minimal-disclosure public verification of a
// permit by its public permit number: a permit is valid only when it is
// GRANTED and the verification time is inside its validity window. Owner,
// vessel and site attributes are never disclosed.
func (store *Store) VerifyFisheriesPermit(ctx context.Context, permitNumber string) (FisheriesVerification, error) {
	if len(permitNumber) < 4 || len(permitNumber) > 64 {
		return FisheriesVerification{}, errors.New("permitNumber must be 4-64 characters")
	}
	var verification FisheriesVerification
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, _ tenantctx.Claims) error {
		var (
			status  FisheriesPermitStatus
			validTo time.Time
			kind    FisheriesPermitType
		)
		err := tx.QueryRow(ctx, `
			SELECT permit_type, status, valid_to FROM registry_fisheries_permits
			WHERE permit_number = $1`, permitNumber).Scan(&kind, &status, &validTo)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: fisheries permit number %s", ErrNotFound, permitNumber)
		}
		if err != nil {
			return fmt.Errorf("verify fisheries permit: %w", err)
		}
		now := time.Now().UTC()
		verification = FisheriesVerification{
			PermitNumber: permitNumber,
			PermitType:   kind,
			Valid:        status == FisheriesGranted && now.Before(validTo),
			ValidTo:      validTo,
			CheckedAt:    now,
		}
		return nil
	})
	return verification, err
}
