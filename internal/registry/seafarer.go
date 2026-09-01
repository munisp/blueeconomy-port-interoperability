package registry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantdb"
)

// SeafarerStatus is the registry standing of a seafarer.
type SeafarerStatus string

const (
	SeafarerActive    SeafarerStatus = "ACTIVE"
	SeafarerSuspended SeafarerStatus = "SUSPENDED"
	SeafarerDeceased  SeafarerStatus = "DECEASED"
)

// CertificateStatus is the lifecycle state of an STCW certificate.
type CertificateStatus string

const (
	CertificateActive    CertificateStatus = "ACTIVE"
	CertificateExpired   CertificateStatus = "EXPIRED"
	CertificateSuspended CertificateStatus = "SUSPENDED"
	CertificateRevoked   CertificateStatus = "REVOKED"
)

// certificateTransitions is the closed certificate state machine. EXPIRED
// is reached only by the expiry sweep; SUSPENDED may be lifted back to
// ACTIVE; REVOKED is terminal.
var certificateTransitions = map[CertificateStatus][]CertificateStatus{
	CertificateActive:    {CertificateExpired, CertificateSuspended, CertificateRevoked},
	CertificateSuspended: {CertificateActive, CertificateRevoked},
	CertificateExpired:   {},
	CertificateRevoked:   {},
}

// STCW certificate classes admitted by the registry (STCW 1978, as
// amended). The closed set is mirrored by the migration CHECK constraint.
var stcwCertificateTypes = map[string]bool{
	"STCW-II-1":  true,
	"STCW-II-2":  true,
	"STCW-II-3":  true,
	"STCW-III-1": true,
	"STCW-III-2": true,
	"STCW-III-3": true,
	"STCW-V-1":   true,
	"STCW-VI-1":  true,
	"STCW-VI-2":  true,
	"STCW-VI-3":  true,
	"STCW-VI-6":  true,
	"GMDSS-GOC":  true,
}

// Seafarer is a registered seafarer aggregate.
type Seafarer struct {
	SeafarerID  string         `json:"seafarerId"`
	FullName    string         `json:"fullName"`
	DateOfBirth time.Time      `json:"dateOfBirth"`
	Nationality string         `json:"nationality"`
	Rank        string         `json:"rank"`
	Status      SeafarerStatus `json:"status"`
	CreatedBy   string         `json:"createdBy"`
	CreatedAt   time.Time      `json:"createdAt"`
	UpdatedAt   time.Time      `json:"updatedAt"`
	Version     int            `json:"version"`
}

// RegisterSeafarerRequest enrols a seafarer.
type RegisterSeafarerRequest struct {
	SeafarerID  string `json:"seafarerId"`
	FullName    string `json:"fullName"`
	DateOfBirth string `json:"dateOfBirth"` // RFC 3339 date (YYYY-MM-DD)
	Nationality string `json:"nationality"`
	Rank        string `json:"rank"`
}

// Validate enforces the seafarer invariants fail-closed.
func (request RegisterSeafarerRequest) Validate() error {
	if !identifier.MatchString(request.SeafarerID) {
		return errors.New("seafarerId must be 1-64 characters of [A-Za-z0-9._:-]")
	}
	if strings.TrimSpace(request.FullName) == "" || len(request.FullName) > 256 {
		return errors.New("fullName must be 1-256 characters")
	}
	birth, err := time.Parse("2006-01-02", request.DateOfBirth)
	if err != nil {
		return errors.New("dateOfBirth must be a YYYY-MM-DD date")
	}
	if birth.After(time.Now().UTC().AddDate(-14, 0, 0)) {
		return errors.New("dateOfBirth is implausibly recent for a certifiable seafarer")
	}
	if !countryCode.MatchString(request.Nationality) {
		return errors.New("nationality must be an ISO 3166-1 alpha-2 code")
	}
	if strings.TrimSpace(request.Rank) == "" || len(request.Rank) > 128 {
		return errors.New("rank must be 1-128 characters")
	}
	return nil
}

// Certificate is an STCW certificate held by a seafarer.
type Certificate struct {
	CertificateNumber string            `json:"certificateNumber"`
	SeafarerID        string            `json:"seafarerId"`
	CertificateType   string            `json:"certificateType"`
	IssuingAuthority  string            `json:"issuingAuthority"`
	FlagEndorsement   string            `json:"flagEndorsement"`
	IssuedAt          time.Time         `json:"issuedAt"`
	ExpiresAt         time.Time         `json:"expiresAt"`
	Status            CertificateStatus `json:"status"`
	CreatedBy         string            `json:"createdBy"`
	CreatedAt         time.Time         `json:"createdAt"`
	UpdatedAt         time.Time         `json:"updatedAt"`
	Version           int               `json:"version"`
}

// IssueCertificateRequest issues an STCW certificate to a seafarer.
type IssueCertificateRequest struct {
	CertificateNumber string `json:"certificateNumber"`
	SeafarerID        string `json:"seafarerId"`
	CertificateType   string `json:"certificateType"`
	IssuingAuthority  string `json:"issuingAuthority"`
	FlagEndorsement   string `json:"flagEndorsement"`
	IssuedAt          string `json:"issuedAt"`  // YYYY-MM-DD
	ExpiresAt         string `json:"expiresAt"` // YYYY-MM-DD
}

// Validate enforces certificate invariants fail-closed.
func (request IssueCertificateRequest) Validate() error {
	if len(request.CertificateNumber) < 4 || len(request.CertificateNumber) > 64 {
		return errors.New("certificateNumber must be 4-64 characters")
	}
	if !identifier.MatchString(request.SeafarerID) {
		return errors.New("seafarerId must be 1-64 characters of [A-Za-z0-9._:-]")
	}
	if !stcwCertificateTypes[request.CertificateType] {
		return fmt.Errorf("certificateType %q is not an admitted STCW class", request.CertificateType)
	}
	if strings.TrimSpace(request.IssuingAuthority) == "" || len(request.IssuingAuthority) > 256 {
		return errors.New("issuingAuthority must be 1-256 characters")
	}
	if !countryCode.MatchString(request.FlagEndorsement) {
		return errors.New("flagEndorsement must be an ISO 3166-1 alpha-2 code")
	}
	issued, err := time.Parse("2006-01-02", request.IssuedAt)
	if err != nil {
		return errors.New("issuedAt must be a YYYY-MM-DD date")
	}
	expires, err := time.Parse("2006-01-02", request.ExpiresAt)
	if err != nil {
		return errors.New("expiresAt must be a YYYY-MM-DD date")
	}
	if !expires.After(issued) {
		return errors.New("expiresAt must be after issuedAt")
	}
	return nil
}

const seafarerColumns = `seafarer_id, full_name, date_of_birth, nationality, rank, status, created_by, created_at, updated_at, version`

func scanSeafarer(row pgx.Row) (Seafarer, error) {
	var seafarer Seafarer
	err := row.Scan(&seafarer.SeafarerID, &seafarer.FullName, &seafarer.DateOfBirth, &seafarer.Nationality,
		&seafarer.Rank, &seafarer.Status, &seafarer.CreatedBy, &seafarer.CreatedAt, &seafarer.UpdatedAt, &seafarer.Version)
	return seafarer, err
}

const certificateColumns = `certificate_number, seafarer_id, certificate_type, issuing_authority, flag_endorsement,
	issued_at, expires_at, status, created_by, created_at, updated_at, version`

func scanCertificate(row pgx.Row) (Certificate, error) {
	var certificate Certificate
	err := row.Scan(&certificate.CertificateNumber, &certificate.SeafarerID, &certificate.CertificateType,
		&certificate.IssuingAuthority, &certificate.FlagEndorsement, &certificate.IssuedAt, &certificate.ExpiresAt,
		&certificate.Status, &certificate.CreatedBy, &certificate.CreatedAt, &certificate.UpdatedAt, &certificate.Version)
	return certificate, err
}

// RegisterSeafarer enrols a seafarer, idempotently.
func (store *Store) RegisterSeafarer(ctx context.Context, idempotencyKey string, request RegisterSeafarerRequest, principal Principal) (Seafarer, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return Seafarer{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if err := request.Validate(); err != nil {
		return Seafarer{}, err
	}
	if !principal.valid() {
		return Seafarer{}, errors.New("a verified principal is required")
	}
	birth, _ := time.Parse("2006-01-02", request.DateOfBirth)
	var seafarer Seafarer
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		now := time.Now().UTC()
		created, err := scanSeafarer(tx.QueryRow(ctx, `
			INSERT INTO registry_seafarers
				(tenant_id, seafarer_id, idempotency_key, full_name, date_of_birth, nationality, rank, status, created_by, created_at, updated_at, version)
			VALUES ($1, $2, $3, $4, $5, $6, $7, 'ACTIVE', $8, $9, $9, 1)
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING `+seafarerColumns,
			claims.TenantID, request.SeafarerID, idempotencyKey, request.FullName, birth, request.Nationality,
			request.Rank, principal.ID, now))
		if errors.Is(err, pgx.ErrNoRows) {
			existing, lookupErr := scanSeafarer(tx.QueryRow(ctx,
				`SELECT `+seafarerColumns+` FROM registry_seafarers WHERE idempotency_key = $1`, idempotencyKey))
			if lookupErr != nil {
				return fmt.Errorf("resolve idempotent seafarer registration: %w", lookupErr)
			}
			seafarer = existing
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert seafarer: %w", err)
		}
		if err := emit(ctx, tx, claims, events.TopicRegistrySeafarer, "registry.seafarer.registered", idempotencyKey, created.SeafarerID, map[string]string{
			"seafarerId":  created.SeafarerID,
			"nationality": created.Nationality,
			"rank":        created.Rank,
		}, map[string]string{
			"seafarer": created.SeafarerID,
		}, principal, now, store.signer); err != nil {
			return err
		}
		seafarer = created
		return nil
	})
	return seafarer, err
}

// IssueCertificate issues an STCW certificate to a registered seafarer,
// idempotently. Issuing to a seafarer not visible to the tenant fails
// closed (FK + RLS).
func (store *Store) IssueCertificate(ctx context.Context, idempotencyKey string, request IssueCertificateRequest, principal Principal) (Certificate, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return Certificate{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if err := request.Validate(); err != nil {
		return Certificate{}, err
	}
	if !principal.valid() {
		return Certificate{}, errors.New("a verified principal is required")
	}
	issued, _ := time.Parse("2006-01-02", request.IssuedAt)
	expires, _ := time.Parse("2006-01-02", request.ExpiresAt)
	var certificate Certificate
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		var holder string
		if err := tx.QueryRow(ctx, `SELECT seafarer_id FROM registry_seafarers WHERE seafarer_id = $1 FOR SHARE`, request.SeafarerID).Scan(&holder); errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: seafarer %s", ErrNotFound, request.SeafarerID)
		} else if err != nil {
			return fmt.Errorf("verify seafarer: %w", err)
		}
		now := time.Now().UTC()
		created, err := scanCertificate(tx.QueryRow(ctx, `
			INSERT INTO registry_seafarer_certificates
				(tenant_id, certificate_number, seafarer_id, idempotency_key, certificate_type, issuing_authority,
				 flag_endorsement, issued_at, expires_at, status, created_by, created_at, updated_at, version)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'ACTIVE', $10, $11, $11, 1)
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING `+certificateColumns,
			claims.TenantID, request.CertificateNumber, request.SeafarerID, idempotencyKey,
			request.CertificateType, request.IssuingAuthority, request.FlagEndorsement, issued, expires,
			principal.ID, now))
		if errors.Is(err, pgx.ErrNoRows) {
			existing, lookupErr := scanCertificate(tx.QueryRow(ctx,
				`SELECT `+certificateColumns+` FROM registry_seafarer_certificates WHERE idempotency_key = $1`, idempotencyKey))
			if lookupErr != nil {
				return fmt.Errorf("resolve idempotent certificate issuance: %w", lookupErr)
			}
			certificate = existing
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert certificate: %w", err)
		}
		if err := emit(ctx, tx, claims, events.TopicRegistrySeafarer, "registry.seafarer.certificate-issued", idempotencyKey, created.CertificateNumber, map[string]string{
			"certificateNumber": created.CertificateNumber,
			"seafarerId":        created.SeafarerID,
			"certificateType":   created.CertificateType,
			"flagEndorsement":   created.FlagEndorsement,
			"expiresAt":         expires.Format("2006-01-02"),
		}, map[string]string{
			"certificate": created.CertificateNumber,
			"type":        created.CertificateType,
		}, principal, now, store.signer); err != nil {
			return err
		}
		certificate = created
		return nil
	})
	return certificate, err
}

// TransitionCertificate moves a certificate through its administration
// state machine (SUSPEND / REINSTATE / REVOKE). EXPIRED is reserved for the
// sweep and is rejected here.
func (store *Store) TransitionCertificate(ctx context.Context, idempotencyKey, certificateNumber string, target CertificateStatus, principal Principal) (Certificate, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return Certificate{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if target == CertificateExpired {
		return Certificate{}, errors.New("EXPIRED is set by the expiry sweep, never by an officer")
	}
	if !principal.valid() {
		return Certificate{}, errors.New("a verified principal is required")
	}
	var certificate Certificate
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		current, err := scanCertificate(tx.QueryRow(ctx,
			`SELECT `+certificateColumns+` FROM registry_seafarer_certificates WHERE certificate_number = $1 FOR UPDATE`, certificateNumber))
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: certificate %s", ErrNotFound, certificateNumber)
		}
		if err != nil {
			return fmt.Errorf("lock certificate: %w", err)
		}
		legal := false
		for _, next := range certificateTransitions[current.Status] {
			if next == target {
				legal = true
			}
		}
		if !legal {
			return fmt.Errorf("%w: certificate %s cannot move %s -> %s", ErrConflict, certificateNumber, current.Status, target)
		}
		now := time.Now().UTC()
		updated, err := scanCertificate(tx.QueryRow(ctx, `
			UPDATE registry_seafarer_certificates
			SET status = $3, updated_at = $4, version = version + 1
			WHERE certificate_number = $1 AND version = $2
			RETURNING `+certificateColumns, certificateNumber, current.Version, string(target), now))
		if err != nil {
			return fmt.Errorf("transition certificate: %w", err)
		}
		if err := emit(ctx, tx, claims, events.TopicRegistrySeafarer, "registry.seafarer.certificate-transitioned", idempotencyKey, certificateNumber, map[string]string{
			"certificateNumber": certificateNumber,
			"from":              string(current.Status),
			"to":                string(target),
		}, map[string]string{
			"certificate": certificateNumber,
			"status":      string(target),
		}, principal, now, store.signer); err != nil {
			return err
		}
		certificate = updated
		return nil
	})
	return certificate, err
}

// Verification is the metered third-party verification result.
type Verification struct {
	CertificateNumber string `json:"certificateNumber"`
	Outcome           string `json:"outcome"` // VALID | EXPIRED | SUSPENDED | REVOKED | NOT_FOUND
	CertificateType   string `json:"certificateType,omitempty"`
	FlagEndorsement   string `json:"flagEndorsement,omitempty"`
	ExpiresAt         string `json:"expiresAt,omitempty"`
	UsageID           string `json:"usageId"`
}

// VerifyCertificate is the metered third-party verification path: every
// call — hit or miss — appends a usage row the marketplace billing hook
// aggregates. A certificate past its expiry date but not yet swept reports
// EXPIRED (time comparison is authoritative, the sweep is bookkeeping).
func (store *Store) VerifyCertificate(ctx context.Context, certificateNumber, verifierID string) (Verification, error) {
	if len(certificateNumber) < 4 || len(certificateNumber) > 64 {
		return Verification{}, errors.New("certificateNumber must be 4-64 characters")
	}
	if verifierID == "" || len(verifierID) > 256 {
		return Verification{}, errors.New("a verified verifier identity is required")
	}
	verification := Verification{CertificateNumber: certificateNumber, UsageID: uuid.NewString()}
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		now := time.Now().UTC()
		certificate, err := scanCertificate(tx.QueryRow(ctx,
			`SELECT `+certificateColumns+` FROM registry_seafarer_certificates WHERE certificate_number = $1`, certificateNumber))
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			verification.Outcome = "NOT_FOUND"
		case err != nil:
			return fmt.Errorf("read certificate for verification: %w", err)
		default:
			switch {
			case certificate.Status == CertificateActive && !now.Before(certificate.ExpiresAt):
				verification.Outcome = "EXPIRED"
			case certificate.Status == CertificateActive:
				verification.Outcome = "VALID"
			default:
				verification.Outcome = string(certificate.Status)
			}
			verification.CertificateType = certificate.CertificateType
			verification.FlagEndorsement = certificate.FlagEndorsement
			verification.ExpiresAt = certificate.ExpiresAt.Format("2006-01-02")
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO registry_certificate_verification_usage
				(tenant_id, usage_id, certificate_number, verifier_id, verified_at, outcome)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			claims.TenantID, verification.UsageID, certificateNumber, verifierID, now, verification.Outcome); err != nil {
			return fmt.Errorf("meter verification usage: %w", err)
		}
		return nil
	})
	return verification, err
}

// ExpireCertificates transitions every ACTIVE certificate whose expiry
// window has closed to EXPIRED for the tenant in context, emitting one
// registry.seafarer.v1 event per certificate. It returns the number of
// certificates expired; the sweep calls it once per tenant.
func (store *Store) ExpireCertificates(ctx context.Context, principal Principal) (int, error) {
	if !principal.valid() {
		return 0, errors.New("a verified principal is required")
	}
	expired := 0
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		now := time.Now().UTC()
		rows, err := tx.Query(ctx, `
			UPDATE registry_seafarer_certificates
			SET status = 'EXPIRED', updated_at = $1, version = version + 1
			WHERE status = 'ACTIVE' AND expires_at <= $1
			RETURNING certificate_number, seafarer_id`, now)
		if err != nil {
			return fmt.Errorf("expire certificates: %w", err)
		}
		defer rows.Close()
		type expiredRow struct{ number, seafarer string }
		var batch []expiredRow
		for rows.Next() {
			var row expiredRow
			if err := rows.Scan(&row.number, &row.seafarer); err != nil {
				return err
			}
			batch = append(batch, row)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, row := range batch {
			if err := emit(ctx, tx, claims, events.TopicRegistrySeafarer, "registry.seafarer.certificate-expired", "sweep-"+now.Format("20060102"), row.number, map[string]string{
				"certificateNumber": row.number,
				"seafarerId":        row.seafarer,
			}, map[string]string{
				"certificate": row.number,
				"status":      string(CertificateExpired),
			}, principal, now, store.signer); err != nil {
				return err
			}
		}
		expired = len(batch)
		return nil
	})
	return expired, err
}
