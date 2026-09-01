package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantdb"
)

// Principal is the verified actor behind a registry mutation; it becomes
// the provenance block of every emitted platform event and the maker/checker
// identity for dual-control transitions.
type Principal struct {
	ID   string
	Role string
}

func (principal Principal) valid() bool {
	return principal.ID != "" && principal.Role != ""
}

// Store is the tenant-scoped ship-registry repository. Every method runs
// inside tenantdb.WithTx (RLS isolation); lifecycle events are JWS-signed
// into the platform outbox in the same transaction as the mutation.
type Store struct {
	pool   *pgxpool.Pool
	signer *events.Signer
}

// NewStore builds the ship-registry store. The pool and envelope signer are
// mandatory — an unsigned event pipeline fails closed at construction.
func NewStore(pool *pgxpool.Pool, signer *events.Signer) (*Store, error) {
	if pool == nil {
		return nil, errors.New("ship-registry store requires a database pool")
	}
	if signer == nil {
		return nil, errors.New("ship-registry store requires an envelope signer")
	}
	return &Store{pool: pool, signer: signer}, nil
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
	return NewStore(pool, signer)
}

func (store *Store) Close() { store.pool.Close() }

// Pool exposes the pool for test harnesses and infrastructure adapters.
func (store *Store) Pool() *pgxpool.Pool { return store.pool }

// emit writes a FHIR-enveloped, JWS-signed event into the platform outbox
// inside the caller's transaction.
func emit(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, topic, eventType, correlationID, subjectID string, payload any, extensions map[string]string, principal Principal, occurredAt time.Time, signer *events.Signer) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s payload: %w", eventType, err)
	}
	envelope, err := events.Message(eventType, topic, correlationID, subjectID, payloadJSON, extensions, events.Provenance{
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
		eventID, claims.TenantID, topic, eventType, envelope.EventID, envelopeJSON, occurredAt); err != nil {
		return fmt.Errorf("write %s outbox event: %w", eventType, err)
	}
	return nil
}

const vesselColumns = `vessel_id, imo_number, mmsi, vessel_name, flag_state, class_society,
	gross_tonnage, build_year, build_country, owner_name, owner_country, status,
	COALESCE(certificate_number, ''), created_by, created_at, updated_at, version`

func scanVessel(row pgx.Row) (Vessel, error) {
	var vessel Vessel
	err := row.Scan(&vessel.VesselID, &vessel.IMONumber, &vessel.MMSI, &vessel.VesselName,
		&vessel.FlagState, &vessel.ClassSociety, &vessel.GrossTonnage, &vessel.BuildYear,
		&vessel.BuildCountry, &vessel.OwnerName, &vessel.OwnerCountry, &vessel.Status,
		&vessel.CertificateNumber, &vessel.CreatedBy, &vessel.CreatedAt, &vessel.UpdatedAt,
		&vessel.Version)
	return vessel, err
}

// genesisHash is the previous_hash of the first ownership entry of a
// vessel: a domain-separated commitment to the vessel identity, so a chain
// cannot be transplanted from another vessel.
func genesisHash(vesselID, imoNumber string) string {
	sum := sha256.Sum256([]byte("registry-ownership-genesis\x00" + vesselID + "\x00" + imoNumber))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// chainHash computes the entry hash of one ownership record.
func chainHash(previousHash, vesselID string, sequenceNo int, ownerName, ownerCountry string, effectiveFrom, recordedAt time.Time, recordedBy string) string {
	material := strings.Join([]string{
		"registry-ownership-entry",
		previousHash,
		vesselID,
		fmt.Sprintf("%d", sequenceNo),
		ownerName,
		ownerCountry,
		effectiveFrom.UTC().Format(time.RFC3339Nano),
		recordedAt.UTC().Format(time.RFC3339Nano),
		recordedBy,
	}, "\x00")
	sum := sha256.Sum256([]byte(material))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func appendOwnership(ctx context.Context, tx pgx.Tx, vessel Vessel, ownerName, ownerCountry string, effectiveFrom, recordedAt time.Time, recordedBy string) (OwnershipEntry, error) {
	var (
		sequenceNo   int
		previousHash string
	)
	err := tx.QueryRow(ctx, `
		SELECT sequence_no, entry_hash FROM registry_vessel_ownership
		WHERE vessel_id = $1 ORDER BY sequence_no DESC LIMIT 1 FOR UPDATE`,
		vessel.VesselID).Scan(&sequenceNo, &previousHash)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		sequenceNo, previousHash = 0, genesisHash(vessel.VesselID, vessel.IMONumber)
	case err != nil:
		return OwnershipEntry{}, fmt.Errorf("read ownership chain head: %w", err)
	}
	entry := OwnershipEntry{
		VesselID:      vessel.VesselID,
		SequenceNo:    sequenceNo + 1,
		OwnerName:     ownerName,
		OwnerCountry:  ownerCountry,
		EffectiveFrom: effectiveFrom.UTC(),
		RecordedBy:    recordedBy,
		RecordedAt:    recordedAt.UTC(),
		PreviousHash:  previousHash,
	}
	entry.EntryHash = chainHash(previousHash, vessel.VesselID, entry.SequenceNo, ownerName, ownerCountry, entry.EffectiveFrom, entry.RecordedAt, recordedBy)
	if _, err := tx.Exec(ctx, `
		INSERT INTO registry_vessel_ownership
			(tenant_id, vessel_id, sequence_no, owner_name, owner_country, effective_from, recorded_by, recorded_at, previous_hash, entry_hash)
		VALUES (current_setting('app.tenant_id', true), $1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		entry.VesselID, entry.SequenceNo, ownerName, ownerCountry, entry.EffectiveFrom, recordedBy, entry.RecordedAt, previousHash, entry.EntryHash); err != nil {
		return OwnershipEntry{}, fmt.Errorf("append ownership entry: %w", err)
	}
	return entry, nil
}

// Register opens a vessel registration application, idempotently. The IMO
// check digit and MMSI MID are validated fail-closed before persistence,
// and the initial ownership record starts the hash chain.
func (store *Store) Register(ctx context.Context, idempotencyKey string, request RegisterVesselRequest, principal Principal) (Vessel, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return Vessel{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if err := request.Validate(); err != nil {
		return Vessel{}, err
	}
	if !principal.valid() {
		return Vessel{}, errors.New("a verified principal is required")
	}
	var vessel Vessel
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		now := time.Now().UTC()
		created, err := scanVessel(tx.QueryRow(ctx, `
			INSERT INTO registry_vessels
				(tenant_id, vessel_id, idempotency_key, imo_number, mmsi, vessel_name, flag_state,
				 class_society, gross_tonnage, build_year, build_country, owner_name, owner_country,
				 status, created_by, created_at, updated_at, version)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'APPLICATION', $14, $15, $15, 1)
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING `+vesselColumns,
			claims.TenantID, request.VesselID, idempotencyKey, request.IMONumber, request.MMSI,
			request.VesselName, request.FlagState, request.ClassSociety, request.GrossTonnage,
			request.BuildYear, request.BuildCountry, request.OwnerName, request.OwnerCountry,
			principal.ID, now))
		if errors.Is(err, pgx.ErrNoRows) {
			existing, lookupErr := scanVessel(tx.QueryRow(ctx,
				`SELECT `+vesselColumns+` FROM registry_vessels WHERE idempotency_key = $1`, idempotencyKey))
			if lookupErr != nil {
				return fmt.Errorf("resolve idempotent vessel registration: %w", lookupErr)
			}
			vessel = existing
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert vessel registration: %w", err)
		}
		if _, err := appendOwnership(ctx, tx, created, created.OwnerName, created.OwnerCountry, now, now, principal.ID); err != nil {
			return err
		}
		if err := emit(ctx, tx, claims, events.TopicRegistryVessel, "registry.vessel.registered", idempotencyKey, created.VesselID, map[string]string{
			"vesselId":   created.VesselID,
			"imoNumber":  created.IMONumber,
			"mmsi":       created.MMSI,
			"flagState":  created.FlagState,
			"status":     string(created.Status),
			"registered": "true",
		}, map[string]string{
			"vessel": created.VesselID,
			"imo":    created.IMONumber,
			"flag":   created.FlagState,
		}, principal, now, store.signer); err != nil {
			return err
		}
		vessel = created
		return nil
	})
	return vessel, err
}

// Get returns one vessel aggregate visible to the tenant.
func (store *Store) Get(ctx context.Context, vesselID string) (Vessel, error) {
	var vessel Vessel
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, _ tenantctx.Claims) error {
		found, err := scanVessel(tx.QueryRow(ctx,
			`SELECT `+vesselColumns+` FROM registry_vessels WHERE vessel_id = $1`, vesselID))
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: vessel %s", ErrNotFound, vesselID)
		}
		if err != nil {
			return fmt.Errorf("read vessel: %w", err)
		}
		vessel = found
		return nil
	})
	return vessel, err
}

// List returns vessels visible to the tenant, optionally filtered by
// status, in stable creation order.
func (store *Store) List(ctx context.Context, status VesselStatus, limit int) ([]Vessel, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	vessels := []Vessel{}
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, _ tenantctx.Claims) error {
		query := `SELECT ` + vesselColumns + ` FROM registry_vessels`
		args := []any{}
		if status != "" {
			query += ` WHERE status = $1`
			args = append(args, string(status))
		}
		query += ` ORDER BY created_at, vessel_id LIMIT ` + fmt.Sprintf("%d", limit)
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("list vessels: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			vessel, err := scanVessel(rows)
			if err != nil {
				return err
			}
			vessels = append(vessels, vessel)
		}
		return rows.Err()
	})
	return vessels, err
}

// OwnershipHistory returns the hash-chained ownership entries of a vessel
// in chain order. VerifyChain re-derives every hash fail-closed before
// returning, so a tampered history surfaces as an error, not data.
func (store *Store) OwnershipHistory(ctx context.Context, vesselID string) ([]OwnershipEntry, error) {
	entries := []OwnershipEntry{}
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, _ tenantctx.Claims) error {
		var imo string
		if err := tx.QueryRow(ctx, `SELECT imo_number FROM registry_vessels WHERE vessel_id = $1`, vesselID).Scan(&imo); errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: vessel %s", ErrNotFound, vesselID)
		} else if err != nil {
			return fmt.Errorf("read vessel for ownership history: %w", err)
		}
		rows, err := tx.Query(ctx, `
			SELECT vessel_id, sequence_no, owner_name, owner_country, effective_from, recorded_by, recorded_at, previous_hash, entry_hash
			FROM registry_vessel_ownership WHERE vessel_id = $1 ORDER BY sequence_no`, vesselID)
		if err != nil {
			return fmt.Errorf("read ownership history: %w", err)
		}
		defer rows.Close()
		previous := genesisHash(vesselID, imo)
		for rows.Next() {
			var entry OwnershipEntry
			if err := rows.Scan(&entry.VesselID, &entry.SequenceNo, &entry.OwnerName, &entry.OwnerCountry,
				&entry.EffectiveFrom, &entry.RecordedBy, &entry.RecordedAt, &entry.PreviousHash, &entry.EntryHash); err != nil {
				return err
			}
			if entry.PreviousHash != previous {
				return fmt.Errorf("ownership hash chain broken at entry %d of vessel %s", entry.SequenceNo, vesselID)
			}
			if recomputed := chainHash(entry.PreviousHash, entry.VesselID, entry.SequenceNo, entry.OwnerName, entry.OwnerCountry, entry.EffectiveFrom, entry.RecordedAt, entry.RecordedBy); recomputed != entry.EntryHash {
				return fmt.Errorf("ownership entry %d of vessel %s fails hash verification", entry.SequenceNo, vesselID)
			}
			previous = entry.EntryHash
			entries = append(entries, entry)
		}
		return rows.Err()
	})
	return entries, err
}

// Transition advances the vessel registration workflow. SURVEY →
// REGISTRATION → CERTIFICATE_ISSUED are checker steps: the acting principal
// must differ from the principal who opened the preceding maker step
// (APPLICATION maker is created_by; certificate issuance additionally
// requires a certificate number). Ownership transfer (owner change) is a
// separate audited path via TransferOwnership.
func (store *Store) Transition(ctx context.Context, idempotencyKey, vesselID string, target VesselStatus, certificateNumber string, principal Principal) (Vessel, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return Vessel{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if !principal.valid() {
		return Vessel{}, errors.New("a verified principal is required")
	}
	var vessel Vessel
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		current, err := scanVessel(tx.QueryRow(ctx,
			`SELECT `+vesselColumns+` FROM registry_vessels WHERE vessel_id = $1 FOR UPDATE`, vesselID))
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: vessel %s", ErrNotFound, vesselID)
		}
		if err != nil {
			return fmt.Errorf("lock vessel: %w", err)
		}
		if !CanTransition(current.Status, target) {
			return fmt.Errorf("%w: vessel %s cannot move %s -> %s", ErrConflict, vesselID, current.Status, target)
		}
		// Maker-checker: forward checker steps (past the maker's own
		// application) must be executed by a different officer.
		if (target == VesselRegistration || target == VesselCertificateIssued) && principal.ID == current.CreatedBy {
			return fmt.Errorf("%w: %s decided by application maker", ErrMakerChecker, target)
		}
		if target == VesselCertificateIssued {
			if len(certificateNumber) < 4 || len(certificateNumber) > 64 {
				return errors.New("certificate issuance requires a certificate number of 4-64 characters")
			}
		}
		now := time.Now().UTC()
		updated, err := scanVessel(tx.QueryRow(ctx, `
			UPDATE registry_vessels
			SET status = $3, certificate_number = CASE WHEN $3 = 'CERTIFICATE_ISSUED' THEN $4 ELSE certificate_number END,
			    updated_at = $5, version = version + 1
			WHERE vessel_id = $1 AND version = $2
			RETURNING `+vesselColumns,
			vesselID, current.Version, string(target), certificateNumber, now))
		if err != nil {
			return fmt.Errorf("transition vessel: %w", err)
		}
		if err := emit(ctx, tx, claims, events.TopicRegistryVessel, "registry.vessel.transitioned", idempotencyKey, vesselID, map[string]string{
			"vesselId": vesselID,
			"from":     string(current.Status),
			"to":       string(target),
		}, map[string]string{
			"vessel": vesselID,
			"status": string(target),
		}, principal, now, store.signer); err != nil {
			return err
		}
		vessel = updated
		return nil
	})
	return vessel, err
}

// TransferOwnership records an audited ownership change: the vessel row is
// updated and a hash-chained history entry appended in the same
// transaction, with a registry.vessel.v1 event.
func (store *Store) TransferOwnership(ctx context.Context, idempotencyKey, vesselID, ownerName, ownerCountry string, effectiveFrom time.Time, principal Principal) (OwnershipEntry, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return OwnershipEntry{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if strings.TrimSpace(ownerName) == "" || len(ownerName) > 256 {
		return OwnershipEntry{}, errors.New("ownerName must be 1-256 characters")
	}
	if !countryCode.MatchString(ownerCountry) {
		return OwnershipEntry{}, errors.New("ownerCountry must be an ISO 3166-1 alpha-2 code")
	}
	if !principal.valid() {
		return OwnershipEntry{}, errors.New("a verified principal is required")
	}
	var entry OwnershipEntry
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		current, err := scanVessel(tx.QueryRow(ctx,
			`SELECT `+vesselColumns+` FROM registry_vessels WHERE vessel_id = $1 FOR UPDATE`, vesselID))
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: vessel %s", ErrNotFound, vesselID)
		}
		if err != nil {
			return fmt.Errorf("lock vessel: %w", err)
		}
		if current.Status == VesselDeregistered {
			return fmt.Errorf("%w: deregistered vessel %s cannot change ownership", ErrConflict, vesselID)
		}
		now := time.Now().UTC()
		if _, err := tx.Exec(ctx, `
			UPDATE registry_vessels SET owner_name = $2, owner_country = $3, updated_at = $4, version = version + 1
			WHERE vessel_id = $1`, vesselID, ownerName, ownerCountry, now); err != nil {
			return fmt.Errorf("update vessel owner: %w", err)
		}
		appended, err := appendOwnership(ctx, tx, current, ownerName, ownerCountry, effectiveFrom, now, principal.ID)
		if err != nil {
			return err
		}
		if err := emit(ctx, tx, claims, events.TopicRegistryVessel, "registry.vessel.ownership-transferred", idempotencyKey, vesselID, map[string]string{
			"vesselId":     vesselID,
			"ownerName":    ownerName,
			"ownerCountry": ownerCountry,
			"sequenceNo":   fmt.Sprintf("%d", appended.SequenceNo),
		}, map[string]string{
			"vessel": vesselID,
		}, principal, now, store.signer); err != nil {
			return err
		}
		entry = appended
		return nil
	})
	return entry, err
}
