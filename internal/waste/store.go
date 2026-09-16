// Package waste implements the MARPOL Annex I-VI port reception facility
// waste-delivery receipt registry. Receipts link to port calls, record the
// delivered quantity/unit against a reception-facility reference, and
// reference the receipt document by id/URI only — document storage lives
// in the evidence service; nothing is stored or fabricated here. The
// lifecycle is DRAFT -> DELIVERED -> VERIFIED with maker-checker on
// verification, and every mutation emits a JWS-signed
// ports.waste-receipts.v1 event into the platform outbox in the same
// transaction.
package waste

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/events"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantdb"
)

var (
	// ErrNotFound is returned when the addressed receipt or port call is
	// absent from the tenant scope.
	ErrNotFound = errors.New("waste receipt aggregate not found")
	// ErrConflict is returned when a state-machine transition is not legal
	// from the current state.
	ErrConflict = errors.New("waste receipt state conflict")
	// ErrMakerChecker is returned when the verifying officer is the same
	// person who recorded the receipt.
	ErrMakerChecker = errors.New("maker-checker violation: checker must differ from maker")

	identifier = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)
)

// Principal is the verified actor behind a receipt mutation.
type Principal struct {
	ID   string
	Role string
}

func (principal Principal) valid() bool {
	return principal.ID != "" && principal.Role != ""
}

// MarpolAnnex enumerates the MARPOL 73/78 annexes under which waste is
// delivered to a reception facility.
type MarpolAnnex string

const (
	AnnexI   MarpolAnnex = "I"   // oil
	AnnexII  MarpolAnnex = "II"  // noxious liquid substances in bulk
	AnnexIII MarpolAnnex = "III" // harmful substances in packaged form
	AnnexIV  MarpolAnnex = "IV"  // sewage
	AnnexV   MarpolAnnex = "V"   // garbage
	AnnexVI  MarpolAnnex = "VI"  // air emissions (residues, e.g. scrubber sludge)
)

func (annex MarpolAnnex) admitted() bool {
	switch annex {
	case AnnexI, AnnexII, AnnexIII, AnnexIV, AnnexV, AnnexVI:
		return true
	default:
		return false
	}
}

// Unit is the admitted quantity unit for a delivery.
type Unit string

const (
	UnitCubicMetres Unit = "M3"
	UnitTonnes      Unit = "TONNES"
	UnitKilograms   Unit = "KG"
)

func (unit Unit) admitted() bool {
	switch unit {
	case UnitCubicMetres, UnitTonnes, UnitKilograms:
		return true
	default:
		return false
	}
}

// Status is the receipt workflow state.
type Status string

const (
	StatusDraft     Status = "DRAFT"
	StatusDelivered Status = "DELIVERED"
	StatusVerified  Status = "VERIFIED"
)

// Receipt is a MARPOL waste-delivery receipt linked to a port call.
type Receipt struct {
	ReceiptID          string     `json:"receiptId"`
	PortCallID         string     `json:"portCallId"`
	MarpolAnnex        MarpolAnnex `json:"marpolAnnex"`
	WasteType          string     `json:"wasteType"`
	Quantity           float64    `json:"quantity"`
	Unit               Unit       `json:"unit"`
	FacilityReference  string     `json:"facilityReference"`
	ReceiptDocumentID  string     `json:"receiptDocumentId,omitempty"`
	ReceiptDocumentURI string     `json:"receiptDocumentUri,omitempty"`
	Status             Status     `json:"status"`
	CreatedBy          string     `json:"createdBy"`
	DeliveredBy        string     `json:"deliveredBy,omitempty"`
	VerifiedBy         string     `json:"verifiedBy,omitempty"`
	DeliveredAt        *time.Time `json:"deliveredAt,omitempty"`
	VerifiedAt         *time.Time `json:"verifiedAt,omitempty"`
	CreatedAt          time.Time  `json:"createdAt"`
	UpdatedAt          time.Time  `json:"updatedAt"`
	Version            int        `json:"version"`
}

// CreateRequest opens a DRAFT waste-delivery receipt on a port call.
type CreateRequest struct {
	ReceiptID          string      `json:"receiptId"`
	PortCallID         string      `json:"portCallId"`
	MarpolAnnex        MarpolAnnex `json:"marpolAnnex"`
	WasteType          string      `json:"wasteType"`
	Quantity           float64     `json:"quantity"`
	Unit               Unit        `json:"unit"`
	FacilityReference  string      `json:"facilityReference"`
	ReceiptDocumentID  string      `json:"receiptDocumentId,omitempty"`
	ReceiptDocumentURI string      `json:"receiptDocumentUri,omitempty"`
}

func (request CreateRequest) validate() error {
	if !identifier.MatchString(request.ReceiptID) {
		return errors.New("receiptId must be 1-64 characters of [A-Za-z0-9._:-]")
	}
	if !identifier.MatchString(request.PortCallID) {
		return errors.New("portCallId must be 1-64 characters of [A-Za-z0-9._:-]")
	}
	if !request.MarpolAnnex.admitted() {
		return fmt.Errorf("marpolAnnex %q is not admitted (I-VI)", request.MarpolAnnex)
	}
	if strings.TrimSpace(request.WasteType) == "" || len(request.WasteType) > 256 {
		return errors.New("wasteType must be 1-256 characters")
	}
	if request.Quantity <= 0 {
		return errors.New("quantity must be greater than zero")
	}
	if !request.Unit.admitted() {
		return fmt.Errorf("unit %q is not admitted (M3, TONNES, KG)", request.Unit)
	}
	if strings.TrimSpace(request.FacilityReference) == "" || len(request.FacilityReference) > 256 {
		return errors.New("facilityReference must be 1-256 characters")
	}
	if len(request.ReceiptDocumentID) > 128 {
		return errors.New("receiptDocumentId must be at most 128 characters")
	}
	if len(request.ReceiptDocumentURI) > 1024 {
		return errors.New("receiptDocumentUri must be at most 1024 characters")
	}
	return nil
}

// Store is the tenant-scoped waste-receipt repository; every method runs
// inside tenantdb.WithTx (RLS isolation) and emits signed outbox events in
// the same transaction as the mutation.
type Store struct {
	pool   *pgxpool.Pool
	signer *events.Signer
}

// NewStore builds the store; the pool and signer are mandatory — an
// unsigned event pipeline fails closed at construction.
func NewStore(pool *pgxpool.Pool, signer *events.Signer) (*Store, error) {
	if pool == nil {
		return nil, errors.New("waste-receipt store requires a database pool")
	}
	if signer == nil {
		return nil, errors.New("waste-receipt store requires an envelope signer")
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

// Pool exposes the pool for test harnesses.
func (store *Store) Pool() *pgxpool.Pool { return store.pool }

const receiptColumns = `receipt_id, port_call_id, marpol_annex, waste_type, quantity, unit,
	facility_reference, COALESCE(receipt_document_id, ''), COALESCE(receipt_document_uri, ''),
	status, created_by, COALESCE(delivered_by, ''), COALESCE(verified_by, ''),
	delivered_at, verified_at, created_at, updated_at, version`

func scanReceipt(row pgx.Row) (Receipt, error) {
	var receipt Receipt
	err := row.Scan(&receipt.ReceiptID, &receipt.PortCallID, &receipt.MarpolAnnex, &receipt.WasteType,
		&receipt.Quantity, &receipt.Unit, &receipt.FacilityReference, &receipt.ReceiptDocumentID,
		&receipt.ReceiptDocumentURI, &receipt.Status, &receipt.CreatedBy, &receipt.DeliveredBy,
		&receipt.VerifiedBy, &receipt.DeliveredAt, &receipt.VerifiedAt, &receipt.CreatedAt,
		&receipt.UpdatedAt, &receipt.Version)
	return receipt, err
}

func (store *Store) emit(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, eventType, idempotencyKey, subjectID string, payload any, extensions map[string]string, principal Principal, occurredAt time.Time) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s payload: %w", eventType, err)
	}
	envelope, err := events.Message(eventType, events.TopicWasteReceipts, idempotencyKey, subjectID, payloadJSON, extensions, events.Provenance{
		PrincipalID:   principal.ID,
		PrincipalRole: principal.Role,
	}, occurredAt, store.signer)
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
		eventID, claims.TenantID, events.TopicWasteReceipts, eventType, envelope.EventID, envelopeJSON, occurredAt); err != nil {
		return fmt.Errorf("write %s outbox event: %w", eventType, err)
	}
	return nil
}

// lockPortCall verifies the port call exists in the tenant scope (fail
// closed: receipts can never be recorded against a foreign or unknown
// port call).
func lockPortCall(ctx context.Context, tx pgx.Tx, callID string) error {
	var exists string
	err := tx.QueryRow(ctx, `SELECT call_id FROM port_calls WHERE call_id = $1 FOR SHARE`, callID).Scan(&exists)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: port call %s", ErrNotFound, callID)
	}
	if err != nil {
		return fmt.Errorf("lock port call: %w", err)
	}
	return nil
}

// Create opens a DRAFT waste-delivery receipt on an existing port call.
func (store *Store) Create(ctx context.Context, idempotencyKey string, request CreateRequest, principal Principal) (Receipt, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return Receipt{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if err := request.validate(); err != nil {
		return Receipt{}, err
	}
	if !principal.valid() {
		return Receipt{}, errors.New("a verified principal is required")
	}
	var receipt Receipt
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		if err := lockPortCall(ctx, tx, request.PortCallID); err != nil {
			return err
		}
		now := time.Now().UTC()
		created, err := scanReceipt(tx.QueryRow(ctx, `
			INSERT INTO portcall_waste_receipts
				(tenant_id, receipt_id, idempotency_key, port_call_id, marpol_annex, waste_type, quantity, unit,
				 facility_reference, receipt_document_id, receipt_document_uri, status, created_by, created_at, updated_at, version)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULLIF($10, ''), NULLIF($11, ''), 'DRAFT', $12, $13, $13, 1)
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING `+receiptColumns,
			claims.TenantID, request.ReceiptID, idempotencyKey, request.PortCallID, string(request.MarpolAnnex),
			request.WasteType, request.Quantity, string(request.Unit), request.FacilityReference,
			request.ReceiptDocumentID, request.ReceiptDocumentURI, principal.ID, now))
		if errors.Is(err, pgx.ErrNoRows) {
			existing, lookupErr := scanReceipt(tx.QueryRow(ctx,
				`SELECT `+receiptColumns+` FROM portcall_waste_receipts WHERE idempotency_key = $1`, idempotencyKey))
			if lookupErr != nil {
				return fmt.Errorf("resolve idempotent receipt: %w", lookupErr)
			}
			receipt = existing
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert waste receipt: %w", err)
		}
		if err := store.emit(ctx, tx, claims, "waste.receipt.created", idempotencyKey, created.ReceiptID, map[string]string{
			"receiptId":         created.ReceiptID,
			"portCallId":        created.PortCallID,
			"marpolAnnex":       string(created.MarpolAnnex),
			"facilityReference": created.FacilityReference,
		}, map[string]string{
			"portcall": created.PortCallID,
			"receipt":  created.ReceiptID,
		}, principal, now); err != nil {
			return err
		}
		receipt = created
		return nil
	})
	return receipt, err
}

// transition is the shared DRAFT -> DELIVERED -> VERIFIED state machine.
func (store *Store) transition(ctx context.Context, idempotencyKey, receiptID string, target Status, principal Principal) (Receipt, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return Receipt{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if !principal.valid() {
		return Receipt{}, errors.New("a verified principal is required")
	}
	var receipt Receipt
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		current, err := scanReceipt(tx.QueryRow(ctx,
			`SELECT `+receiptColumns+` FROM portcall_waste_receipts WHERE receipt_id = $1 FOR UPDATE`, receiptID))
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: waste receipt %s", ErrNotFound, receiptID)
		}
		if err != nil {
			return fmt.Errorf("lock waste receipt: %w", err)
		}
		legal := (current.Status == StatusDraft && target == StatusDelivered) ||
			(current.Status == StatusDelivered && target == StatusVerified)
		if !legal {
			return fmt.Errorf("%w: waste receipt %s cannot move %s -> %s", ErrConflict, receiptID, current.Status, target)
		}
		if target == StatusVerified && principal.ID == current.CreatedBy {
			return fmt.Errorf("%w: waste receipt %s verified by its recorder", ErrMakerChecker, receiptID)
		}
		now := time.Now().UTC()
		var updated Receipt
		if target == StatusDelivered {
			updated, err = scanReceipt(tx.QueryRow(ctx, `
				UPDATE portcall_waste_receipts
				SET status = 'DELIVERED', delivered_by = $3, delivered_at = $4, updated_at = $4, version = version + 1
				WHERE receipt_id = $1 AND version = $2
				RETURNING `+receiptColumns, receiptID, current.Version, principal.ID, now))
		} else {
			updated, err = scanReceipt(tx.QueryRow(ctx, `
				UPDATE portcall_waste_receipts
				SET status = 'VERIFIED', verified_by = $3, verified_at = $4, updated_at = $4, version = version + 1
				WHERE receipt_id = $1 AND version = $2
				RETURNING `+receiptColumns, receiptID, current.Version, principal.ID, now))
		}
		if err != nil {
			return fmt.Errorf("transition waste receipt: %w", err)
		}
		if err := store.emit(ctx, tx, claims, "waste.receipt."+strings.ToLower(string(target)), idempotencyKey, receiptID, map[string]string{
			"receiptId":  receiptID,
			"portCallId": current.PortCallID,
			"from":       string(current.Status),
			"to":         string(target),
		}, map[string]string{
			"portcall": current.PortCallID,
			"receipt":  receiptID,
			"status":   string(target),
		}, principal, now); err != nil {
			return err
		}
		receipt = updated
		return nil
	})
	return receipt, err
}

// MarkDelivered records the physical delivery of the waste load to the
// reception facility (DRAFT -> DELIVERED).
func (store *Store) MarkDelivered(ctx context.Context, idempotencyKey, receiptID string, principal Principal) (Receipt, error) {
	return store.transition(ctx, idempotencyKey, receiptID, StatusDelivered, principal)
}

// Verify is the checker step confirming the delivery against the receipt
// document (DELIVERED -> VERIFIED); the verifier must differ from the
// recorder.
func (store *Store) Verify(ctx context.Context, idempotencyKey, receiptID string, principal Principal) (Receipt, error) {
	return store.transition(ctx, idempotencyKey, receiptID, StatusVerified, principal)
}

// ListByPortCall returns all receipts recorded against a port call —
// this is the port-call-record exposure surface.
func (store *Store) ListByPortCall(ctx context.Context, portCallID string) ([]Receipt, error) {
	var receipts []Receipt
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, _ tenantctx.Claims) error {
		if err := lockPortCall(ctx, tx, portCallID); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT `+receiptColumns+` FROM portcall_waste_receipts
			WHERE port_call_id = $1 ORDER BY created_at, receipt_id`, portCallID)
		if err != nil {
			return fmt.Errorf("list waste receipts: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			receipt, err := scanReceipt(rows)
			if err != nil {
				return fmt.Errorf("scan waste receipt: %w", err)
			}
			receipts = append(receipts, receipt)
		}
		return rows.Err()
	})
	if receipts == nil {
		receipts = []Receipt{}
	}
	return receipts, err
}
