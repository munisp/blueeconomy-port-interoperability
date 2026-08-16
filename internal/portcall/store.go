package portcall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

func (store *Store) Exec(ctx context.Context, statement string) (int64, error) {
	result, err := store.pool.Exec(ctx, statement)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (store *Store) Create(ctx context.Context, idempotencyKey string, request CreateRequest) (PortCall, error) {
	if idempotencyKey == "" || len(idempotencyKey) > 256 {
		return PortCall{}, errors.New("idempotency key must be non-empty and at most 256 characters")
	}
	if err := request.Validate(); err != nil {
		return PortCall{}, err
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return PortCall{}, fmt.Errorf("begin create: %w", err)
	}
	defer tx.Rollback(ctx)

	createdAt := time.Now().UTC()
	var call PortCall
	inserted := false
	err = tx.QueryRow(ctx, `
		INSERT INTO port_calls (
			call_id, vessel_imo, port_code, declaration_reference, submitted_by,
			status, idempotency_key, created_at, updated_at, version
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, 1)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING call_id, vessel_imo, port_code, declaration_reference, submitted_by,
			status, created_at, updated_at, version`,
		request.CallID, request.VesselIMO, request.PortCode, request.DeclarationRef,
		request.SubmittedBy, StatusDraft, idempotencyKey, createdAt,
	).Scan(&call.CallID, &call.VesselIMO, &call.PortCode, &call.DeclarationRef, &call.SubmittedBy,
		&call.Status, &call.CreatedAt, &call.UpdatedAt, &call.Version)
	if err == nil {
		inserted = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return PortCall{}, fmt.Errorf("insert port call: %w", err)
	}
	if !inserted {
		err = tx.QueryRow(ctx, `
			SELECT call_id, vessel_imo, port_code, declaration_reference, submitted_by,
				status, created_at, updated_at, version
			FROM port_calls WHERE idempotency_key = $1 FOR UPDATE`, idempotencyKey).
			Scan(&call.CallID, &call.VesselIMO, &call.PortCode, &call.DeclarationRef, &call.SubmittedBy,
				&call.Status, &call.CreatedAt, &call.UpdatedAt, &call.Version)
		if errors.Is(err, pgx.ErrNoRows) {
			return PortCall{}, fmt.Errorf("idempotency lookup: %w", ErrNotFound)
		}
		if err != nil {
			return PortCall{}, fmt.Errorf("lookup idempotent port call: %w", err)
		}
		if !call.Matches(request) {
			return PortCall{}, ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return PortCall{}, fmt.Errorf("commit idempotent replay: %w", err)
		}
		return call, nil
	}

	payload, err := json.Marshal(call)
	if err != nil {
		return PortCall{}, fmt.Errorf("encode created event: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO port_call_outbox (event_id, call_id, event_type, payload, created_at)
		VALUES ($1, $2, 'port_call.created', $3, $4)`, uuid.New(), call.CallID, payload, createdAt); err != nil {
		return PortCall{}, fmt.Errorf("write created event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return PortCall{}, fmt.Errorf("commit create: %w", err)
	}
	return call, nil
}

func (store *Store) Get(ctx context.Context, callID string) (PortCall, error) {
	var call PortCall
	err := store.pool.QueryRow(ctx, `
		SELECT call_id, vessel_imo, port_code, declaration_reference, submitted_by,
			status, created_at, updated_at, version
		FROM port_calls WHERE call_id = $1`, callID).
		Scan(&call.CallID, &call.VesselIMO, &call.PortCode, &call.DeclarationRef, &call.SubmittedBy,
			&call.Status, &call.CreatedAt, &call.UpdatedAt, &call.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return PortCall{}, ErrNotFound
	}
	if err != nil {
		return PortCall{}, fmt.Errorf("get port call: %w", err)
	}
	return call, nil
}

func (store *Store) Transition(ctx context.Context, callID string, expectedVersion int64, next Status) (PortCall, error) {
	current, err := store.Get(ctx, callID)
	if err != nil {
		return PortCall{}, err
	}
	if current.Version != expectedVersion || !ValidTransition(current.Status, next) {
		if current.Version != expectedVersion {
			return PortCall{}, ErrOptimisticConflict
		}
		return PortCall{}, ErrInvalidTransition
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return PortCall{}, fmt.Errorf("begin transition: %w", err)
	}
	defer tx.Rollback(ctx)
	updatedAt := time.Now().UTC()
	var updated PortCall
	err = tx.QueryRow(ctx, `
		UPDATE port_calls
		SET status = $1, updated_at = $2, version = version + 1
		WHERE call_id = $3 AND status = $4 AND version = $5
		RETURNING call_id, vessel_imo, port_code, declaration_reference, submitted_by,
			status, created_at, updated_at, version`,
		next, updatedAt, callID, current.Status, expectedVersion).
		Scan(&updated.CallID, &updated.VesselIMO, &updated.PortCode, &updated.DeclarationRef,
			&updated.SubmittedBy, &updated.Status, &updated.CreatedAt, &updated.UpdatedAt, &updated.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return PortCall{}, ErrOptimisticConflict
	}
	if err != nil {
		return PortCall{}, fmt.Errorf("transition port call: %w", err)
	}
	payload, err := json.Marshal(updated)
	if err != nil {
		return PortCall{}, fmt.Errorf("encode status event: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO port_call_outbox (event_id, call_id, event_type, payload, created_at)
		VALUES ($1, $2, 'port_call.status_changed', $3, $4)`, uuid.New(), updated.CallID, payload, updatedAt); err != nil {
		return PortCall{}, fmt.Errorf("write status event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return PortCall{}, fmt.Errorf("commit transition: %w", err)
	}
	return updated, nil
}

type Clearance struct {
	DecisionID  string            `json:"decision_id"`
	CallID      string            `json:"call_id"`
	Decision    ClearanceDecision `json:"decision"`
	Reason      string            `json:"reason"`
	DecidedBy   string            `json:"decided_by"`
	CallVersion int64             `json:"call_version"`
	DecidedAt   time.Time         `json:"decided_at"`
}

func (store *Store) DeclareDocument(ctx context.Context, callID string, request DocumentDeclarationRequest) (DocumentDeclaration, error) {
	if callID == "" || len(callID) > 256 || callID != strings.TrimSpace(callID) {
		return DocumentDeclaration{}, ErrNotFound
	}
	if err := request.Validate(); err != nil {
		return DocumentDeclaration{}, err
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return DocumentDeclaration{}, fmt.Errorf("begin document declaration: %w", err)
	}
	defer tx.Rollback(ctx)
	var currentStatus Status
	if err := tx.QueryRow(ctx, `SELECT status FROM port_calls WHERE call_id = $1 FOR UPDATE`, callID).Scan(&currentStatus); errors.Is(err, pgx.ErrNoRows) {
		return DocumentDeclaration{}, ErrNotFound
	} else if err != nil {
		return DocumentDeclaration{}, fmt.Errorf("lock port call for document: %w", err)
	}
	if currentStatus == StatusRejected {
		return DocumentDeclaration{}, ErrInvalidTransition
	}
	createdAt := time.Now().UTC()
	id := uuid.New()
	var document DocumentDeclaration
	err = tx.QueryRow(ctx, `
		INSERT INTO port_call_documents (document_id, call_id, document_type, media_type, size_bytes, sha256, declared_by, status, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9)
		ON CONFLICT (call_id, document_type, sha256) DO NOTHING
		RETURNING document_id, call_id, document_type, media_type, size_bytes, sha256, declared_by, status, created_at, updated_at`,
		id, callID, request.DocumentType, request.MediaType, request.SizeBytes, request.SHA256, request.DeclaredBy, DocumentDeclared, createdAt,
	).Scan(&document.DocumentID, &document.CallID, &document.DocumentType, &document.MediaType, &document.SizeBytes, &document.SHA256, &document.DeclaredBy, &document.Status, &document.CreatedAt, &document.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `SELECT document_id, call_id, document_type, media_type, size_bytes, sha256, declared_by, status, created_at, updated_at FROM port_call_documents WHERE call_id=$1 AND document_type=$2 AND sha256=$3 FOR UPDATE`, callID, request.DocumentType, request.SHA256).
			Scan(&document.DocumentID, &document.CallID, &document.DocumentType, &document.MediaType, &document.SizeBytes, &document.SHA256, &document.DeclaredBy, &document.Status, &document.CreatedAt, &document.UpdatedAt)
		if err != nil {
			return DocumentDeclaration{}, fmt.Errorf("lookup document replay: %w", err)
		}
		if document.MediaType != request.MediaType || document.SizeBytes != request.SizeBytes || document.DeclaredBy != request.DeclaredBy {
			return DocumentDeclaration{}, ErrDocumentConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return DocumentDeclaration{}, fmt.Errorf("commit document replay: %w", err)
		}
		return document, nil
	}
	if err != nil {
		return DocumentDeclaration{}, fmt.Errorf("insert document declaration: %w", err)
	}
	payload, err := json.Marshal(document)
	if err != nil {
		return DocumentDeclaration{}, fmt.Errorf("encode document event: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO port_call_outbox (event_id, call_id, event_type, payload, created_at) VALUES ($1,$2,'port_call.document_declared',$3,$4)`, uuid.New(), callID, payload, createdAt); err != nil {
		return DocumentDeclaration{}, fmt.Errorf("write document event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return DocumentDeclaration{}, fmt.Errorf("commit document declaration: %w", err)
	}
	return document, nil
}

func (store *Store) DecideClearance(ctx context.Context, callID string, expectedVersion int64, decision ClearanceDecision, reason, decidedBy string) (Clearance, error) {
	if callID == "" || expectedVersion < 1 || (decision != ClearanceApproved && decision != ClearanceRejected) || reason == "" || reason != strings.TrimSpace(reason) || len(reason) > 1024 || decidedBy == "" || decidedBy != strings.TrimSpace(decidedBy) {
		return Clearance{}, ErrClearanceInvalid
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return Clearance{}, fmt.Errorf("begin clearance: %w", err)
	}
	defer tx.Rollback(ctx)
	var status Status
	var currentVersion int64
	if err := tx.QueryRow(ctx, `SELECT status, version FROM port_calls WHERE call_id=$1 FOR UPDATE`, callID).Scan(&status, &currentVersion); errors.Is(err, pgx.ErrNoRows) {
		return Clearance{}, ErrNotFound
	} else if err != nil {
		return Clearance{}, fmt.Errorf("lock port call for clearance: %w", err)
	}
	if currentVersion != expectedVersion {
		return Clearance{}, ErrOptimisticConflict
	}
	if status != StatusAccepted {
		return Clearance{}, ErrClearanceInvalid
	}
	if decision == ClearanceApproved {
		rows, queryErr := tx.Query(ctx, `SELECT status FROM port_call_documents WHERE call_id=$1 FOR UPDATE`, callID)
		if queryErr != nil {
			return Clearance{}, fmt.Errorf("lock documents for clearance: %w", queryErr)
		}
		defer rows.Close()
		documentCount := 0
		for rows.Next() {
			var documentStatus DocumentStatus
			if scanErr := rows.Scan(&documentStatus); scanErr != nil {
				return Clearance{}, fmt.Errorf("read document status for clearance: %w", scanErr)
			}
			documentCount++
			if documentStatus != DocumentVerified {
				return Clearance{}, ErrClearanceInvalid
			}
		}
		if rows.Err() != nil {
			return Clearance{}, fmt.Errorf("iterate documents for clearance: %w", rows.Err())
		}
		if documentCount == 0 {
			return Clearance{}, ErrClearanceInvalid
		}
	}
	decidedAt := time.Now().UTC()
	var clearance Clearance
	err = tx.QueryRow(ctx, `
		INSERT INTO port_call_clearance_decisions (decision_id, call_id, decision, reason, decided_by, call_version, decided_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (call_id) DO NOTHING
		RETURNING decision_id, call_id, decision, reason, decided_by, call_version, decided_at`, uuid.New(), callID, decision, reason, decidedBy, expectedVersion+1, decidedAt).
		Scan(&clearance.DecisionID, &clearance.CallID, &clearance.Decision, &clearance.Reason, &clearance.DecidedBy, &clearance.CallVersion, &clearance.DecidedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Clearance{}, ErrClearanceConflict
	}
	if err != nil {
		return Clearance{}, fmt.Errorf("insert clearance decision: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE port_calls SET updated_at=$1, version=version+1 WHERE call_id=$2 AND version=$3`, decidedAt, callID, expectedVersion); err != nil {
		return Clearance{}, fmt.Errorf("advance clearance version: %w", err)
	}
	payload, err := json.Marshal(clearance)
	if err != nil {
		return Clearance{}, fmt.Errorf("encode clearance event: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO port_call_outbox (event_id, call_id, event_type, payload, created_at) VALUES ($1,$2,'port_call.clearance_decided',$3,$4)`, uuid.New(), callID, payload, decidedAt); err != nil {
		return Clearance{}, fmt.Errorf("write clearance event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Clearance{}, fmt.Errorf("commit clearance: %w", err)
	}
	return clearance, nil
}

func (store *Store) ReviewDocument(ctx context.Context, callID, documentID string, request DocumentReviewRequest) (DocumentDeclaration, error) {
	if callID == "" || documentID == "" || request.ExpectedVersion < 1 || (request.Status != DocumentVerified && request.Status != DocumentRejected) || request.ReviewedBy == "" || request.ReviewedBy != strings.TrimSpace(request.ReviewedBy) || request.Reason == "" || request.Reason != strings.TrimSpace(request.Reason) || len(request.Reason) > 1024 {
		return DocumentDeclaration{}, ErrDocumentConflict
	}
	docID, err := uuid.Parse(documentID)
	if err != nil {
		return DocumentDeclaration{}, ErrDocumentConflict
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return DocumentDeclaration{}, fmt.Errorf("begin document review: %w", err)
	}
	defer tx.Rollback(ctx)
	var document DocumentDeclaration
	var reviewedBy, reviewedReason *string
	var reviewedAt *time.Time
	var currentStatus DocumentStatus
	err = tx.QueryRow(ctx, `
		UPDATE port_call_documents
		SET status=$1, version=version+1, reviewed_by=$2, reviewed_reason=$3, reviewed_at=$4, updated_at=$4
		WHERE document_id=$5 AND call_id=$6 AND status='DECLARED' AND version=$7
		RETURNING document_id, call_id, document_type, media_type, size_bytes, sha256, declared_by, status, created_at, updated_at, version, reviewed_by, reviewed_reason, reviewed_at`,
		request.Status, request.ReviewedBy, request.Reason, time.Now().UTC(), docID, callID, request.ExpectedVersion).
		Scan(&document.DocumentID, &document.CallID, &document.DocumentType, &document.MediaType, &document.SizeBytes, &document.SHA256, &document.DeclaredBy, &document.Status, &document.CreatedAt, &document.UpdatedAt, &document.Version, &reviewedBy, &reviewedReason, &reviewedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `SELECT status FROM port_call_documents WHERE document_id=$1 AND call_id=$2`, docID, callID).Scan(&currentStatus)
		if errors.Is(err, pgx.ErrNoRows) {
			return DocumentDeclaration{}, ErrNotFound
		}
		if err != nil {
			return DocumentDeclaration{}, fmt.Errorf("lookup document review conflict: %w", err)
		}
		if currentStatus == request.Status {
			return DocumentDeclaration{}, ErrDocumentConflict
		}
		return DocumentDeclaration{}, ErrOptimisticConflict
	}
	if err != nil {
		return DocumentDeclaration{}, fmt.Errorf("review document: %w", err)
	}
	document.ReviewedBy, document.ReviewedReason, document.ReviewedAt = reviewedBy, reviewedReason, reviewedAt
	payload, err := json.Marshal(document)
	if err != nil {
		return DocumentDeclaration{}, fmt.Errorf("encode document review event: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO port_call_outbox (event_id, call_id, event_type, payload, created_at) VALUES ($1,$2,'port_call.document_reviewed',$3,$4)`, uuid.New(), callID, payload, document.UpdatedAt); err != nil {
		return DocumentDeclaration{}, fmt.Errorf("write document review event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return DocumentDeclaration{}, fmt.Errorf("commit document review: %w", err)
	}
	return document, nil
}

func (store *Store) SupersedeDocument(ctx context.Context, callID string, request DocumentSupersessionRequest) error {
	if !validateWorkflowText(request.OriginalDocumentID, 64) || !validateWorkflowText(request.ReplacementDocumentID, 64) || request.OriginalDocumentID == request.ReplacementDocumentID || !validateWorkflowText(request.Reason, 1024) || !validateWorkflowText(request.SupersededBy, 256) {
		return ErrDocumentConflict
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var originalType, replacementType string
	var originalStatus, replacementStatus DocumentStatus
	if err := tx.QueryRow(ctx, `SELECT document_type,status FROM port_call_documents WHERE document_id=$1 AND call_id=$2 FOR UPDATE`, request.OriginalDocumentID, callID).Scan(&originalType, &originalStatus); err != nil {
		return ErrNotFound
	}
	if err := tx.QueryRow(ctx, `SELECT document_type,status FROM port_call_documents WHERE document_id=$1 AND call_id=$2 FOR UPDATE`, request.ReplacementDocumentID, callID).Scan(&replacementType, &replacementStatus); err != nil {
		return ErrNotFound
	}
	if originalType != replacementType || originalStatus != DocumentVerified || replacementStatus != DocumentVerified {
		return ErrDocumentConflict
	}
	now := time.Now().UTC()
	id := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO port_call_document_supersessions (supersession_id,call_id,original_document_id,replacement_document_id,reason,superseded_by,superseded_at) VALUES ($1,$2,$3,$4,$5,$6,$7)`, id, callID, request.OriginalDocumentID, request.ReplacementDocumentID, request.Reason, request.SupersededBy, now); err != nil {
		return ErrDocumentConflict
	}
	payload, _ := json.Marshal(request)
	if _, err := tx.Exec(ctx, `INSERT INTO port_call_outbox (event_id,call_id,event_type,payload,created_at) VALUES ($1,$2,'port_call.document_superseded',$3,$4)`, uuid.New(), callID, payload, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *Store) AmendClearance(ctx context.Context, callID string, request ClearanceAmendmentRequest) (Clearance, error) {
	if request.ExpectedVersion < 1 || (request.Decision != ClearanceApproved && request.Decision != ClearanceRejected) || !validateWorkflowText(request.Reason, 1024) || !validateWorkflowText(request.AmendedBy, 256) {
		return Clearance{}, ErrClearanceInvalid
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return Clearance{}, err
	}
	defer tx.Rollback(ctx)
	var callVersion int64
	if err := tx.QueryRow(ctx, `SELECT version FROM port_calls WHERE call_id=$1 FOR UPDATE`, callID).Scan(&callVersion); err != nil {
		return Clearance{}, ErrNotFound
	}
	if callVersion != request.ExpectedVersion {
		return Clearance{}, ErrOptimisticConflict
	}
	var prior Clearance
	if err := tx.QueryRow(ctx, `SELECT decision_id,call_id,decision,reason,decided_by,call_version,decided_at FROM port_call_clearance_decisions WHERE call_id=$1 FOR UPDATE`, callID).Scan(&prior.DecisionID, &prior.CallID, &prior.Decision, &prior.Reason, &prior.DecidedBy, &prior.CallVersion, &prior.DecidedAt); err != nil {
		return Clearance{}, ErrNotFound
	}
	if prior.Decision == request.Decision || prior.DecidedBy == request.AmendedBy {
		return Clearance{}, ErrClearanceConflict
	}
	now := time.Now().UTC()
	amended := prior
	amended.Decision = request.Decision
	amended.Reason = request.Reason
	amended.DecidedBy = request.AmendedBy
	amended.CallVersion = callVersion
	amended.DecidedAt = now
	if _, err := tx.Exec(ctx, `UPDATE port_call_clearance_decisions SET decision=$1,reason=$2,decided_by=$3,call_version=$4,decided_at=$5 WHERE decision_id=$6`, amended.Decision, amended.Reason, amended.DecidedBy, amended.CallVersion, amended.DecidedAt, prior.DecisionID); err != nil {
		return Clearance{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO port_call_clearance_amendments (amendment_id,call_id,prior_decision_id,prior_decision,amended_decision,reason,amended_by,call_version,amended_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, uuid.New(), callID, prior.DecisionID, prior.Decision, amended.Decision, amended.Reason, amended.DecidedBy, callVersion, now); err != nil {
		return Clearance{}, err
	}
	payload, _ := json.Marshal(amended)
	if _, err := tx.Exec(ctx, `INSERT INTO port_call_outbox (event_id,call_id,event_type,payload,created_at) VALUES ($1,$2,'port_call.clearance_amended',$3,$4)`, uuid.New(), callID, payload, now); err != nil {
		return Clearance{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Clearance{}, err
	}
	return amended, nil
}
