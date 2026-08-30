package manifests

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Principal is the verified actor submitting a manifest; it becomes the
// provenance block of the emitted ingest event.
type Principal struct {
	ID   string
	Role string
}

// tracer returns the manifests tracer. With telemetry disabled the global
// provider is a no-op: spans are non-recording and ingest semantics are
// unchanged.
func tracer() trace.Tracer {
	return otel.Tracer("github.com/munisp/blueeconomy-port-interoperability/internal/manifests")
}

// Store ingests and queries passenger manifests. The authority public key
// and kid prefix pin the manifest-signing authority; ingest fails closed
// when either is absent.
type Store struct {
	pool         *pgxpool.Pool
	signer       *events.Signer
	authorityKey ed25519.PublicKey
	kidPrefix    string
}

// NewStore builds the manifest store. signer signs ingest-outcome events;
// authorityKey/kidPrefix verify inbound manifest envelopes. All are
// mandatory — an unverifiable pipeline fails closed at construction.
func NewStore(pool *pgxpool.Pool, signer *events.Signer, authorityKey ed25519.PublicKey, kidPrefix string) (*Store, error) {
	if pool == nil {
		return nil, errors.New("manifest store requires a database pool")
	}
	if signer == nil {
		return nil, errors.New("manifest store requires an envelope signer")
	}
	if len(authorityKey) != ed25519.PublicKeySize {
		return nil, errors.New("manifest authority public key must be a valid Ed25519 key")
	}
	if kidPrefix == "" {
		return nil, errors.New("manifest authority kid prefix is required (fail closed)")
	}
	return &Store{pool: pool, signer: signer, authorityKey: authorityKey, kidPrefix: kidPrefix}, nil
}

func Open(ctx context.Context, databaseURL string, signer *events.Signer, authorityKey ed25519.PublicKey, kidPrefix string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return NewStore(pool, signer, authorityKey, kidPrefix)
}

func (store *Store) Close() { store.pool.Close() }

// Pool exposes the pool for test harnesses and infrastructure adapters.
func (store *Store) Pool() *pgxpool.Pool { return store.pool }

// fhirBundle is the minimal FHIR R4 message Bundle shape of the platform
// envelope (see internal/events).
type fhirBundle struct {
	ResourceType string `json:"resourceType"`
	Type         string `json:"type"`
	Entry        []struct {
		Resource struct {
			ResourceType string `json:"resourceType"`
			ID           string `json:"id"`
			Code         struct {
				Text string `json:"text"`
			} `json:"code"`
			Extension []struct {
				URL         string `json:"url"`
				ValueString string `json:"valueString"`
			} `json:"extension"`
		} `json:"resource"`
	} `json:"entry"`
}

const domainPayloadURL = "https://blueeconomy.gov.ng/fhir/StructureDefinition/domain-payload"

// extractPayload unwraps the FHIR bundle and returns the domain payload and
// the bundle subject id. The artifact must be a message Bundle with exactly
// one entry whose Basic resource carries exactly one domain-payload
// extension for the manifest.submit event type.
func extractPayload(envelope events.Envelope) (payloadJSON string, subjectID string, err error) {
	var bundle fhirBundle
	if json.Unmarshal(envelope.FHIR, &bundle) != nil ||
		bundle.ResourceType != "Bundle" || bundle.Type != "message" || len(bundle.Entry) != 1 {
		return "", "", ErrMalformedEnvelope
	}
	resource := bundle.Entry[0].Resource
	if resource.ResourceType != "Basic" || resource.ID == "" || resource.Code.Text != EventTypeSubmit {
		return "", "", ErrMalformedEnvelope
	}
	found := ""
	for _, extension := range resource.Extension {
		if extension.URL == domainPayloadURL {
			if found != "" {
				return "", "", ErrMalformedEnvelope
			}
			found = extension.ValueString
		}
	}
	if found == "" {
		return "", "", ErrMalformedEnvelope
	}
	return found, resource.ID, nil
}

// Ingest verifies, validates and records one signed manifest artifact.
// Signature or artifact failures are quarantined in the rejection queue
// (nothing silently dropped) and returned as errors; per-record failures
// are rejection-queue rows and the manifest is recorded with the accepted
// subset. An exact replay (same envelope event id and payload) returns the
// retained manifest without re-processing; the same id with a different
// payload fails closed.
func (store *Store) Ingest(ctx context.Context, artifact []byte, principal Principal) (Manifest, error) {
	if principal.ID == "" || principal.Role == "" {
		return Manifest{}, errors.New("a verified principal is required")
	}
	ctx, span := tracer().Start(ctx, "manifests.ingest")
	defer span.End()
	manifest, err := store.ingest(ctx, artifact, principal, span)
	if err != nil {
		span.RecordError(err)
	}
	return manifest, err
}

func (store *Store) ingest(ctx context.Context, artifact []byte, principal Principal, span trace.Span) (Manifest, error) {
	var envelope events.Envelope
	decoder := json.NewDecoder(bytes.NewReader(artifact))
	if err := decoder.Decode(&envelope); err != nil {
		return Manifest{}, store.quarantineEnvelope(ctx, nil, ReasonMalformedEnvelope, "artifact is not JSON: "+err.Error(), ErrMalformedEnvelope)
	}
	if envelope.EnvelopeVersion != events.EnvelopeVersion || envelope.EventType != EventTypeSubmit {
		return Manifest{}, store.quarantineEnvelope(ctx, parseUUID(envelope.EventID), ReasonMalformedEnvelope, "envelopeVersion must be 1.0 and eventType manifest.submit", ErrMalformedEnvelope)
	}
	eventID, parseErr := uuid.Parse(envelope.EventID)
	if parseErr != nil {
		return Manifest{}, store.quarantineEnvelope(ctx, nil, ReasonMalformedEnvelope, "eventId is not a UUID", ErrMalformedEnvelope)
	}
	if err := events.VerifyWithKeyIDPrefix(envelope, store.authorityKey, store.kidPrefix); err != nil {
		return Manifest{}, store.quarantineEnvelope(ctx, &eventID, ReasonInvalidSignature, err.Error(), ErrSignatureVerification)
	}
	payloadJSON, subjectID, err := extractPayload(envelope)
	if err != nil {
		return Manifest{}, store.quarantineEnvelope(ctx, &eventID, ReasonMalformedEnvelope, err.Error(), ErrMalformedEnvelope)
	}
	var payload Payload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return Manifest{}, store.quarantineEnvelope(ctx, &eventID, ReasonInvalidPayload, "domain payload is not valid JSON: "+err.Error(), ErrMalformedEnvelope)
	}
	if detail, ok := validatePayload(payload); !ok {
		return Manifest{}, store.quarantineEnvelope(ctx, &eventID, ReasonInvalidPayload, detail, ErrMalformedEnvelope)
	}
	if subjectID != payload.ManifestReference {
		return Manifest{}, store.quarantineEnvelope(ctx, &eventID, ReasonSubjectMismatch,
			"FHIR subject id does not match the payload manifest_reference", ErrMalformedEnvelope)
	}
	span.SetAttributes(
		attribute.String("manifests.reference", payload.ManifestReference),
		attribute.String("manifests.kind", payload.ManifestKind),
	)

	digest := sha256.Sum256([]byte(payloadJSON))
	payloadSHA256 := "sha256:" + hex.EncodeToString(digest[:])
	now := time.Now().UTC()

	// Per-record validation happens before persistence: accepted records and
	// rejections are inserted with the manifest in one transaction so the
	// counts can never drift from the rows.
	type acceptedRecord struct {
		index  int
		record Record
	}
	var accepted []acceptedRecord
	var rejections []Rejection
	recordCount := len(payload.Records)
	switch {
	case recordCount == 0:
		rejections = append(rejections, Rejection{ReasonCode: ReasonEmptyManifest, ReasonDetail: "manifest carries no records"})
	case recordCount > MaxRecordsPerManifest:
		rejections = append(rejections, Rejection{ReasonCode: ReasonRecordCountExceeded,
			ReasonDetail: fmt.Sprintf("manifest carries %d records; the BRI batch limit is %d", recordCount, MaxRecordsPerManifest)})
	default:
		for index, record := range payload.Records {
			if code, detail, ok := validateRecord(record, now); ok {
				accepted = append(accepted, acceptedRecord{index: index, record: record})
			} else {
				recordIndex := index
				rejections = append(rejections, Rejection{RecordIndex: &recordIndex, ReasonCode: code, ReasonDetail: detail})
			}
		}
	}
	status := StatusAccepted
	switch {
	case len(accepted) == 0:
		status = StatusRejected
	case len(rejections) > 0:
		status = StatusAcceptedWithRejections
	}
	total := recordCount
	if recordCount > MaxRecordsPerManifest {
		// The batch was refused whole: every record is accounted as rejected
		// by the manifest-level rejection entry above.
		total = recordCount
	}
	span.SetAttributes(
		attribute.String("manifests.outcome", string(status)),
		attribute.Int("manifests.records_total", total),
		attribute.Int("manifests.records_accepted", len(accepted)),
		attribute.Int("manifests.records_rejected", total-len(accepted)),
	)

	authorityKID := jwsKeyID(envelope.Provenance.Signature)
	var manifest Manifest
	err = tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		span.SetAttributes(attribute.String("tenant.id", claims.TenantID))
		inserted := true
		err := tx.QueryRow(ctx, `
			INSERT INTO passenger_manifests (
				manifest_id, tenant_id, authority_kid, principal_id, manifest_reference, voyage_reference,
				call_reference, manifest_kind, vessel_imo, status, records_total, records_accepted,
				records_rejected, payload_sha256, payload, received_by, received_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
			ON CONFLICT (manifest_id) DO NOTHING
			RETURNING manifest_id, received_at`,
			eventID, claims.TenantID, authorityKID, envelope.Provenance.PrincipalID,
			payload.ManifestReference, payload.VoyageReference, payload.CallReference, payload.ManifestKind,
			payload.VesselIMO, status, total, len(accepted), total-len(accepted), payloadSHA256, payloadJSON,
			principal.ID, now).Scan(&manifest.ManifestID, &manifest.ReceivedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			inserted = false
		} else if err != nil {
			return fmt.Errorf("insert passenger manifest: %w", err)
		}
		if !inserted {
			var retainedSHA string
			var retainedStatus Status
			var retainedTotal, retainedAccepted, retainedRejected int
			if err := tx.QueryRow(ctx, `
				SELECT payload_sha256, status, records_total, records_accepted, records_rejected, received_at
				FROM passenger_manifests WHERE manifest_id = $1 FOR UPDATE`, eventID).
				Scan(&retainedSHA, &retainedStatus, &retainedTotal, &retainedAccepted, &retainedRejected, &manifest.ReceivedAt); err != nil {
				return fmt.Errorf("load retained manifest: %w", err)
			}
			if retainedSHA != payloadSHA256 {
				return ErrManifestConflict
			}
			manifest = Manifest{
				ManifestID: eventID.String(), ManifestReference: payload.ManifestReference,
				VoyageReference: payload.VoyageReference, CallReference: payload.CallReference,
				Kind: ManifestKind(payload.ManifestKind), VesselIMO: payload.VesselIMO,
				Status: retainedStatus, RecordsTotal: retainedTotal, RecordsAccepted: retainedAccepted,
				RecordsRejected: retainedRejected, PayloadSHA256: retainedSHA, ReceivedBy: principal.ID,
			}
			return nil
		}
		for _, acceptedRecord := range accepted {
			sex := (*string)(nil)
			if acceptedRecord.record.Sex != "" {
				sex = &acceptedRecord.record.Sex
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO passenger_manifest_records (
					tenant_id, manifest_id, record_index, record_type, family_name, given_name,
					date_of_birth, nationality, document_number, sex
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
				claims.TenantID, eventID, acceptedRecord.index, acceptedRecord.record.RecordType,
				acceptedRecord.record.FamilyName, acceptedRecord.record.GivenName, acceptedRecord.record.DateOfBirth,
				acceptedRecord.record.Nationality, acceptedRecord.record.DocumentNumber, sex); err != nil {
				return fmt.Errorf("insert manifest record %d: %w", acceptedRecord.index, err)
			}
		}
		for _, rejection := range rejections {
			rawRecord := json.RawMessage(nil)
			if rejection.RecordIndex != nil {
				encoded, err := json.Marshal(payload.Records[*rejection.RecordIndex])
				if err != nil {
					return fmt.Errorf("encode rejected record: %w", err)
				}
				rawRecord = encoded
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO passenger_manifest_rejections (tenant_id, manifest_id, envelope_event_id, record_index, reason_code, reason_detail, raw_record, rejected_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				claims.TenantID, eventID, eventID, rejection.RecordIndex, rejection.ReasonCode, rejection.ReasonDetail, rawRecord, now); err != nil {
				return fmt.Errorf("insert manifest rejection: %w", err)
			}
		}
		manifest = Manifest{
			ManifestID: eventID.String(), ManifestReference: payload.ManifestReference,
			VoyageReference: payload.VoyageReference, CallReference: payload.CallReference,
			Kind: ManifestKind(payload.ManifestKind), VesselIMO: payload.VesselIMO,
			Status: status, RecordsTotal: total, RecordsAccepted: len(accepted),
			RecordsRejected: total - len(accepted), PayloadSHA256: payloadSHA256,
			ReceivedBy: principal.ID, ReceivedAt: now,
		}
		payloadOut, err := json.Marshal(manifest)
		if err != nil {
			return fmt.Errorf("encode manifest ingest payload: %w", err)
		}
		outEnvelope, err := events.Message("manifest.ingested", events.TopicManifests,
			payload.ManifestReference, eventID.String(), payloadOut, map[string]string{
				"manifest-kind":    payload.ManifestKind,
				"records-total":    fmt.Sprint(total),
				"records-accepted": fmt.Sprint(len(accepted)),
				"records-rejected": fmt.Sprint(total - len(accepted)),
			}, events.Provenance{PrincipalID: principal.ID, PrincipalRole: principal.Role}, now, store.signer)
		if err != nil {
			return fmt.Errorf("build manifest ingest envelope: %w", err)
		}
		envelopeJSON, err := json.Marshal(outEnvelope)
		if err != nil {
			return fmt.Errorf("encode manifest ingest envelope: %w", err)
		}
		outEventID, err := uuid.Parse(outEnvelope.EventID)
		if err != nil {
			return fmt.Errorf("parse event id: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO platform_outbox (event_id, tenant_id, topic, event_type, idempotency_key, payload, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			outEventID, claims.TenantID, events.TopicManifests, "manifest.ingested", outEnvelope.EventID, envelopeJSON, now); err != nil {
			return fmt.Errorf("write manifest ingest outbox event: %w", err)
		}
		return nil
	})
	return manifest, err
}

// quarantineEnvelope records an envelope-level rejection (no trusted
// payload) and returns it together with the cause error.
func (store *Store) quarantineEnvelope(ctx context.Context, eventID *uuid.UUID, reasonCode, detail string, cause error) error {
	if len(detail) > 512 {
		detail = detail[:512]
	}
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO passenger_manifest_rejections (tenant_id, manifest_id, envelope_event_id, record_index, reason_code, reason_detail, raw_record, rejected_at)
			VALUES ($1, NULL, $2, NULL, $3, $4, NULL, $5)`,
			claims.TenantID, eventID, reasonCode, detail, time.Now().UTC())
		return err
	})
	if err != nil {
		return fmt.Errorf("quarantine envelope rejection: %w (cause: %v)", err, cause)
	}
	return cause
}

// ListRejections returns the rejection queue, newest first. A manifestID of
// "" lists envelope-level quarantine entries and all manifest rejections
// for the tenant.
func (store *Store) ListRejections(ctx context.Context, manifestID string) ([]Rejection, error) {
	var rejections []Rejection
	err := tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, _ tenantctx.Claims) error {
		query := `
			SELECT manifest_id, record_index, reason_code, reason_detail
			FROM passenger_manifest_rejections`
		args := []any{}
		if manifestID != "" {
			parsed, err := uuid.Parse(manifestID)
			if err != nil {
				return ErrNotFound
			}
			query += ` WHERE manifest_id = $1`
			args = append(args, parsed)
		}
		query += ` ORDER BY rejection_seq`
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("list manifest rejections: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var rejection Rejection
			var id *uuid.UUID
			if err := rows.Scan(&id, &rejection.RecordIndex, &rejection.ReasonCode, &rejection.ReasonDetail); err != nil {
				return fmt.Errorf("scan manifest rejection: %w", err)
			}
			if id != nil {
				rejection.ManifestID = id.String()
			}
			rejections = append(rejections, rejection)
		}
		return rows.Err()
	})
	return rejections, err
}

// Get returns a recorded manifest by id.
func (store *Store) Get(ctx context.Context, manifestID string) (Manifest, error) {
	parsed, err := uuid.Parse(manifestID)
	if err != nil {
		return Manifest{}, ErrNotFound
	}
	var manifest Manifest
	err = tenantdb.WithTx(ctx, store.pool, func(tx pgx.Tx, _ tenantctx.Claims) error {
		err := tx.QueryRow(ctx, `
			SELECT manifest_id, manifest_reference, voyage_reference, call_reference, manifest_kind, vessel_imo,
				status, records_total, records_accepted, records_rejected, payload_sha256, received_by, received_at
			FROM passenger_manifests WHERE manifest_id = $1`, parsed).
			Scan(&manifest.ManifestID, &manifest.ManifestReference, &manifest.VoyageReference, &manifest.CallReference,
				&manifest.Kind, &manifest.VesselIMO, &manifest.Status, &manifest.RecordsTotal, &manifest.RecordsAccepted,
				&manifest.RecordsRejected, &manifest.PayloadSHA256, &manifest.ReceivedBy, &manifest.ReceivedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	return manifest, err
}

func parseUUID(value string) *uuid.UUID {
	parsed, err := uuid.Parse(value)
	if err != nil {
		return nil
	}
	return &parsed
}

// jwsKeyID extracts the kid from a verified compact JWS. The signature was
// already verified at this point; a malformed header yields "".
func jwsKeyID(compact string) string {
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		return ""
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return ""
	}
	var parsed struct {
		KeyID string `json:"kid"`
	}
	if json.Unmarshal(header, &parsed) != nil {
		return ""
	}
	return parsed.KeyID
}
