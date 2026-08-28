package nswadapter

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
	"github.com/munisp/blueeconomy-port-interoperability/internal/nswsecurity"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantdb"
)

// Runner drains NSW-relevant outbox events into signed NSW deliveries. All
// state transitions run inside tenant-scoped transactions (tenantdb.WithTx)
// so the RLS policies on nsw_delivery and port_call_outbox isolate tenants.
type Runner struct {
	pool   *pgxpool.Pool
	signer *nswsecurity.Signer
	client *Client
	config Config
	now    func() time.Time
}

// NewRunner wires the drain pipeline; every dependency is mandatory.
func NewRunner(pool *pgxpool.Pool, signer *nswsecurity.Signer, client *Client, config Config) (*Runner, error) {
	if pool == nil || signer == nil || client == nil {
		return nil, errors.New("NSW adapter requires a database pool, a signer and a client")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Runner{pool: pool, signer: signer, client: client, config: config, now: time.Now}, nil
}

// DrainOnce runs one enqueue + deliver cycle across every active tenant and
// returns the number of events delivered. Tenant failures are isolated: the
// first error is returned after the remaining tenants have been processed.
func (runner *Runner) DrainOnce(ctx context.Context) (int, error) {
	tenants, err := runner.activeTenants(ctx)
	if err != nil {
		return 0, err
	}
	delivered := 0
	var firstErr error
	for _, tenantID := range tenants {
		count, err := runner.drainTenant(ctx, tenantID)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("drain tenant %s: %w", tenantID, err)
		}
		delivered += count
	}
	return delivered, firstErr
}

func (runner *Runner) activeTenants(ctx context.Context) ([]string, error) {
	rows, err := runner.pool.Query(ctx, `SELECT tenant_id FROM platform_tenants WHERE active ORDER BY tenant_id`)
	if err != nil {
		return nil, fmt.Errorf("list active tenants: %w", err)
	}
	defer rows.Close()
	var tenants []string
	for rows.Next() {
		var tenantID string
		if err := rows.Scan(&tenantID); err != nil {
			return nil, fmt.Errorf("scan tenant: %w", err)
		}
		tenants = append(tenants, tenantID)
	}
	return tenants, rows.Err()
}

func (runner *Runner) drainTenant(ctx context.Context, tenantID string) (int, error) {
	bound, err := tenantctx.WithClaims(ctx, tenantctx.Claims{
		Issuer:   "nsw-adapter",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  "nsw-adapter",
		Expires:  runner.now().Add(time.Hour).Unix(),
	})
	if err != nil {
		return 0, fmt.Errorf("bind tenant claims: %w", err)
	}
	delivered := 0
	err = tenantdb.WithTx(bound, runner.pool, func(tx pgx.Tx, claims tenantctx.Claims) error {
		if err := runner.enqueue(ctx, tx, claims); err != nil {
			return err
		}
		due, err := runner.claimDue(ctx, tx)
		if err != nil {
			return err
		}
		for _, delivery := range due {
			ok, err := runner.attempt(ctx, tx, delivery)
			if err != nil {
				return err
			}
			if ok {
				delivered++
			}
		}
		return nil
	})
	return delivered, err
}

// enqueue registers PENDING deliveries for NSW-relevant outbox events that
// have no delivery row yet. NSW-relevant means: booking created/paid, gate
// decisions, port-call clearance decisions, queue call-ups and cleared
// customs declarations.
func (runner *Runner) enqueue(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims) error {
	rows, err := tx.Query(ctx, `
		SELECT o.event_id, o.event_type, o.payload::text, o.created_at FROM platform_outbox o
		WHERE o.tenant_id = $1
			AND ((o.topic = 'ports.booking.v1' AND o.event_type IN ('booking.drafted', 'booking.paid'))
				OR (o.topic = 'ports.gate.v1' AND o.event_type IN ('gate.scan_approved', 'gate.scan_denied'))
				OR (o.topic = 'ports.queue.v1' AND o.event_type = 'queue.called_up')
				OR (o.topic = 'trade.declarations.v1' AND o.event_type = 'trade.declaration.cleared.v1'))
			AND NOT EXISTS (SELECT 1 FROM nsw_delivery d WHERE d.source = 'platform_outbox' AND d.event_id = o.event_id)
		ORDER BY o.created_at
		LIMIT $2`, claims.TenantID, runner.config.BatchSize)
	if err != nil {
		return fmt.Errorf("scan platform outbox for NSW events: %w", err)
	}
	// Buffer the batch before writing: pgx holds the connection until the
	// rows are closed, so deliveries are inserted only after the scan.
	type pendingDelivery struct {
		eventID, eventType, reference, payload string
		createdAt                              time.Time
	}
	var pending []pendingDelivery
	for rows.Next() {
		var delivery pendingDelivery
		if err := rows.Scan(&delivery.eventID, &delivery.eventType, &delivery.payload, &delivery.createdAt); err != nil {
			rows.Close()
			return fmt.Errorf("scan platform outbox event: %w", err)
		}
		pending = append(pending, delivery)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, delivery := range pending {
		reference := envelopeSubjectID(delivery.payload)
		if reference == "" {
			reference = delivery.eventID
		}
		if err := runner.insertDelivery(ctx, tx, claims, "platform_outbox", delivery.eventID, delivery.eventType, reference, delivery.payload, delivery.createdAt); err != nil {
			return err
		}
	}

	portCallRows, err := tx.Query(ctx, `
		SELECT o.event_id, o.call_id, o.payload::text, o.created_at FROM port_call_outbox o
		WHERE o.event_type = 'port_call.clearance_decided'
			AND NOT EXISTS (SELECT 1 FROM nsw_delivery d WHERE d.source = 'port_call_outbox' AND d.event_id = o.event_id)
		ORDER BY o.created_at
		LIMIT $1`, runner.config.BatchSize)
	if err != nil {
		return fmt.Errorf("scan port-call outbox for NSW events: %w", err)
	}
	var pendingPortCalls []pendingDelivery
	for portCallRows.Next() {
		var delivery pendingDelivery
		if err := portCallRows.Scan(&delivery.eventID, &delivery.reference, &delivery.payload, &delivery.createdAt); err != nil {
			portCallRows.Close()
			return fmt.Errorf("scan port-call outbox event: %w", err)
		}
		pendingPortCalls = append(pendingPortCalls, delivery)
	}
	if err := portCallRows.Err(); err != nil {
		portCallRows.Close()
		return err
	}
	portCallRows.Close()
	for _, delivery := range pendingPortCalls {
		if err := runner.insertDelivery(ctx, tx, claims, "port_call_outbox", delivery.eventID, "port_call.clearance_decided", delivery.reference, delivery.payload, delivery.createdAt); err != nil {
			return err
		}
	}
	return nil
}

// insertDelivery serializes the handoff body per the negotiated content type
// and registers the PENDING delivery row.
func (runner *Runner) insertDelivery(ctx context.Context, tx pgx.Tx, claims tenantctx.Claims, source, eventID, eventType, reference, payload string, occurredAt time.Time) error {
	payloadDigest := sha256.Sum256([]byte(payload))
	payloadSHA256 := "sha256:" + hex.EncodeToString(payloadDigest[:])
	body := payload
	if runner.config.ContentType == ContentTypeXML {
		document, err := MarshalPortCallEvent(PortCallEvent{
			EventID:       eventID,
			CallReference: reference,
			EventType:     eventType,
			OccurredAt:    occurredAt.UTC().Format(time.RFC3339),
			TenantID:      claims.TenantID,
			PayloadSHA256: payloadSHA256,
			Payload:       payload,
		})
		if err != nil {
			return fmt.Errorf("serialize NSW XML handoff for %s: %w", eventID, err)
		}
		body = string(document)
	}
	bodyDigest := sha256.Sum256([]byte(body))
	now := runner.now().UTC()
	if _, err := tx.Exec(ctx, `
		INSERT INTO nsw_delivery (
			delivery_id, tenant_id, source, event_id, event_type, call_reference,
			content_type, payload, payload_sha256, status, attempts, max_attempts,
			next_attempt_at, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'PENDING',0,$10,$11,$11,$11)
		ON CONFLICT (source, event_id) DO NOTHING`,
		uuid.New(), claims.TenantID, source, eventID, eventType, reference,
		runner.config.ContentType, body, "sha256:"+hex.EncodeToString(bodyDigest[:]),
		runner.config.MaxAttempts, now); err != nil {
		return fmt.Errorf("register NSW delivery for %s: %w", eventID, err)
	}
	return nil
}

func (runner *Runner) claimDue(ctx context.Context, tx pgx.Tx) ([]Delivery, error) {
	rows, err := tx.Query(ctx, `
		SELECT delivery_id, tenant_id, source, event_id, event_type, call_reference,
			content_type, payload, payload_sha256, status, attempts, max_attempts, next_attempt_at
		FROM nsw_delivery
		WHERE status = 'PENDING' AND next_attempt_at <= $1
		ORDER BY next_attempt_at
		LIMIT $2
		FOR UPDATE SKIP LOCKED`, runner.now().UTC(), runner.config.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("claim due NSW deliveries: %w", err)
	}
	defer rows.Close()
	var due []Delivery
	for rows.Next() {
		var delivery Delivery
		if err := rows.Scan(&delivery.DeliveryID, &delivery.TenantID, &delivery.Source, &delivery.EventID,
			&delivery.EventType, &delivery.CallReference, &delivery.ContentType, &delivery.Payload,
			&delivery.PayloadSHA256, &delivery.Status, &delivery.Attempts, &delivery.MaxAttempts, &delivery.NextAttemptAt); err != nil {
			return nil, fmt.Errorf("scan NSW delivery: %w", err)
		}
		due = append(due, delivery)
	}
	return due, rows.Err()
}

// attempt signs and sends one delivery, then persists the outcome. The jti is
// the delivery id, stable across retries, so the NSW replay store
// deduplicates redelivery.
func (runner *Runner) attempt(ctx context.Context, tx pgx.Tx, delivery Delivery) (bool, error) {
	signature, err := runner.signer.Sign(nswsecurity.OutboundClaims{
		TenantID:      delivery.TenantID,
		JTI:           delivery.DeliveryID,
		PayloadSHA256: delivery.PayloadSHA256,
	})
	if err != nil {
		return false, fmt.Errorf("sign NSW delivery %s: %w", delivery.DeliveryID, err)
	}
	sendErr := runner.client.Send(ctx, []byte(delivery.Payload), delivery.ContentType, signature)
	outcome := settleAttempt(runner.now(), delivery, sendErr, runner.config.BackoffBase, runner.config.BackoffMax)
	if _, err := tx.Exec(ctx, `
		UPDATE nsw_delivery
		SET status=$1, attempts=$2, next_attempt_at=$3, last_error=$4, delivered_at=$5, updated_at=$6
		WHERE delivery_id=$7 AND status='PENDING'`,
		outcome.Status, outcome.Attempts, outcome.NextAttemptAt, outcome.LastError, outcome.DeliveredAt,
		runner.now().UTC(), delivery.DeliveryID); err != nil {
		return false, fmt.Errorf("persist NSW delivery outcome %s: %w", delivery.DeliveryID, err)
	}
	return sendErr == nil, nil
}

// envelopeSubjectID extracts the aggregate reference (FHIR Basic resource id)
// from a platform envelope payload. Empty when the payload is not an
// envelope; callers fall back to the event id.
func envelopeSubjectID(payload string) string {
	var probe struct {
		FHIR struct {
			Entry []struct {
				Resource struct {
					ID string `json:"id"`
				} `json:"resource"`
			} `json:"entry"`
		} `json:"fhir"`
	}
	if json.Unmarshal([]byte(payload), &probe) != nil || len(probe.FHIR.Entry) == 0 {
		return ""
	}
	return probe.FHIR.Entry[0].Resource.ID
}
