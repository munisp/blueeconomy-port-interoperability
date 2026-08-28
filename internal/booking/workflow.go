package booking

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/customs"
	"github.com/munisp/blueeconomy-port-interoperability/internal/ledger"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"go.temporal.io/sdk/workflow"
)

const (
	// SignalPaymentConfirmed carries the Mojaloop receipt reference.
	SignalPaymentConfirmed = "payment-confirmed"
	// SignalGateScan carries the approved gate scan id.
	SignalGateScan = "gate-scan"
	// QueryObserver exposes a read-only progress snapshot for observers.
	QueryObserver = "observer"

	TaskQueue = "ecallup-booking"
)

// WorkflowInput starts one workflow instance per booking (workflow id =
// booking id, so restarts are idempotent).
type WorkflowInput struct {
	BookingID        string
	TenantID         string
	PrincipalID      string
	AmountKobo       uint64
	FgnShareKobo     uint64
	ExpectedVersion  int64
	PaymentDeadline  time.Time
	GateScanDeadline time.Time
}

type WorkflowResult struct {
	BookingID        string `json:"booking_id"`
	Outcome          string `json:"outcome"` // COMPLETED, EXPIRED or REJECTED
	LedgerCommitHash string `json:"ledger_commit_hash,omitempty"`
}

// ObserverState is the queryable progress snapshot.
type ObserverState struct {
	BookingID       string `json:"booking_id"`
	Stage           string `json:"stage"`
	Receipt         string `json:"receipt_ref,omitempty"`
	ScanID          string `json:"scan_id,omitempty"`
	CustomsDecision string `json:"customs_decision,omitempty"`
	UpdatedAt       string `json:"updated_at"`
}

type paymentConfirmedSignal struct {
	ReceiptRef string `json:"receipt_ref"`
}

type gateScanSignal struct {
	ScanID string `json:"scan_id"`
}

// Activities wraps the side-effecting steps. Every dependency is mandatory
// except the customs validator, which is wired separately so bookings without
// a cargo declaration never touch the customs boundary.
type Activities struct {
	Store                     *Store
	Ledger                    ledger.Ledger
	Customs                   customs.Validator
	CustomsWeightToleranceBPS int64
}

func NewActivities(store *Store, settlement ledger.Ledger) (*Activities, error) {
	if store == nil || settlement == nil {
		return nil, errors.New("booking activities require a store and a ledger")
	}
	return &Activities{Store: store, Ledger: settlement}, nil
}

// SetCustomsValidator wires the Nigeria Customs declaration client and the
// weight tolerance (basis points) applied by the cross-check. Bookings that
// reference a cargo declaration fail closed in VALIDATION_PENDING until a
// configured validator clears them.
func (activities *Activities) SetCustomsValidator(validator customs.Validator, weightToleranceBPS int64) error {
	if validator == nil || weightToleranceBPS < 0 {
		return errors.New("customs validator and a non-negative weight tolerance are required")
	}
	activities.Customs = validator
	activities.CustomsWeightToleranceBPS = weightToleranceBPS
	return nil
}

// activityCtx rebuilds the tenant context for activity executions, which run
// outside the request middleware chain.
func activityCtx(ctx context.Context, tenantID, principalID string) (context.Context, error) {
	return tenantctx.WithClaims(ctx, tenantctx.Claims{
		Issuer:   "ecallup-booking-workflow",
		Audience: "s1-port-interoperability",
		TenantID: tenantID,
		Subject:  principalID,
		Expires:  time.Now().Add(time.Hour).Unix(),
	})
}

func workflowPrincipal(input WorkflowInput) Principal {
	return Principal{ID: input.PrincipalID, Role: "booking-workflow"}
}

// ReceiptCheck verifies that the payment receipt signalled into the workflow
// was actually persisted by the payment path before gate approval proceeds.
func (activities *Activities) ReceiptCheck(ctx context.Context, input WorkflowInput, receiptRef string) error {
	activityCtx, err := activityCtx(ctx, input.TenantID, input.PrincipalID)
	if err != nil {
		return err
	}
	booking, err := activities.Store.Get(activityCtx, input.BookingID)
	if err != nil {
		return fmt.Errorf("receipt check load: %w", err)
	}
	if booking.Status != StatusPaid || booking.PaymentReceiptRef == nil || *booking.PaymentReceiptRef != receiptRef {
		return errors.New("payment receipt is not persisted for this booking")
	}
	return nil
}

// Customs decision values returned by the CustomsValidation activity.
const (
	CustomsNotRequired = "NOT_REQUIRED"
	CustomsMatched     = "MATCH"
	CustomsRejected    = "REJECTED"
)

// customsValidatorIdentity is recorded as validated_by on customs_validations
// rows written by the workflow activity.
const customsValidatorIdentity = "nigeria-customs-declaration-api"

// CustomsValidation runs the Nigeria Customs cross-check for a booking that
// references a cargo declaration. Ordering: receipt-check -> customs
// validation -> gate approval. A validator outage leaves the booking in
// VALIDATION_PENDING and the activity error is retryable; a rule mismatch is
// persisted with a reason code and the booking is REJECTED.
func (activities *Activities) CustomsValidation(ctx context.Context, input WorkflowInput) (string, error) {
	activityCtx, err := activityCtx(ctx, input.TenantID, input.PrincipalID)
	if err != nil {
		return "", err
	}
	booking, err := activities.Store.Get(activityCtx, input.BookingID)
	if err != nil {
		return "", fmt.Errorf("customs validation load: %w", err)
	}
	if booking.CargoDeclarationRef == nil {
		return CustomsNotRequired, nil
	}
	if booking.Status == StatusRejected {
		return CustomsRejected, nil
	}
	pending, err := activities.Store.BeginCustomsValidation(activityCtx, input.BookingID, workflowPrincipal(input))
	if err != nil {
		return "", fmt.Errorf("begin customs validation: %w", err)
	}
	if pending.Status == StatusPaid {
		// A MATCH validation already exists (activity retry after commit).
		return CustomsMatched, nil
	}
	if activities.Customs == nil {
		return "", errors.New("customs validator is not configured: booking stays VALIDATION_PENDING")
	}
	var evaluation customs.Evaluation
	declaration, err := activities.Customs.Declaration(ctx, *pending.CargoDeclarationRef)
	switch {
	case errors.Is(err, customs.ErrDeclarationNotFound):
		evaluation = customs.Evaluation{
			Decision:        customs.DecisionMismatch,
			ReasonCode:      customs.ReasonDeclarationNotFound,
			DeclarationRef:  *pending.CargoDeclarationRef,
			BookingWeightKg: *pending.DeclaredWeightKg,
		}
	case err != nil:
		// Validator unreachable: stay VALIDATION_PENDING, retry with backoff.
		return "", fmt.Errorf("customs validator unreachable: %w", err)
	default:
		evaluation = customs.Evaluate(declaration, customs.BookingExpectation{
			DeclarationRef:     *pending.CargoDeclarationRef,
			DeclaredWeightKg:   *pending.DeclaredWeightKg,
			ConsigneeID:        *pending.ConsigneeID,
			OperatorID:         *pending.OperatorID,
			WeightToleranceBPS: activities.CustomsWeightToleranceBPS,
		})
	}
	if _, err := activities.Store.RecordCustomsValidation(activityCtx, input.BookingID, evaluation, customsValidatorIdentity, workflowPrincipal(input)); err != nil {
		return "", fmt.Errorf("record customs validation: %w", err)
	}
	if evaluation.Decision == customs.DecisionMatch {
		return CustomsMatched, nil
	}
	return CustomsRejected, nil
}

// CommitLedger posts the double-entry settlement to TigerBeetle.
func (activities *Activities) CommitLedger(ctx context.Context, input WorkflowInput) (string, error) {
	return activities.Ledger.CommitBookingSettlement(ctx, ledger.Settlement{
		BookingID:    input.BookingID,
		AmountKobo:   input.AmountKobo,
		FgnShareKobo: input.FgnShareKobo,
	})
}

// AuditCommit completes the booking and appends the audit event carrying the
// ledger commit hash.
func (activities *Activities) AuditCommit(ctx context.Context, input WorkflowInput, ledgerCommitHash string) error {
	activityCtx, err := activityCtx(ctx, input.TenantID, input.PrincipalID)
	if err != nil {
		return err
	}
	booking, err := activities.Store.Get(activityCtx, input.BookingID)
	if err != nil {
		return fmt.Errorf("audit commit load: %w", err)
	}
	_, err = activities.Store.Complete(activityCtx, input.BookingID, booking.Version, ledgerCommitHash, workflowPrincipal(input))
	return err
}

// ExpireBooking fails the booking closed when payment or gate scan deadlines pass.
func (activities *Activities) ExpireBooking(ctx context.Context, input WorkflowInput) error {
	activityCtx, err := activityCtx(ctx, input.TenantID, input.PrincipalID)
	if err != nil {
		return err
	}
	_, err = activities.Store.ExpireDue(activityCtx, time.Now().UTC(), workflowPrincipal(input))
	return err
}

// ECallUpBookingWorkflow drives receipt-check -> Nigeria Customs
// cross-validation -> paper-free gate approval -> audit commit for one truck
// booking.
func ECallUpBookingWorkflow(ctx workflow.Context, input WorkflowInput) (WorkflowResult, error) {
	if input.BookingID == "" || input.TenantID == "" {
		return WorkflowResult{}, errors.New("booking id and tenant id are required")
	}
	options := workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
	}
	ctx = workflow.WithActivityOptions(ctx, options)
	var activities *Activities

	state := ObserverState{BookingID: input.BookingID, Stage: "AWAITING_PAYMENT", UpdatedAt: workflow.Now(ctx).UTC().Format(time.RFC3339)}
	if err := workflow.SetQueryHandler(ctx, QueryObserver, func() (ObserverState, error) {
		return state, nil
	}); err != nil {
		return WorkflowResult{}, err
	}

	update := func(stage string) {
		state.Stage = stage
		state.UpdatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
	}

	// Stage 1: wait for the payment-confirmed signal, then verify the receipt.
	var payment paymentConfirmedSignal
	paymentSelector := workflow.NewSelector(ctx)
	paymentChannel := workflow.GetSignalChannel(ctx, SignalPaymentConfirmed)
	paymentSelector.AddReceive(paymentChannel, func(channel workflow.ReceiveChannel, _ bool) {
		channel.Receive(ctx, &payment)
	})
	paymentSelector.AddFuture(workflow.NewTimer(ctx, time.Until(input.PaymentDeadline)), func(workflow.Future) {})
	paymentSelector.Select(ctx)
	if payment.ReceiptRef == "" {
		update("EXPIRED_AWAITING_PAYMENT")
		if err := workflow.ExecuteActivity(ctx, activities.ExpireBooking, input).Get(ctx, nil); err != nil {
			return WorkflowResult{}, fmt.Errorf("expire unpaid booking: %w", err)
		}
		return WorkflowResult{BookingID: input.BookingID, Outcome: string(StatusExpired)}, nil
	}
	state.Receipt = payment.ReceiptRef
	update("RECEIPT_CHECK")
	if err := workflow.ExecuteActivity(ctx, activities.ReceiptCheck, input, payment.ReceiptRef).Get(ctx, nil); err != nil {
		return WorkflowResult{}, fmt.Errorf("receipt check: %w", err)
	}

	// Stage 2: Nigeria Customs cross-validation. Bookings without a cargo
	// declaration pass through as NOT_REQUIRED; a mismatch fails closed.
	update("CUSTOMS_VALIDATION")
	var customsDecision string
	if err := workflow.ExecuteActivity(ctx, activities.CustomsValidation, input).Get(ctx, &customsDecision); err != nil {
		return WorkflowResult{}, fmt.Errorf("customs validation: %w", err)
	}
	state.CustomsDecision = customsDecision
	if customsDecision == CustomsRejected {
		update("REJECTED")
		return WorkflowResult{BookingID: input.BookingID, Outcome: string(StatusRejected)}, nil
	}

	// Stage 3: paper-free gate approval — wait for the gate-scan signal.
	update("AWAITING_GATE_SCAN")
	var scan gateScanSignal
	scanSelector := workflow.NewSelector(ctx)
	scanChannel := workflow.GetSignalChannel(ctx, SignalGateScan)
	scanSelector.AddReceive(scanChannel, func(channel workflow.ReceiveChannel, _ bool) {
		channel.Receive(ctx, &scan)
	})
	scanSelector.AddFuture(workflow.NewTimer(ctx, time.Until(input.GateScanDeadline)), func(workflow.Future) {})
	scanSelector.Select(ctx)
	if scan.ScanID == "" {
		update("EXPIRED_AWAITING_GATE_SCAN")
		if err := workflow.ExecuteActivity(ctx, activities.ExpireBooking, input).Get(ctx, nil); err != nil {
			return WorkflowResult{}, fmt.Errorf("expire unscanned booking: %w", err)
		}
		return WorkflowResult{BookingID: input.BookingID, Outcome: string(StatusExpired)}, nil
	}
	state.ScanID = scan.ScanID

	// Stage 4: settle in the ledger, then audit-commit.
	update("LEDGER_COMMIT")
	var ledgerCommitHash string
	if err := workflow.ExecuteActivity(ctx, activities.CommitLedger, input).Get(ctx, &ledgerCommitHash); err != nil {
		return WorkflowResult{}, fmt.Errorf("ledger commit: %w", err)
	}
	update("AUDIT_COMMIT")
	if err := workflow.ExecuteActivity(ctx, activities.AuditCommit, input, ledgerCommitHash).Get(ctx, nil); err != nil {
		return WorkflowResult{}, fmt.Errorf("audit commit: %w", err)
	}
	update("COMPLETED")
	return WorkflowResult{BookingID: input.BookingID, Outcome: string(StatusCompleted), LedgerCommitHash: ledgerCommitHash}, nil
}
