package booking

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/customs"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// customsValidationReason reads the persisted reason code inside a
// tenant-bound transaction (set_config app.tenant_id), exactly like the
// production store paths: customs_validations is RLS-enforced with FORCE,
// and the test role does not bypass RLS, so a raw pool read returns zero
// rows.
func (env testEnv) customsValidationReason(t *testing.T, bookingID string) string {
	t.Helper()
	claims, err := tenantctx.Tenant(env.ctx)
	if err != nil {
		t.Fatalf("resolve tenant claims: %v", err)
	}
	tx, err := env.store.Pool().Begin(env.ctx)
	if err != nil {
		t.Fatalf("begin tenant-bound read: %v", err)
	}
	defer tx.Rollback(env.ctx)
	if _, err := tx.Exec(env.ctx, "SELECT set_config('app.tenant_id', $1, true)", claims.TenantID); err != nil {
		t.Fatalf("bind tenant for read: %v", err)
	}
	var reasonCode string
	if err := tx.QueryRow(env.ctx,
		`SELECT reason_code FROM customs_validations WHERE booking_id=$1`, bookingID).Scan(&reasonCode); err != nil {
		t.Fatalf("load validation reason: %v", err)
	}
	return reasonCode
}

// These tests run against a real PostgreSQL when BOOKING_TEST_DATABASE_URL is
// set (see store_test.go); they are skipped otherwise. They cover the Nigeria
// Customs cross-validation gate: state machine, persisted decisions, gate
// eligibility and validator-down fail-closed behavior.

type stubCustomsValidator struct {
	declaration customs.Declaration
	err         error
}

func (stub stubCustomsValidator) Declaration(context.Context, string) (customs.Declaration, error) {
	return stub.declaration, stub.err
}

func (env testEnv) makeDeclaredBooking(t *testing.T, terminalID, requestID string) Booking {
	t.Helper()
	principal := Principal{ID: "test-trucker", Role: "trucker"}
	created, err := env.store.Create(env.ctx, CreateRequest{
		RequestID:           requestID,
		TruckPlate:          "LAG-222-BB",
		TruckerMSISDN:       "+2348012345678",
		TerminalID:          terminalID,
		Channel:             ChannelWeb,
		AmountKobo:          250000,
		ExpiresAt:           time.Now().Add(2 * time.Hour),
		CargoDeclarationRef: "NCS-2026-ABC123",
		DeclaredWeightKg:    10000,
		ConsigneeID:         "consignee-dangote-01",
		OperatorID:          "operator-apapa-01",
	}, principal)
	if err != nil {
		t.Fatalf("create declared booking: %v", err)
	}
	return created
}

func (env testEnv) payBooking(t *testing.T, booking Booking, slot Slot) Booking {
	t.Helper()
	trucker := Principal{ID: "test-trucker", Role: "trucker"}
	reserved, err := env.store.ReserveSlot(env.ctx, booking.BookingID, slot.SlotID, booking.Version, trucker)
	if err != nil {
		t.Fatalf("reserve slot: %v", err)
	}
	if _, err := env.store.CreatePaymentIntent(env.ctx, booking.BookingID, "pay-"+booking.RequestID, "tx-"+booking.RequestID, reserved.Version); err != nil {
		t.Fatalf("create payment intent: %v", err)
	}
	paid, err := env.store.ConfirmPayment(env.ctx, booking.BookingID, "rcpt-"+booking.RequestID, reserved.Version, trucker)
	if err != nil {
		t.Fatalf("confirm payment: %v", err)
	}
	return paid
}

func TestCustomsGateBlocksUnvalidatedBookingAndClearsOnMatch(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, slot := env.makeTerminalAndSlot(t, 2)
	gate := Principal{ID: "gate-officer-1", Role: "gate-officer"}
	validatorPrincipal := Principal{ID: "booking-workflow", Role: "booking-workflow"}

	booking := env.makeDeclaredBooking(t, terminalID, "req-customs-001")
	env.payBooking(t, booking, slot)

	// Even PAID with a receipt, a declaration-carrying booking is denied at
	// the gate until customs validation matches.
	if _, _, err := env.store.RecordGateScan(env.ctx, booking.BookingID, "GATE-A", "officer-1", time.Now().UTC(), gate); !errors.Is(err, ErrGateDenied) {
		t.Fatalf("gate scan before customs validation: got %v, want ErrGateDenied", err)
	}

	pending, err := env.store.BeginCustomsValidation(env.ctx, booking.BookingID, validatorPrincipal)
	if err != nil {
		t.Fatalf("begin customs validation: %v", err)
	}
	if pending.Status != StatusValidationPending {
		t.Fatalf("status = %s, want VALIDATION_PENDING", pending.Status)
	}
	// The pending booking still occupies slot capacity (no overbooking while
	// the customs check runs).
	other := env.makeBooking(t, terminalID, "req-customs-002", ChannelWeb)
	reservedOther, err := env.store.ReserveSlot(env.ctx, other.BookingID, slot.SlotID, other.Version, Principal{ID: "test-trucker", Role: "trucker"})
	if err != nil {
		t.Fatalf("second reservation must succeed within capacity: %v", err)
	}
	_ = reservedOther
	third := env.makeBooking(t, terminalID, "req-customs-003", ChannelWeb)
	if _, err := env.store.ReserveSlot(env.ctx, third.BookingID, slot.SlotID, third.Version, Principal{ID: "test-trucker", Role: "trucker"}); !errors.Is(err, ErrSlotUnavailable) {
		t.Fatalf("third reservation during validation: got %v, want ErrSlotUnavailable", err)
	}
	// VALIDATION_PENDING is never gate-eligible.
	if _, _, err := env.store.RecordGateScan(env.ctx, booking.BookingID, "GATE-A", "officer-1", time.Now().UTC(), gate); !errors.Is(err, ErrGateDenied) {
		t.Fatalf("gate scan during validation: got %v, want ErrGateDenied", err)
	}

	resolved, err := env.store.RecordCustomsValidation(env.ctx, booking.BookingID, customs.Evaluation{
		Decision:        customs.DecisionMatch,
		DeclarationRef:  "NCS-2026-ABC123",
		CustomsStatus:   "RELEASED",
		CustomsWeightKg: 10200,
		BookingWeightKg: 10000,
		ConsigneeID:     "consignee-dangote-01",
		OperatorID:      "operator-apapa-01",
	}, "nigeria-customs-declaration-api", validatorPrincipal)
	if err != nil {
		t.Fatalf("record customs match: %v", err)
	}
	if resolved.Status != StatusPaid {
		t.Fatalf("resolved status = %s, want PAID after MATCH", resolved.Status)
	}
	scan, approved, err := env.store.RecordGateScan(env.ctx, booking.BookingID, "GATE-A", "officer-1", time.Now().UTC(), gate)
	if err != nil {
		t.Fatalf("gate scan after customs match: %v", err)
	}
	if scan.Decision != "APPROVED" || approved.Status != StatusGateApproved {
		t.Fatalf("scan=%s status=%s, want APPROVED/GATE_APPROVED", scan.Decision, approved.Status)
	}

	// The customs_validated event must be on the platform outbox.
	var eventCount int
	if err := env.store.Pool().QueryRow(env.ctx,
		`SELECT count(*) FROM platform_outbox WHERE topic='ports.booking.v1' AND event_type='booking.customs_validated'`).Scan(&eventCount); err != nil {
		t.Fatalf("count customs events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("customs_validated events = %d, want 1", eventCount)
	}
}

func TestCustomsMismatchRejectsBookingWithReasonCode(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, slot := env.makeTerminalAndSlot(t, 1)
	validatorPrincipal := Principal{ID: "booking-workflow", Role: "booking-workflow"}

	booking := env.makeDeclaredBooking(t, terminalID, "req-customs-004")
	env.payBooking(t, booking, slot)
	if _, err := env.store.BeginCustomsValidation(env.ctx, booking.BookingID, validatorPrincipal); err != nil {
		t.Fatalf("begin customs validation: %v", err)
	}
	resolved, err := env.store.RecordCustomsValidation(env.ctx, booking.BookingID, customs.Evaluation{
		Decision:        customs.DecisionMismatch,
		ReasonCode:      customs.ReasonConsigneeMismatch,
		DeclarationRef:  "NCS-2026-ABC123",
		CustomsStatus:   "VALID",
		CustomsWeightKg: 10000,
		BookingWeightKg: 10000,
		ConsigneeID:     "consignee-other",
		OperatorID:      "operator-apapa-01",
	}, "nigeria-customs-declaration-api", validatorPrincipal)
	if err != nil {
		t.Fatalf("record customs mismatch: %v", err)
	}
	if resolved.Status != StatusRejected {
		t.Fatalf("resolved status = %s, want REJECTED", resolved.Status)
	}
	if reasonCode := env.customsValidationReason(t, booking.BookingID); reasonCode != customs.ReasonConsigneeMismatch {
		t.Fatalf("reason_code = %s, want %s", reasonCode, customs.ReasonConsigneeMismatch)
	}
	// A rejected booking can only be cancelled afterwards.
	if _, err := env.store.RecordCustomsValidation(env.ctx, booking.BookingID, customs.Evaluation{
		Decision: customs.DecisionMatch, DeclarationRef: "NCS-2026-ABC123",
	}, "nigeria-customs-declaration-api", validatorPrincipal); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("re-validating a rejected booking: got %v, want ErrInvalidTransition", err)
	}
}

func TestCustomsValidationActivityFailsClosedWhenValidatorDown(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, slot := env.makeTerminalAndSlot(t, 1)
	claims, err := tenantctx.Tenant(env.ctx)
	if err != nil {
		t.Fatalf("load test tenant claims: %v", err)
	}

	booking := env.makeDeclaredBooking(t, terminalID, "req-customs-005")
	env.payBooking(t, booking, slot)
	input := WorkflowInput{BookingID: booking.BookingID, TenantID: claims.TenantID, PrincipalID: "test-trucker"}

	activities := &Activities{Store: env.store, Customs: stubCustomsValidator{err: errors.New("customs API unreachable")}, CustomsWeightToleranceBPS: 500}
	if _, err := activities.CustomsValidation(context.Background(), input); err == nil {
		t.Fatal("validator outage must surface an activity error (retryable)")
	}
	// Fail closed: the booking waits in VALIDATION_PENDING and is not approved.
	current, err := env.store.Get(env.ctx, booking.BookingID)
	if err != nil {
		t.Fatalf("reload booking: %v", err)
	}
	if current.Status != StatusValidationPending {
		t.Fatalf("status after validator outage = %s, want VALIDATION_PENDING", current.Status)
	}

	// Activity retry with a mismatched declaration rejects the booking.
	activities.Customs = stubCustomsValidator{declaration: customs.Declaration{
		DeclarationRef: "NCS-2026-ABC123",
		Status:         "VALID",
		WeightKg:       20000, // far outside tolerance
		ConsigneeID:    "consignee-dangote-01",
		OperatorID:     "operator-apapa-01",
	}}
	decision, err := activities.CustomsValidation(context.Background(), input)
	if err != nil {
		t.Fatalf("customs activity: %v", err)
	}
	if decision != CustomsRejected {
		t.Fatalf("activity decision = %s, want REJECTED", decision)
	}
	current, err = env.store.Get(env.ctx, booking.BookingID)
	if err != nil {
		t.Fatalf("reload booking: %v", err)
	}
	if current.Status != StatusRejected {
		t.Fatalf("status after mismatch = %s, want REJECTED", current.Status)
	}
}

func TestCustomsValidationActivityMatchesAndRestoresGateEligibility(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	terminalID, slot := env.makeTerminalAndSlot(t, 1)
	claims, err := tenantctx.Tenant(env.ctx)
	if err != nil {
		t.Fatalf("load test tenant claims: %v", err)
	}

	booking := env.makeDeclaredBooking(t, terminalID, "req-customs-006")
	env.payBooking(t, booking, slot)
	input := WorkflowInput{BookingID: booking.BookingID, TenantID: claims.TenantID, PrincipalID: "test-trucker"}

	activities := &Activities{Store: env.store, CustomsWeightToleranceBPS: 500, Customs: stubCustomsValidator{declaration: customs.Declaration{
		DeclarationRef: "NCS-2026-ABC123",
		Status:         "RELEASED",
		WeightKg:       10500, // exactly at the 5% boundary
		ConsigneeID:    "consignee-dangote-01",
		OperatorID:     "operator-apapa-01",
	}}}
	decision, err := activities.CustomsValidation(context.Background(), input)
	if err != nil {
		t.Fatalf("customs activity: %v", err)
	}
	if decision != CustomsMatched {
		t.Fatalf("activity decision = %s, want MATCH", decision)
	}
	current, err := env.store.Get(env.ctx, booking.BookingID)
	if err != nil {
		t.Fatalf("reload booking: %v", err)
	}
	if current.Status != StatusPaid {
		t.Fatalf("status after match = %s, want PAID", current.Status)
	}

	// Bookings without a declaration never touch the validator.
	plain := env.makeBooking(t, terminalID, "req-customs-007", ChannelWeb)
	plainInput := WorkflowInput{BookingID: plain.BookingID, TenantID: claims.TenantID, PrincipalID: "test-trucker"}
	decision, err = activities.CustomsValidation(context.Background(), plainInput)
	if err != nil {
		t.Fatalf("customs activity for undeclared booking: %v", err)
	}
	if decision != CustomsNotRequired {
		t.Fatalf("decision = %s, want NOT_REQUIRED", decision)
	}
}
