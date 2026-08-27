package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/payments"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// pgReplayStore is the PostgreSQL-backed NSW ingress replay store. The table
// is platform-scoped (not tenant RLS) because the replay hash precedes tenant
// trust; reservation is atomic via the primary key.
type pgReplayStore struct {
	pool *pgxpool.Pool
}

func (store *pgReplayStore) Reserve(ctx context.Context, replayHash string, expiresAt time.Time) (bool, error) {
	if _, err := store.pool.Exec(ctx, `DELETE FROM nsw_ingress_replay WHERE expires_at < now()`); err != nil {
		return false, err
	}
	result, err := store.pool.Exec(ctx, `
		INSERT INTO nsw_ingress_replay (replay_hash, expires_at) VALUES ($1, $2)
		ON CONFLICT (replay_hash) DO NOTHING`, replayHash, expiresAt)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
}

func principalOf(request *http.Request, role string) booking.Principal {
	claims, err := tenantctx.Tenant(request.Context())
	if err != nil {
		return booking.Principal{ID: "unknown", Role: role}
	}
	return booking.Principal{ID: claims.Subject, Role: role}
}

func (server *Server) createTerminal(response http.ResponseWriter, request *http.Request) {
	var input struct {
		TerminalID     string `json:"terminal_id"`
		PortCode       string `json:"port_code"`
		Name           string `json:"name"`
		BookingFeeKobo int64  `json:"booking_fee_kobo"`
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid terminal JSON")
		return
	}
	if err := server.bookings.CreateTerminal(request.Context(), input.TerminalID, input.PortCode, input.Name, input.BookingFeeKobo); err != nil {
		writeBookingError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, map[string]string{"terminal_id": input.TerminalID, "status": "registered"})
}

func (server *Server) createSlot(response http.ResponseWriter, request *http.Request) {
	var input struct {
		TerminalID string    `json:"terminal_id"`
		StartsAt   time.Time `json:"starts_at"`
		EndsAt     time.Time `json:"ends_at"`
		Capacity   int       `json:"capacity"`
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid slot JSON")
		return
	}
	slot, err := server.bookings.CreateSlot(request.Context(), input.TerminalID, input.StartsAt, input.EndsAt, input.Capacity)
	if err != nil {
		writeBookingError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, slot)
}

func (server *Server) listSlots(response http.ResponseWriter, request *http.Request) {
	terminalID := request.URL.Query().Get("terminal_id")
	from, err := time.Parse(time.RFC3339, request.URL.Query().Get("from"))
	if err != nil {
		writeError(response, http.StatusBadRequest, "from must be RFC3339")
		return
	}
	to, err := time.Parse(time.RFC3339, request.URL.Query().Get("to"))
	if err != nil {
		writeError(response, http.StatusBadRequest, "to must be RFC3339")
		return
	}
	slots, err := server.bookings.ListSlots(request.Context(), terminalID, from, to)
	if err != nil {
		writeBookingError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"slots": slots})
}

func (server *Server) createBooking(response http.ResponseWriter, request *http.Request) {
	var input booking.CreateRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid booking JSON")
		return
	}
	created, err := server.bookings.Create(request.Context(), input, principalOf(request, "trucker"))
	if err != nil {
		writeBookingError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, created)
}

func (server *Server) bookingRead(response http.ResponseWriter, request *http.Request) {
	path := strings.TrimPrefix(request.URL.Path, "/v1/bookings/")
	parts := strings.Split(path, "/")
	if len(parts) == 2 && parts[0] != "" && parts[1] == "observer" {
		state, err := server.orchestrator.ObserverState(request.Context(), parts[0])
		if err != nil {
			writeBookingError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, state)
		return
	}
	if len(parts) != 1 || parts[0] == "" {
		writeError(response, http.StatusNotFound, "booking not found")
		return
	}
	found, err := server.bookings.Get(request.Context(), parts[0])
	if err != nil {
		writeBookingError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, found)
}

func (server *Server) bookingOperation(response http.ResponseWriter, request *http.Request) {
	path := strings.TrimPrefix(request.URL.Path, "/v1/bookings/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[0] == "" {
		writeError(response, http.StatusNotFound, "booking route not found")
		return
	}
	// Annotate the active span with the booking operation dispatch (no-op when
	// telemetry is disabled).
	trace.SpanFromContext(request.Context()).SetAttributes(
		attribute.String("ecallup.booking_id", parts[0]),
		attribute.String("ecallup.booking.operation", parts[1]),
	)
	switch parts[1] {
	case "reserve":
		server.reserveSlot(response, request, parts[0])
	case "reconcile":
		server.reconcileBooking(response, request, parts[0])
	case "payment-intents":
		server.createPaymentIntent(response, request, parts[0])
	case "payment-confirmations":
		server.confirmPayment(response, request, parts[0])
	case "cancel":
		server.cancelBooking(response, request, parts[0])
	default:
		writeError(response, http.StatusNotFound, "booking operation not found")
	}
}

type slotOperationRequest struct {
	SlotID          string `json:"slot_id"`
	ExpectedVersion int64  `json:"expected_version"`
}

func (server *Server) reserveSlot(response http.ResponseWriter, request *http.Request, bookingID string) {
	var input slotOperationRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.SlotID == "" || input.ExpectedVersion < 1 {
		writeError(response, http.StatusBadRequest, "slot_id and positive expected_version are required")
		return
	}
	reserved, err := server.bookings.ReserveSlot(request.Context(), bookingID, input.SlotID, input.ExpectedVersion, principalOf(request, "trucker"))
	if err != nil {
		writeBookingError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, reserved)
}

// reconcileBooking is the offline-mode reconnect path: a PENDING_SYNC booking
// is reconciled against a live slot, and conflicts surface as
// RECONCILIATION_REQUIRED — never a silent drop.
func (server *Server) reconcileBooking(response http.ResponseWriter, request *http.Request, bookingID string) {
	var input slotOperationRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.SlotID == "" || input.ExpectedVersion < 1 {
		writeError(response, http.StatusBadRequest, "slot_id and positive expected_version are required")
		return
	}
	reconciled, err := server.bookings.Reconcile(request.Context(), bookingID, input.SlotID, input.ExpectedVersion, principalOf(request, "sync-agent"))
	if err != nil {
		writeBookingError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, reconciled)
}

func (server *Server) createPaymentIntent(response http.ResponseWriter, request *http.Request, bookingID string) {
	var input struct {
		RequestID       string `json:"request_id"`
		ExpectedVersion int64  `json:"expected_version"`
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.RequestID == "" || input.ExpectedVersion < 1 {
		writeError(response, http.StatusBadRequest, "request_id and positive expected_version are required")
		return
	}
	found, err := server.bookings.Get(request.Context(), bookingID)
	if err != nil {
		writeBookingError(response, err)
		return
	}
	if found.Version != input.ExpectedVersion {
		writeBookingError(response, booking.ErrOptimisticConflict)
		return
	}
	receipt, err := server.payments.RequestPayment(request.Context(), payments.Intent{
		RequestID:   input.RequestID,
		BookingID:   bookingID,
		AmountKobo:  found.AmountKobo,
		Currency:    found.Currency,
		PayerMSISDN: found.TruckerMSISDN,
	})
	if err != nil {
		writeError(response, http.StatusBadGateway, "payment switch rejected the intent")
		return
	}
	intent, err := server.bookings.CreatePaymentIntent(request.Context(), bookingID, input.RequestID, receipt.TxRef, input.ExpectedVersion)
	if err != nil {
		writeBookingError(response, err)
		return
	}
	gateDeadline := found.ExpiresAt
	if found.SlotID != nil {
		if slot, slotErr := server.bookings.GetSlot(request.Context(), *found.SlotID); slotErr == nil {
			gateDeadline = slot.EndsAt
		}
	}
	if err := server.orchestrator.StartBookingWorkflow(request.Context(), booking.WorkflowInput{
		BookingID:        found.BookingID,
		TenantID:         found.TenantID,
		PrincipalID:      principalOf(request, "trucker").ID,
		AmountKobo:       uint64(found.AmountKobo),
		FgnShareKobo:     uint64(found.AmountKobo * server.fgnShareBPS / 10000),
		ExpectedVersion:  found.Version,
		PaymentDeadline:  found.ExpiresAt,
		GateScanDeadline: gateDeadline,
	}); err != nil {
		writeError(response, http.StatusBadGateway, "booking workflow could not be started")
		return
	}
	writeJSON(response, http.StatusCreated, intent)
}

func (server *Server) confirmPayment(response http.ResponseWriter, request *http.Request, bookingID string) {
	var input struct {
		ReceiptRef      string `json:"receipt_ref"`
		ExpectedVersion int64  `json:"expected_version"`
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.ReceiptRef == "" || input.ExpectedVersion < 1 {
		writeError(response, http.StatusBadRequest, "receipt_ref and positive expected_version are required")
		return
	}
	paid, err := server.bookings.ConfirmPayment(request.Context(), bookingID, input.ReceiptRef, input.ExpectedVersion, principalOf(request, "payment-switch"))
	if err != nil {
		writeBookingError(response, err)
		return
	}
	if err := server.orchestrator.SignalPaymentConfirmed(request.Context(), bookingID, input.ReceiptRef); err != nil {
		writeJSON(response, http.StatusBadGateway, map[string]string{
			"error":      "payment recorded but workflow signal failed; retry this confirmation",
			"booking_id": bookingID,
		})
		return
	}
	writeJSON(response, http.StatusOK, paid)
}

func (server *Server) cancelBooking(response http.ResponseWriter, request *http.Request, bookingID string) {
	var input struct {
		ExpectedVersion int64  `json:"expected_version"`
		Reason          string `json:"reason"`
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.ExpectedVersion < 1 || strings.TrimSpace(input.Reason) == "" {
		writeError(response, http.StatusBadRequest, "expected_version and reason are required")
		return
	}
	cancelled, err := server.bookings.Cancel(request.Context(), bookingID, input.ExpectedVersion, input.Reason, principalOf(request, "trucker"))
	if err != nil {
		writeBookingError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, cancelled)
}

// gateScan is the gate scan controller: the scan is validated against booking
// state, slot window and payment receipt before any GATE_APPROVED transition.
func (server *Server) gateScan(response http.ResponseWriter, request *http.Request) {
	var input struct {
		BookingID string     `json:"booking_id"`
		GateID    string     `json:"gate_id"`
		ScannedBy string     `json:"scanned_by"`
		ScannedAt *time.Time `json:"scanned_at,omitempty"`
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.BookingID == "" || input.GateID == "" || input.ScannedBy == "" {
		writeError(response, http.StatusBadRequest, "booking_id, gate_id and scanned_by are required")
		return
	}
	scannedAt := time.Now().UTC()
	if input.ScannedAt != nil {
		scannedAt = *input.ScannedAt
	}
	scan, updated, err := server.bookings.RecordGateScan(request.Context(), input.BookingID, input.GateID, input.ScannedBy, scannedAt, principalOf(request, "gate-officer"))
	if err != nil {
		writeBookingError(response, err)
		return
	}
	if err := server.orchestrator.SignalGateScan(request.Context(), input.BookingID, scan.ScanID); err != nil {
		writeJSON(response, http.StatusBadGateway, map[string]string{
			"error":   "gate approval recorded but workflow signal failed; retry the scan signal",
			"scan_id": scan.ScanID,
		})
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"scan": scan, "booking": updated})
}
