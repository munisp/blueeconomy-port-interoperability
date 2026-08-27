package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/queue"
)

// createQueueRequest registers a truck into a terminal call-up queue. The
// Idempotency-Key header makes replays atomic; without a booking_id a PENDING
// booking priced at the terminal fee is created atomically.
func (server *Server) createQueueRequest(response http.ResponseWriter, request *http.Request) {
	idempotencyKey := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		writeError(response, http.StatusBadRequest, "missing Idempotency-Key")
		return
	}
	var input struct {
		TruckPlate    string              `json:"truck_plate"`
		TruckerMSISDN string              `json:"trucker_msisdn"`
		TerminalID    string              `json:"terminal_id"`
		PriorityClass queue.PriorityClass `json:"priority_class"`
		BookingID     string              `json:"booking_id,omitempty"`
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid queue request JSON")
		return
	}
	created, err := server.queues.Create(request.Context(), queue.CreateRequest{
		IdempotencyKey: idempotencyKey,
		TruckPlate:     input.TruckPlate,
		TruckerMSISDN:  input.TruckerMSISDN,
		TerminalID:     input.TerminalID,
		PriorityClass:  input.PriorityClass,
		BookingID:      input.BookingID,
	}, booking.ChannelWeb, principalOf(request, "trucker"))
	if err != nil {
		writeQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, created)
}

func (server *Server) queueRead(response http.ResponseWriter, request *http.Request) {
	path := strings.TrimPrefix(request.URL.Path, "/v1/queue-requests/")
	parts := strings.Split(path, "/")
	if len(parts) == 2 && parts[0] != "" && parts[1] == "observer" {
		state, err := server.callUps.CallUpObserverState(request.Context(), parts[0])
		if err != nil {
			writeQueueError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, state)
		return
	}
	if len(parts) != 1 || parts[0] == "" {
		writeError(response, http.StatusNotFound, "queue request not found")
		return
	}
	found, err := server.queues.Get(request.Context(), parts[0])
	if err != nil {
		writeQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, found)
}

func (server *Server) queueOperation(response http.ResponseWriter, request *http.Request) {
	path := strings.TrimPrefix(request.URL.Path, "/v1/queue-requests/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[0] == "" {
		writeError(response, http.StatusNotFound, "queue route not found")
		return
	}
	switch parts[1] {
	case "arrive":
		server.arriveQueueRequest(response, request, parts[0])
	case "depart":
		server.departQueueRequest(response, request, parts[0])
	case "cancel":
		server.cancelQueueRequest(response, request, parts[0])
	default:
		writeError(response, http.StatusNotFound, "queue operation not found")
	}
}

// startPromotedCallUps (idempotently) starts the grace-window workflow for
// every request promoted by a capacity-release chain.
func (server *Server) startPromotedCallUps(request *http.Request, promoted *queue.Request) {
	if promoted == nil || promoted.GraceDeadline == nil {
		return
	}
	_ = server.callUps.StartCallUpWorkflow(request.Context(), queue.CallUpWorkflowInput{
		QueueRequestID: promoted.QueueRequestID,
		TenantID:       promoted.TenantID,
		PrincipalID:    principalOf(request, "callup-engine").ID,
		TerminalID:     promoted.TerminalID,
		GraceDeadline:  *promoted.GraceDeadline,
	})
}

// arriveQueueRequest is the gate arrival path: the gate officer confirms a
// called-up truck, capacity frees and the next in queue is promoted. Arrival
// after the grace deadline fails closed as a forfeiture.
func (server *Server) arriveQueueRequest(response http.ResponseWriter, request *http.Request, queueRequestID string) {
	var input struct {
		GateID          string `json:"gate_id"`
		ExpectedVersion int64  `json:"expected_version"`
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.GateID == "" || input.ExpectedVersion < 1 {
		writeError(response, http.StatusBadRequest, "gate_id and positive expected_version are required")
		return
	}
	arrived, promoted, err := server.queues.Arrive(request.Context(), queueRequestID, input.GateID, input.ExpectedVersion, principalOf(request, "gate-officer"))
	if err != nil {
		writeQueueError(response, err)
		return
	}
	server.startPromotedCallUps(request, promoted)
	// The workflow may not exist yet when promotion happened through the
	// in-transaction capacity hook; starting is idempotent before signalling.
	if arrived.GraceDeadline != nil {
		if err := server.callUps.StartCallUpWorkflow(request.Context(), queue.CallUpWorkflowInput{
			QueueRequestID: arrived.QueueRequestID,
			TenantID:       arrived.TenantID,
			PrincipalID:    principalOf(request, "gate-officer").ID,
			TerminalID:     arrived.TerminalID,
			GraceDeadline:  *arrived.GraceDeadline,
		}); err != nil {
			writeError(response, http.StatusBadGateway, "arrival recorded but call-up workflow could not be ensured")
			return
		}
	}
	if err := server.callUps.SignalArrivalConfirmed(request.Context(), queueRequestID, input.GateID); err != nil {
		writeJSON(response, http.StatusBadGateway, map[string]string{
			"error":            "arrival recorded but workflow signal failed; retry this arrival",
			"queue_request_id": queueRequestID,
		})
		return
	}
	writeJSON(response, http.StatusOK, arrived)
}

// departQueueRequest records the trucker acknowledging the call-up
// (CALLED_UP -> EN_ROUTE).
func (server *Server) departQueueRequest(response http.ResponseWriter, request *http.Request, queueRequestID string) {
	var input struct {
		ExpectedVersion int64 `json:"expected_version"`
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.ExpectedVersion < 1 {
		writeError(response, http.StatusBadRequest, "positive expected_version is required")
		return
	}
	departed, err := server.queues.Depart(request.Context(), queueRequestID, input.ExpectedVersion, principalOf(request, "trucker"))
	if err != nil {
		writeQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, departed)
}

func (server *Server) cancelQueueRequest(response http.ResponseWriter, request *http.Request, queueRequestID string) {
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
	cancelled, promoted, err := server.queues.Cancel(request.Context(), queueRequestID, input.ExpectedVersion, input.Reason, principalOf(request, "trucker"))
	if err != nil {
		writeQueueError(response, err)
		return
	}
	server.startPromotedCallUps(request, promoted)
	writeJSON(response, http.StatusOK, cancelled)
}

// terminalQueue is the operator (npa-officer) live queue view for a terminal.
func (server *Server) terminalQueue(response http.ResponseWriter, request *http.Request) {
	path := strings.TrimPrefix(request.URL.Path, "/v1/terminals/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] != "queue" {
		writeError(response, http.StatusNotFound, "terminal route not found")
		return
	}
	entries, err := server.queues.ListTerminal(request.Context(), parts[0])
	if err != nil {
		writeQueueError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"terminal_id": parts[0], "queue": entries})
}

func writeQueueError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrNotFound):
		writeError(response, http.StatusNotFound, err.Error())
	case errors.Is(err, queue.ErrIdempotencyConflict), errors.Is(err, queue.ErrOptimisticConflict),
		errors.Is(err, queue.ErrInvalidTransition), errors.Is(err, queue.ErrCallUpCapacity):
		writeError(response, http.StatusConflict, err.Error())
	case errors.Is(err, queue.ErrGraceWindow):
		writeError(response, http.StatusUnprocessableEntity, err.Error())
	default:
		writeError(response, http.StatusInternalServerError, "internal queue failure")
	}
}
