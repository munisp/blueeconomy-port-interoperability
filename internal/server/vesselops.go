package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/cruise"
	"github.com/munisp/blueeconomy-port-interoperability/internal/manifests"
	"github.com/munisp/blueeconomy-port-interoperability/internal/offshore"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tariff"
)

// Handlers for the W-FEAT-6 vessel-operations surface: offshore terminal
// calls (SBM/SPM), API/BRI passenger manifests, cruise call workflows and
// the versioned tariff schedules both revenue domains assess against.
// Role gates mirror the platform PBAC model: NPA officers run vessel
// workflows and assessments, port operator admins register tariff data and
// tender allocations, and only the manifest-authority service identity may
// ingest API/BRI artifacts.

// RoleManifestAuthority is the service identity allowed to submit signed
// API/BRI passenger manifest artifacts.
const RoleManifestAuthority = "manifest-authority-agent"

func writeVesselOpsError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, offshore.ErrNotFound), errors.Is(err, cruise.ErrNotFound),
		errors.Is(err, manifests.ErrNotFound), errors.Is(err, tariff.ErrNotFound):
		writeError(response, http.StatusNotFound, err.Error())
	case errors.Is(err, manifests.ErrSignatureVerification):
		writeError(response, http.StatusUnauthorized, err.Error())
	case errors.Is(err, manifests.ErrMalformedEnvelope):
		writeError(response, http.StatusBadRequest, err.Error())
	case errors.Is(err, offshore.ErrIdempotencyConflict), errors.Is(err, cruise.ErrIdempotencyConflict),
		errors.Is(err, manifests.ErrManifestConflict), errors.Is(err, tariff.ErrAssessmentReplay),
		errors.Is(err, offshore.ErrOptimisticConflict), errors.Is(err, cruise.ErrOptimisticConflict),
		errors.Is(err, offshore.ErrInvalidTransition), errors.Is(err, cruise.ErrInvalidTransition):
		writeError(response, http.StatusConflict, err.Error())
	case errors.Is(err, offshore.ErrEventRejected), errors.Is(err, cruise.ErrExcursionInvalid),
		errors.Is(err, cruise.ErrAllocationInvalid), errors.Is(err, tariff.ErrNotEffective),
		errors.Is(err, tariff.ErrBandGap), errors.Is(err, tariff.ErrScheduleInvalid),
		errors.Is(err, tariff.ErrRuleInvalid), errors.Is(err, tariff.ErrOverflow):
		writeError(response, http.StatusUnprocessableEntity, err.Error())
	default:
		writeError(response, http.StatusInternalServerError, "internal vessel-operations failure")
	}
}

func idempotencyHeader(response http.ResponseWriter, request *http.Request) (string, bool) {
	key := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	if key == "" {
		writeError(response, http.StatusBadRequest, "missing Idempotency-Key")
		return "", false
	}
	return key, true
}

func decodeJSON(response http.ResponseWriter, request *http.Request, target any) bool {
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(response, http.StatusBadRequest, "invalid JSON request")
		return false
	}
	return true
}

// --- Tariff schedules -------------------------------------------------------

func (server *Server) registerTariffSchedule(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RolePortOperatorAdmin)
	if !ok {
		return
	}
	var input tariff.Schedule
	if !decodeJSON(response, request, &input) {
		return
	}
	// The registrar is the verified caller, never a client-supplied field.
	input.RegisteredBy = claims.Subject
	if err := input.Validate(); err != nil {
		writeError(response, http.StatusBadRequest, err.Error())
		return
	}
	if err := server.tariffs.RegisterSchedule(request.Context(), input); err != nil {
		writeVesselOpsError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, input)
}

// --- Offshore terminal calls -------------------------------------------------

func (server *Server) createOffshoreCall(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleNPAOfficer)
	if !ok {
		return
	}
	idempotencyKey, ok := idempotencyHeader(response, request)
	if !ok {
		return
	}
	var input offshore.CreateRequest
	if !decodeJSON(response, request, &input) {
		return
	}
	if err := input.Validate(); err != nil {
		writeError(response, http.StatusBadRequest, err.Error())
		return
	}
	call, err := server.offshore.Create(request.Context(), idempotencyKey, input, offshorePrincipal(claims.Subject))
	if err != nil {
		writeVesselOpsError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, call)
}

func offshorePrincipal(subject string) offshore.Principal {
	return offshore.Principal{ID: subject, Role: RoleNPAOfficer}
}

func cruisePrincipal(subject string) cruise.Principal {
	return cruise.Principal{ID: subject, Role: RoleNPAOfficer}
}

// offshoreCallRead serves GET /v1/offshore-calls/{id} and
// GET /v1/offshore-calls/{id}/events.
func (server *Server) offshoreCallRead(response http.ResponseWriter, request *http.Request) {
	rest := strings.TrimPrefix(request.URL.Path, "/v1/offshore-calls/")
	callID, action, found := strings.Cut(rest, "/")
	if callID == "" || (found && action != "events") {
		writeError(response, http.StatusNotFound, "offshore terminal call not found")
		return
	}
	if found {
		events, err := server.offshore.ListEvents(request.Context(), callID)
		if err != nil {
			writeVesselOpsError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, events)
		return
	}
	call, err := server.offshore.Get(request.Context(), callID)
	if err != nil {
		writeVesselOpsError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, call)
}

// offshoreCallOperation serves POST /v1/offshore-calls/{id}/transitions,
// /events and /assessments.
func (server *Server) offshoreCallOperation(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleNPAOfficer)
	if !ok {
		return
	}
	rest := strings.TrimPrefix(request.URL.Path, "/v1/offshore-calls/")
	callID, action, found := strings.Cut(rest, "/")
	if callID == "" || !found {
		writeError(response, http.StatusNotFound, "offshore terminal call not found")
		return
	}
	switch action {
	case "transitions":
		var input struct {
			ExpectedVersion int64           `json:"expected_version"`
			Next            offshore.Status `json:"next"`
			MooringMaster   string          `json:"mooring_master"`
		}
		if !decodeJSON(response, request, &input) {
			return
		}
		call, err := server.offshore.Transition(request.Context(), callID, input.ExpectedVersion, input.Next, input.MooringMaster, offshorePrincipal(claims.Subject))
		if err != nil {
			writeVesselOpsError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, call)
	case "events":
		var input offshore.EventRequest
		if !decodeJSON(response, request, &input) {
			return
		}
		event, err := server.offshore.RecordEvent(request.Context(), callID, input, offshorePrincipal(claims.Subject))
		if err != nil {
			writeVesselOpsError(response, err)
			return
		}
		writeJSON(response, http.StatusCreated, event)
	case "assessments":
		var input struct {
			IdempotencyKey string    `json:"idempotency_key"`
			AsOf           time.Time `json:"as_of"`
		}
		if !decodeJSON(response, request, &input) {
			return
		}
		assessment, err := server.offshore.Assess(request.Context(), callID, input.IdempotencyKey, input.AsOf, offshorePrincipal(claims.Subject))
		if err != nil {
			writeVesselOpsError(response, err)
			return
		}
		writeJSON(response, http.StatusCreated, assessment)
	default:
		writeError(response, http.StatusNotFound, "unknown offshore call operation")
	}
}

// --- API/BRI passenger manifests ---------------------------------------------

// ingestManifest accepts one signed envelope v1.0 artifact (raw body) from
// the manifest-authority service identity.
func (server *Server) ingestManifest(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleManifestAuthority)
	if !ok {
		return
	}
	artifact, err := io.ReadAll(request.Body)
	if err != nil || len(artifact) == 0 {
		writeError(response, http.StatusBadRequest, "manifest artifact body is required")
		return
	}
	manifest, err := server.manifests.Ingest(request.Context(), artifact, manifests.Principal{ID: claims.Subject, Role: RoleManifestAuthority})
	if err != nil {
		writeVesselOpsError(response, err)
		return
	}
	status := http.StatusCreated
	if manifest.Status == manifests.StatusAcceptedWithRejections || manifest.Status == manifests.StatusRejected {
		status = http.StatusUnprocessableEntity
	}
	writeJSON(response, status, manifest)
}

func (server *Server) manifestRead(response http.ResponseWriter, request *http.Request) {
	manifestID := strings.TrimPrefix(request.URL.Path, "/v1/manifests/")
	if manifestID == "" || strings.Contains(manifestID, "/") {
		writeError(response, http.StatusNotFound, "passenger manifest not found")
		return
	}
	manifest, err := server.manifests.Get(request.Context(), manifestID)
	if err != nil {
		writeVesselOpsError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, manifest)
}

func (server *Server) manifestRejections(response http.ResponseWriter, request *http.Request) {
	limit := 0
	if raw := strings.TrimSpace(request.URL.Query().Get("limit")); raw != "" {
		if parsed, convErr := strconv.Atoi(raw); convErr == nil {
			limit = parsed
		}
	}
	rejections, err := server.manifests.ListRejectionsPage(request.Context(), strings.TrimSpace(request.URL.Query().Get("manifest_id")), limit)
	if err != nil {
		writeVesselOpsError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, rejections)
}

// --- Cruise calls --------------------------------------------------------------

func (server *Server) createCruiseCall(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleNPAOfficer)
	if !ok {
		return
	}
	idempotencyKey, ok := idempotencyHeader(response, request)
	if !ok {
		return
	}
	var input cruise.CreateRequest
	if !decodeJSON(response, request, &input) {
		return
	}
	// The registrar is the verified caller.
	input.CreatedBy = claims.Subject
	if err := input.Validate(); err != nil {
		writeError(response, http.StatusBadRequest, err.Error())
		return
	}
	call, err := server.cruise.Create(request.Context(), idempotencyKey, input, cruisePrincipal(claims.Subject))
	if err != nil {
		writeVesselOpsError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, call)
}

func (server *Server) cruiseCallRead(response http.ResponseWriter, request *http.Request) {
	callID := strings.TrimPrefix(request.URL.Path, "/v1/cruise-calls/")
	if callID == "" || strings.Contains(callID, "/") {
		writeError(response, http.StatusNotFound, "cruise call not found")
		return
	}
	call, err := server.cruise.Get(request.Context(), callID)
	if err != nil {
		writeVesselOpsError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, call)
}

// cruiseCallOperation serves POST /v1/cruise-calls/{id}/transitions,
// /pax-count, /excursions, /tender-allocations and /dues-assessments.
func (server *Server) cruiseCallOperation(response http.ResponseWriter, request *http.Request) {
	rest := strings.TrimPrefix(request.URL.Path, "/v1/cruise-calls/")
	callID, action, found := strings.Cut(rest, "/")
	if callID == "" || !found {
		writeError(response, http.StatusNotFound, "cruise call not found")
		return
	}
	switch action {
	case "transitions", "pax-count", "excursions", "dues-assessments":
		if _, ok := requireRole(response, request, RoleNPAOfficer); !ok {
			return
		}
	case "tender-allocations":
		if _, ok := requireRole(response, request, RolePortOperatorAdmin); !ok {
			return
		}
	default:
		writeError(response, http.StatusNotFound, "unknown cruise call operation")
		return
	}
	claims, _ := claimsOf(response, request)
	switch action {
	case "transitions":
		var input struct {
			ExpectedVersion int64         `json:"expected_version"`
			Next            cruise.Status `json:"next"`
		}
		if !decodeJSON(response, request, &input) {
			return
		}
		call, err := server.cruise.Transition(request.Context(), callID, input.ExpectedVersion, input.Next, cruisePrincipal(claims.Subject))
		if err != nil {
			writeVesselOpsError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, call)
	case "pax-count":
		var input struct {
			ExpectedVersion int64 `json:"expected_version"`
			PaxCount        int   `json:"pax_count"`
		}
		if !decodeJSON(response, request, &input) {
			return
		}
		call, err := server.cruise.UpdatePaxCount(request.Context(), callID, input.ExpectedVersion, input.PaxCount, cruisePrincipal(claims.Subject))
		if err != nil {
			writeVesselOpsError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, call)
	case "excursions":
		var input struct {
			IdempotencyKey string `json:"idempotency_key"`
			Name           string `json:"name"`
			Operator       string `json:"operator"`
			PaxCount       int    `json:"pax_count"`
		}
		if !decodeJSON(response, request, &input) {
			return
		}
		excursion, err := server.cruise.AddExcursion(request.Context(), callID, input.IdempotencyKey, input.Name, input.Operator, input.PaxCount, cruisePrincipal(claims.Subject))
		if err != nil {
			writeVesselOpsError(response, err)
			return
		}
		writeJSON(response, http.StatusCreated, excursion)
	case "tender-allocations":
		var input struct {
			IdempotencyKey string    `json:"idempotency_key"`
			TerminalCode   string    `json:"terminal_code"`
			BerthCode      string    `json:"berth_code"`
			WindowStart    time.Time `json:"window_start"`
			WindowEnd      time.Time `json:"window_end"`
		}
		if !decodeJSON(response, request, &input) {
			return
		}
		allocation, err := server.cruise.AllocateTender(request.Context(), callID, input.IdempotencyKey, input.TerminalCode, input.BerthCode, input.WindowStart, input.WindowEnd,
			cruise.Principal{ID: claims.Subject, Role: RolePortOperatorAdmin})
		if err != nil {
			writeVesselOpsError(response, err)
			return
		}
		writeJSON(response, http.StatusCreated, allocation)
	case "dues-assessments":
		var input struct {
			IdempotencyKey string    `json:"idempotency_key"`
			AsOf           time.Time `json:"as_of"`
		}
		if !decodeJSON(response, request, &input) {
			return
		}
		assessment, err := server.cruise.AssessDues(request.Context(), callID, input.IdempotencyKey, input.AsOf, cruisePrincipal(claims.Subject))
		if err != nil {
			writeVesselOpsError(response, err)
			return
		}
		writeJSON(response, http.StatusCreated, assessment)
	}
}
