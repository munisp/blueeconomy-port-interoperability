package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/munisp/blueeconomy-port-interoperability/internal/portcall"
)

type Server struct {
	store *portcall.Store
}

func New(store *portcall.Store, authMode string) http.Handler {
	server := &Server{store: store}
	api := http.NewServeMux()
	api.HandleFunc("GET /v1/partner-capabilities", server.partnerCapabilities)
	api.HandleFunc("POST /v1/agency-profiles", server.registerAgencyProfile)
	api.HandleFunc("POST /v1/port-calls", server.create)
	api.HandleFunc("GET /v1/port-calls/", server.get)
	api.HandleFunc("POST /v1/port-calls/", server.transition)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.health)
	mux.Handle("/v1/", requireAuthentication(authMode, api))
	return requestLimit(mux)
}

func requestLimit(next http.Handler) http.Handler {
	return http.MaxBytesHandler(next, 1<<20)
}

func (server *Server) health(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func (server *Server) partnerCapabilities(response http.ResponseWriter, request *http.Request) {
	capabilities, err := server.store.PartnerCapabilities(request.Context())
	if err != nil {
		writePortCallError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, capabilities)
}

func (server *Server) registerAgencyProfile(response http.ResponseWriter, request *http.Request) {
	var input portcall.AgencyProfileRegistration
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid agency profile JSON")
		return
	}
	if err := server.store.RegisterAgencyProfile(request.Context(), input); err != nil {
		writePortCallError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, input)
}

func (server *Server) create(response http.ResponseWriter, request *http.Request) {
	idempotencyKey := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		writeError(response, http.StatusBadRequest, "missing Idempotency-Key")
		return
	}
	var input portcall.CreateRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid JSON request")
		return
	}
	call, err := server.store.Create(request.Context(), idempotencyKey, input)
	if err != nil {
		writePortCallError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, call)
}

func (server *Server) get(response http.ResponseWriter, request *http.Request) {
	callID := strings.TrimPrefix(request.URL.Path, "/v1/port-calls/")
	if callID == "" || strings.Contains(callID, "/") {
		writeError(response, http.StatusNotFound, "port call not found")
		return
	}
	call, err := server.store.Get(request.Context(), callID)
	if err != nil {
		writePortCallError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, call)
}

type transitionRequest struct {
	ExpectedVersion int64 `json:"expected_version"`
}

func (server *Server) transition(response http.ResponseWriter, request *http.Request) {
	path := strings.TrimPrefix(request.URL.Path, "/v1/port-calls/")
	parts := strings.Split(path, "/")
	if len(parts) == 3 && parts[0] != "" && parts[1] == "documents" && parts[2] == "review" {
		server.reviewDocument(response, request, parts[0])
		return
	}
	if len(parts) == 3 && parts[0] != "" && parts[1] == "documents" && parts[2] == "supersede" {
		server.supersedeDocument(response, request, parts[0])
		return
	}
	if len(parts) == 3 && parts[0] != "" && parts[1] == "clearance" && parts[2] == "amend" {
		server.amendClearance(response, request, parts[0])
		return
	}

	if len(parts) != 2 || parts[0] == "" {
		writeError(response, http.StatusNotFound, "port call route not found")
		return
	}
	if parts[1] == "documents" {
		server.declareDocument(response, request, parts[0])
		return
	}
	if parts[1] == "clearance" {
		server.decideClearance(response, request, parts[0])
		return
	}
	nextByOperation := map[string]portcall.Status{
		"submit": portcall.StatusSubmitted,
		"accept": portcall.StatusAccepted,
		"reject": portcall.StatusRejected,
	}
	next, ok := nextByOperation[parts[1]]
	if !ok {
		writeError(response, http.StatusNotFound, "port call operation not found")
		return
	}
	var input transitionRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.ExpectedVersion < 1 {
		writeError(response, http.StatusBadRequest, "expected_version must be a positive integer")
		return
	}
	call, err := server.store.Transition(request.Context(), parts[0], input.ExpectedVersion, next)
	if err != nil {
		writePortCallError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, call)
}

func (server *Server) declareDocument(response http.ResponseWriter, request *http.Request, callID string) {
	var input portcall.DocumentDeclarationRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid document declaration JSON")
		return
	}
	document, err := server.store.DeclareDocument(request.Context(), callID, input)
	if err != nil {
		writePortCallError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, document)
}

type clearanceRequest struct {
	ExpectedVersion int64                      `json:"expected_version"`
	Decision        portcall.ClearanceDecision `json:"decision"`
	Reason          string                     `json:"reason"`
	DecidedBy       string                     `json:"decided_by"`
}

func (server *Server) reviewDocument(response http.ResponseWriter, request *http.Request, callID string) {
	path := strings.TrimPrefix(request.URL.Path, "/v1/port-calls/")
	parts := strings.Split(path, "/")
	if len(parts) != 3 || parts[1] != "documents" {
		writeError(response, http.StatusNotFound, "document review route not found")
		return
	}
	var input struct {
		DocumentID string `json:"document_id"`
		portcall.DocumentReviewRequest
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.DocumentID == "" || input.ExpectedVersion < 1 {
		writeError(response, http.StatusBadRequest, "document_id and positive expected_version are required")
		return
	}
	document, err := server.store.ReviewDocument(request.Context(), callID, input.DocumentID, input.DocumentReviewRequest)
	if err != nil {
		writePortCallError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, document)
}

func (server *Server) supersedeDocument(response http.ResponseWriter, request *http.Request, callID string) {
	var input portcall.DocumentSupersessionRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid document supersession JSON")
		return
	}
	if err := server.store.SupersedeDocument(request.Context(), callID, input); err != nil {
		writePortCallError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, map[string]string{"call_id": callID, "status": "superseded"})
}

func (server *Server) amendClearance(response http.ResponseWriter, request *http.Request, callID string) {
	var input portcall.ClearanceAmendmentRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid clearance amendment JSON")
		return
	}
	clearance, err := server.store.AmendClearance(request.Context(), callID, input)
	if err != nil {
		writePortCallError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, clearance)
}

func (server *Server) decideClearance(response http.ResponseWriter, request *http.Request, callID string) {
	var input clearanceRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.ExpectedVersion < 1 {
		writeError(response, http.StatusBadRequest, "expected_version must be a positive integer")
		return
	}
	clearance, err := server.store.DecideClearance(request.Context(), callID, input.ExpectedVersion, input.Decision, input.Reason, input.DecidedBy)
	if err != nil {
		writePortCallError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, clearance)
}

func writePortCallError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, portcall.ErrNotFound):
		writeError(response, http.StatusNotFound, err.Error())
	case errors.Is(err, portcall.ErrIdempotencyConflict):
		writeError(response, http.StatusConflict, err.Error())
	case errors.Is(err, portcall.ErrOptimisticConflict), errors.Is(err, portcall.ErrInvalidTransition), errors.Is(err, portcall.ErrDocumentConflict), errors.Is(err, portcall.ErrClearanceConflict):
		writeError(response, http.StatusConflict, err.Error())
	case errors.Is(err, portcall.ErrClearanceInvalid):
		writeError(response, http.StatusUnprocessableEntity, err.Error())
	default:
		writeError(response, http.StatusInternalServerError, "internal port-call failure")
	}
}

func writeError(response http.ResponseWriter, status int, message string) {
	writeJSON(response, status, map[string]string{"error": message})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
