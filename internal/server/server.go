package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/booking"
	"github.com/munisp/blueeconomy-port-interoperability/internal/declarations"
	"github.com/munisp/blueeconomy-port-interoperability/internal/nswsecurity"
	"github.com/munisp/blueeconomy-port-interoperability/internal/payments"
	"github.com/munisp/blueeconomy-port-interoperability/internal/portcall"
	"github.com/munisp/blueeconomy-port-interoperability/internal/queue"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// Config wires every security and integration dependency. New fails closed
// when any of them is missing.
type Config struct {
	Store        *portcall.Store
	Bookings     *booking.Store
	Queues       *queue.Store
	Declarations *declarations.Store
	// DeclarationScorer is the fail-closed risk-scoring boundary; declaration
	// submission cannot proceed without it.
	DeclarationScorer declarations.Scorer
	// DeclarationHighValueMinor is the high-value shipment threshold (invoice
	// currency minor units) for the risk-lane rules; 0 disables it.
	DeclarationHighValueMinor int64
	Payments                  payments.Gateway
	Orchestrator              booking.Orchestrator
	CallUps                   queue.CallUpOrchestrator
	AuthMode                  string
	TenantGateway             tenantctx.Verifier
	NSWVerifier               *nswsecurity.Verifier
	Pool                      *pgxpool.Pool
	// FGNShareBasisPoints is the FGN levy split out of each booking amount.
	FGNShareBasisPoints int64
	// NSWReplayTTL bounds how long ingress replay hashes are retained.
	NSWReplayTTL time.Duration
}

type Server struct {
	store                     *portcall.Store
	bookings                  *booking.Store
	queues                    *queue.Store
	declarations              *declarations.Store
	declarationScorer         declarations.Scorer
	declarationHighValueMinor int64
	payments                  payments.Gateway
	orchestrator              booking.Orchestrator
	callUps                   queue.CallUpOrchestrator
	fgnShareBPS               int64
}

func New(config Config) (http.Handler, error) {
	if config.Store == nil || config.Bookings == nil || config.Queues == nil || config.Declarations == nil || config.Pool == nil {
		return nil, errors.New("server requires port-call, booking, queue and declaration stores")
	}
	if config.Payments == nil || config.Orchestrator == nil || config.CallUps == nil {
		return nil, errors.New("server requires a payments gateway and workflow orchestrators")
	}
	if config.DeclarationScorer == nil {
		return nil, errors.New("server requires a fail-closed declaration risk scorer")
	}
	if !config.TenantGateway.Ready() {
		return nil, errors.New("tenant gateway verifier is not configured (key >= 32 bytes, issuer, audience)")
	}
	if config.NSWVerifier == nil || config.Pool == nil || config.NSWReplayTTL <= 0 {
		return nil, errors.New("NSW ingress requires verifier, database pool and replay TTL")
	}
	if config.FGNShareBasisPoints <= 0 || config.FGNShareBasisPoints >= 10000 {
		return nil, errors.New("FGN_SHARE_BASIS_POINTS must be between 1 and 9999")
	}
	server := &Server{
		store:                     config.Store,
		bookings:                  config.Bookings,
		queues:                    config.Queues,
		declarations:              config.Declarations,
		declarationScorer:         config.DeclarationScorer,
		declarationHighValueMinor: config.DeclarationHighValueMinor,
		payments:                  config.Payments,
		orchestrator:              config.Orchestrator,
		callUps:                   config.CallUps,
		fgnShareBPS:               config.FGNShareBasisPoints,
	}
	api := http.NewServeMux()
	api.HandleFunc("GET /v1/partner-capabilities", server.partnerCapabilities)
	api.HandleFunc("POST /v1/agency-profiles", server.registerAgencyProfile)
	api.HandleFunc("POST /v1/port-calls", server.create)
	api.HandleFunc("GET /v1/port-calls/", server.get)
	api.HandleFunc("POST /v1/port-calls/", server.transition)
	api.HandleFunc("POST /v1/terminals", server.createTerminal)
	api.HandleFunc("POST /v1/slots", server.createSlot)
	api.HandleFunc("GET /v1/slots", server.listSlots)
	api.HandleFunc("POST /v1/bookings", server.createBooking)
	api.HandleFunc("GET /v1/bookings/", server.bookingRead)
	api.HandleFunc("POST /v1/bookings/", server.bookingOperation)
	api.HandleFunc("POST /v1/gate/scans", server.gateScan)
	api.HandleFunc("POST /v1/queue-requests", server.createQueueRequest)
	api.HandleFunc("GET /v1/queue-requests/", server.queueRead)
	api.HandleFunc("POST /v1/queue-requests/", server.queueOperation)
	api.HandleFunc("GET /v1/terminals/", server.terminalQueue)
	api.HandleFunc("POST /v1/declarations", server.createDeclaration)
	api.HandleFunc("GET /v1/declarations", server.listDeclarations)
	api.HandleFunc("GET /v1/declarations/", server.declarationRead)
	api.HandleFunc("POST /v1/declarations/", server.declarationOperation)

	nswIngress, err := nswsecurity.NewIngress(nswsecurity.IngressConfig{
		SignatureHeader: "X-NSW-Signature",
		Verifier:        config.NSWVerifier,
		ReplayStore:     &pgReplayStore{pool: config.Pool},
		ReplayTTL:       config.NSWReplayTTL,
	}, http.HandlerFunc(server.nswPortCall))
	if err != nil {
		return nil, fmt.Errorf("build NSW ingress: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.health)
	// Tenant middleware (HS256 gateway token) protects all tenant API routes.
	mux.Handle("/v1/", requireAuthentication(config.AuthMode, tenantctx.Middleware(config.TenantGateway, api)))
	// NSW ingress uses asymmetric JWS authority signatures instead of the
	// gateway token; it is mounted last so the more specific pattern wins.
	mux.Handle("POST /v1/nsw/port-calls", requireAuthentication(config.AuthMode, nswIngress))
	return requestLimit(mux), nil
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

// nswPortCall is the downstream handler of the NSW JWS ingress: the authority
// message was signature-verified and replay-checked, its claims become the
// tenant context, and the jti doubles as the port-call idempotency key.
func (server *Server) nswPortCall(response http.ResponseWriter, request *http.Request) {
	nswClaims, err := nswsecurity.ClaimsFrom(request.Context())
	if err != nil {
		writeError(response, http.StatusUnauthorized, "verified NSW authority claims are required")
		return
	}
	ctx, err := tenantctx.WithClaims(request.Context(), tenantctx.Claims{
		Issuer:   nswClaims.Issuer,
		Audience: nswClaims.Audience,
		TenantID: nswClaims.TenantID,
		Subject:  nswClaims.Subject,
		Expires:  nswClaims.Expires,
	})
	if err != nil {
		writeError(response, http.StatusUnauthorized, "NSW tenant claims are not valid for storage")
		return
	}
	var input portcall.CreateRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid NSW port-call JSON")
		return
	}
	call, err := server.store.Create(ctx, nswClaims.JTI, input)
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

func writeBookingError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, booking.ErrNotFound):
		writeError(response, http.StatusNotFound, err.Error())
	case errors.Is(err, booking.ErrIdempotencyConflict), errors.Is(err, booking.ErrOptimisticConflict),
		errors.Is(err, booking.ErrInvalidTransition), errors.Is(err, booking.ErrSlotUnavailable):
		writeError(response, http.StatusConflict, err.Error())
	case errors.Is(err, booking.ErrSlotWindow), errors.Is(err, booking.ErrPaymentInvalid):
		writeError(response, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, booking.ErrGateDenied):
		writeError(response, http.StatusForbidden, err.Error())
	default:
		writeError(response, http.StatusInternalServerError, "internal booking failure")
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
