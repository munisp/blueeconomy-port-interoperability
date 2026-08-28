package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/munisp/blueeconomy-port-interoperability/internal/declarations"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// declarationPrincipal derives the provenance principal for declaration
// mutations from the verified tenant claims.
func declarationPrincipal(request *http.Request, role string) declarations.Principal {
	claims, err := tenantctx.Tenant(request.Context())
	if err != nil {
		return declarations.Principal{ID: "unknown", Role: role}
	}
	return declarations.Principal{ID: claims.Subject, Role: role}
}

// createDeclaration handles POST /v1/declarations: draft create, idempotent
// on the caller's request_id.
func (server *Server) createDeclaration(response http.ResponseWriter, request *http.Request) {
	var input declarations.CreateRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid declaration JSON")
		return
	}
	created, err := server.declarations.Create(request.Context(), input, declarationPrincipal(request, "trader"))
	if err != nil {
		writeDeclarationError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, created)
}

// listDeclarations handles GET /v1/declarations. The list is role-scoped: a
// caller sees its own declarations (trader_id = verified subject); the
// optional trader_id filter may only name the caller. An optional status
// filter and bounded pagination are supported.
func (server *Server) listDeclarations(response http.ResponseWriter, request *http.Request) {
	claims, err := tenantctx.Tenant(request.Context())
	if err != nil {
		writeError(response, http.StatusUnauthorized, "verified tenant claims are required")
		return
	}
	query := request.URL.Query()
	traderID := claims.Subject
	if requested := strings.TrimSpace(query.Get("trader_id")); requested != "" {
		if requested != claims.Subject {
			writeError(response, http.StatusForbidden, "declaration list is scoped to the caller")
			return
		}
	}
	status := declarations.Status(strings.TrimSpace(query.Get("status")))
	limit, _ := strconv.Atoi(query.Get("limit"))
	offset, _ := strconv.Atoi(query.Get("offset"))
	list, err := server.declarations.List(request.Context(), traderID, status, limit, offset)
	if err != nil {
		writeDeclarationError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"declarations": list})
}

// declarationRead handles GET /v1/declarations/{id} and
// GET /v1/declarations/{id}/clearance-certificate.
func (server *Server) declarationRead(response http.ResponseWriter, request *http.Request) {
	path := strings.TrimPrefix(request.URL.Path, "/v1/declarations/")
	parts := strings.Split(path, "/")
	if len(parts) == 1 && parts[0] != "" {
		declaration, err := server.declarations.Get(request.Context(), parts[0])
		if err != nil {
			writeDeclarationError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, declaration)
		return
	}
	if len(parts) == 2 && parts[0] != "" && parts[1] == "clearance-certificate" {
		certificate, declaration, err := server.declarations.ClearanceCertificate(request.Context(), parts[0])
		if err != nil {
			writeDeclarationError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, map[string]any{
			"certificate": certificate,
			"declaration": declaration,
		})
		return
	}
	writeError(response, http.StatusNotFound, "declaration route not found")
}

// declarationOperation handles the POST sub-resources:
// POST /v1/declarations/{id}/submit and /v1/declarations/{id}/amendments.
func (server *Server) declarationOperation(response http.ResponseWriter, request *http.Request) {
	path := strings.TrimPrefix(request.URL.Path, "/v1/declarations/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[0] == "" {
		writeError(response, http.StatusNotFound, "declaration route not found")
		return
	}
	switch parts[1] {
	case "submit":
		server.submitDeclaration(response, request, parts[0])
	case "amendments":
		server.amendDeclaration(response, request, parts[0])
	default:
		writeError(response, http.StatusNotFound, "declaration operation not found")
	}
}

type declarationVersionRequest struct {
	ExpectedVersion int64 `json:"expected_version"`
}

// submitDeclaration runs DRAFT -> SUBMITTED and then the fail-closed risk
// assessment against the configured scorer. A scorer outage leaves the
// declaration in the terminal SCORING_UNAVAILABLE state; the response always
// reflects the persisted state.
func (server *Server) submitDeclaration(response http.ResponseWriter, request *http.Request, declarationID string) {
	var input declarationVersionRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.ExpectedVersion < 1 {
		writeError(response, http.StatusBadRequest, "expected_version must be a positive integer")
		return
	}
	principal := declarationPrincipal(request, "trader")
	submitted, err := server.declarations.Submit(request.Context(), declarationID, input.ExpectedVersion, principal)
	if err != nil {
		writeDeclarationError(response, err)
		return
	}
	assessed, err := server.declarations.AssessRisk(request.Context(), declarationID, submitted.Version,
		server.declarationScorer, server.declarationHighValueMinor, declarationPrincipal(request, "risk-engine"))
	if err != nil {
		writeDeclarationError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, assessed)
}

// amendDeclaration supersedes the live revision with a new DRAFT revision
// carrying the amended content.
func (server *Server) amendDeclaration(response http.ResponseWriter, request *http.Request, declarationID string) {
	var input struct {
		ExpectedVersion int64 `json:"expected_version"`
		declarations.CreateRequest
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.ExpectedVersion < 1 {
		writeError(response, http.StatusBadRequest, "expected_version and the amendment content are required")
		return
	}
	amended, err := server.declarations.Amend(request.Context(), declarationID, input.CreateRequest,
		input.ExpectedVersion, declarationPrincipal(request, "trader"))
	if err != nil {
		writeDeclarationError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, amended)
}

func writeDeclarationError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, declarations.ErrNotFound):
		writeError(response, http.StatusNotFound, err.Error())
	case errors.Is(err, declarations.ErrIdempotencyConflict), errors.Is(err, declarations.ErrOptimisticConflict),
		errors.Is(err, declarations.ErrInvalidTransition):
		writeError(response, http.StatusConflict, err.Error())
	case errors.Is(err, declarations.ErrNotCleared):
		writeError(response, http.StatusConflict, err.Error())
	case errors.Is(err, declarations.ErrDeclarationInvalid), errors.Is(err, declarations.ErrPermitInvalid):
		writeError(response, http.StatusUnprocessableEntity, err.Error())
	default:
		writeError(response, http.StatusInternalServerError, "internal declaration failure")
	}
}
