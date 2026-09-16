package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/registry"
)

// RegistryStore is the ship-registry persistence seam (clone of the
// registry package surface the handlers need).
type RegistryStore interface {
	Register(context.Context, string, registry.RegisterVesselRequest, registry.Principal) (registry.Vessel, error)
	Get(context.Context, string) (registry.Vessel, error)
	List(context.Context, registry.VesselStatus, int) ([]registry.Vessel, error)
	OwnershipHistory(context.Context, string) ([]registry.OwnershipEntry, error)
	Transition(context.Context, string, string, registry.VesselStatus, string, registry.Principal) (registry.Vessel, error)
	TransferOwnership(context.Context, string, string, string, string, time.Time, registry.Principal) (registry.OwnershipEntry, error)
	RegisterSeafarer(context.Context, string, registry.RegisterSeafarerRequest, registry.Principal) (registry.Seafarer, error)
	IssueCertificate(context.Context, string, registry.IssueCertificateRequest, registry.Principal) (registry.Certificate, error)
	TransitionCertificate(context.Context, string, string, registry.CertificateStatus, registry.Principal) (registry.Certificate, error)
	VerifyCertificate(context.Context, string, string) (registry.Verification, error)
	UpsertCabotageRule(context.Context, string, registry.CabotageRule, registry.Principal) (registry.CabotageRule, error)
	ApplyPermit(context.Context, string, registry.ApplyPermitRequest, registry.Principal) (registry.CabotagePermit, registry.Eligibility, error)
	DecidePermit(context.Context, string, string, bool, registry.Principal) (registry.CabotagePermit, error)
	GetPermit(context.Context, string) (registry.CabotagePermit, error)
	FlagViolation(context.Context, string, registry.Violation, registry.Principal) (registry.Violation, error)
	ResolveViolation(context.Context, string, string, registry.Principal) (registry.Violation, error)
	// Phase 19: fisheries licensing & permit registry.
	ApplyFisheriesPermit(context.Context, string, registry.ApplyFisheriesPermitRequest, registry.Principal) (registry.FisheriesPermit, error)
	DecideFisheriesPermit(context.Context, string, string, bool, registry.Principal) (registry.FisheriesPermit, error)
	TransitionFisheriesPermit(context.Context, string, string, registry.FisheriesPermitStatus, string, registry.Principal) (registry.FisheriesPermit, error)
	GetFisheriesPermit(context.Context, string) (registry.FisheriesPermit, error)
	FisheriesAuditTrail(context.Context, string) ([]registry.FisheriesAuditEntry, error)
	VerifyFisheriesPermit(context.Context, string) (registry.FisheriesVerification, error)
}

func registryPrincipal(request *http.Request) registry.Principal {
	return registry.Principal{ID: authenticatedPrincipal(request), Role: primaryRole(request)}
}

func writeRegistryError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, registry.ErrNotFound):
		writeError(writer, http.StatusNotFound, err.Error())
	case errors.Is(err, registry.ErrConflict), errors.Is(err, registry.ErrMakerChecker):
		writeError(writer, http.StatusConflict, err.Error())
	default:
		writeError(writer, http.StatusBadRequest, err.Error())
	}
}

type fisheriesPermitDecisionRequest struct {
	Approve bool `json:"approve"`
}

type fisheriesPermitTransitionRequest struct {
	Target string `json:"target"`
	Reason string `json:"reason"`
}

// applyFisheriesPermit handles POST /v1/registry/fisheries-permits.
func (server *Server) applyFisheriesPermit(writer http.ResponseWriter, request *http.Request) {
	var body registry.ApplyFisheriesPermitRequest
	if !decodeJSONBody(writer, request, &body) {
		return
	}
	permit, err := server.config.Registry.ApplyFisheriesPermit(request.Context(),
		idempotencyKey(request), body, registryPrincipal(request))
	if err != nil {
		writeRegistryError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, permit)
}

// decideFisheriesPermit handles POST /v1/registry/fisheries-permits/{permitId}/decision.
func (server *Server) decideFisheriesPermit(writer http.ResponseWriter, request *http.Request) {
	var body fisheriesPermitDecisionRequest
	if !decodeJSONBody(writer, request, &body) {
		return
	}
	permit, err := server.config.Registry.DecideFisheriesPermit(request.Context(),
		idempotencyKey(request), request.PathValue("permitId"), body.Approve, registryPrincipal(request))
	if err != nil {
		writeRegistryError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, permit)
}

// transitionFisheriesPermit handles POST /v1/registry/fisheries-permits/{permitId}/transitions.
func (server *Server) transitionFisheriesPermit(writer http.ResponseWriter, request *http.Request) {
	var body fisheriesPermitTransitionRequest
	if !decodeJSONBody(writer, request, &body) {
		return
	}
	permit, err := server.config.Registry.TransitionFisheriesPermit(request.Context(),
		idempotencyKey(request), request.PathValue("permitId"),
		registry.FisheriesPermitStatus(strings.ToUpper(strings.TrimSpace(body.Target))), body.Reason, registryPrincipal(request))
	if err != nil {
		writeRegistryError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, permit)
}

// getFisheriesPermit handles GET /v1/registry/fisheries-permits/{permitId}.
func (server *Server) getFisheriesPermit(writer http.ResponseWriter, request *http.Request) {
	permit, err := server.config.Registry.GetFisheriesPermit(request.Context(), request.PathValue("permitId"))
	if err != nil {
		writeRegistryError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, permit)
}

// fisheriesAuditTrail handles GET /v1/registry/fisheries-permits/{permitId}/audit.
func (server *Server) fisheriesAuditTrail(writer http.ResponseWriter, request *http.Request) {
	trail, err := server.config.Registry.FisheriesAuditTrail(request.Context(), request.PathValue("permitId"))
	if err != nil {
		writeRegistryError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"entries": trail})
}

// verifyFisheriesPermit handles GET /v1/registry/fisheries-permits/verify?permitNumber=
// — the minimal-disclosure public verification surface (registry-verifier role).
func (server *Server) verifyFisheriesPermit(writer http.ResponseWriter, request *http.Request) {
	verification, err := server.config.Registry.VerifyFisheriesPermit(request.Context(),
		strings.TrimSpace(request.URL.Query().Get("permitNumber")))
	if err != nil {
		writeRegistryError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, verification)
}

var _ = json.Marshal
