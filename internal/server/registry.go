package server

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/registry"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// registryPrincipal derives the registry store principal from the verified
// gateway claims; the request body can never impersonate another officer.
func registryPrincipal(claims tenantctx.Claims) registry.Principal {
	return registry.Principal{ID: claims.Subject, Role: RoleRegistryOfficer}
}

// registerVessel handles POST /v1/registry/vessels: open a vessel
// registration application (IMO/MMSI validated fail-closed).
func (server *Server) registerVessel(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleRegistryOfficer)
	if !ok {
		return
	}
	idempotencyKey, ok := idempotencyHeader(response, request)
	if !ok {
		return
	}
	var input registry.RegisterVesselRequest
	if !decodeJSON(response, request, &input) {
		return
	}
	vessel, err := server.registry.Register(request.Context(), idempotencyKey, input, registryPrincipal(claims))
	if err != nil {
		writeRegistryError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, vessel)
}

// registryVesselRead dispatches GET /v1/registry/vessels/... to the vessel
// read or the ownership-history read.
func (server *Server) registryVesselRead(response http.ResponseWriter, request *http.Request) {
	if _, ok := requireRole(response, request, RoleRegistryOfficer); !ok {
		return
	}
	rest := strings.TrimPrefix(request.URL.Path, "/v1/registry/vessels/")
	if strings.HasSuffix(rest, "/ownership") {
		vesselID := strings.TrimSuffix(rest, "/ownership")
		history, err := server.registry.OwnershipHistory(request.Context(), vesselID)
		if err != nil {
			writeRegistryError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, history)
		return
	}
	vessel, err := server.registry.Get(request.Context(), rest)
	if err != nil {
		writeRegistryError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, vessel)
}

// listVessels handles GET /v1/registry/vessels with an optional status
// filter.
func (server *Server) listVessels(response http.ResponseWriter, request *http.Request) {
	if _, ok := requireRole(response, request, RoleRegistryOfficer); !ok {
		return
	}
	vessels, err := server.registry.List(request.Context(), registry.VesselStatus(request.URL.Query().Get("status")), 0)
	if err != nil {
		writeRegistryError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, vessels)
}

// registryVesselOperation dispatches POST /v1/registry/vessels/... to the
// workflow transition or the ownership-transfer operation.
func (server *Server) registryVesselOperation(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleRegistryOfficer)
	if !ok {
		return
	}
	idempotencyKey, ok := idempotencyHeader(response, request)
	if !ok {
		return
	}
	rest := strings.TrimPrefix(request.URL.Path, "/v1/registry/vessels/")
	if strings.HasSuffix(rest, "/transitions") {
		vesselID := strings.TrimSuffix(rest, "/transitions")
		var input struct {
			Target            registry.VesselStatus `json:"target"`
			CertificateNumber string                `json:"certificateNumber,omitempty"`
		}
		if !decodeJSON(response, request, &input) {
			return
		}
		vessel, err := server.registry.Transition(request.Context(), idempotencyKey, vesselID, input.Target, input.CertificateNumber, registryPrincipal(claims))
		if err != nil {
			writeRegistryError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, vessel)
		return
	}
	if strings.HasSuffix(rest, "/ownership") {
		vesselID := strings.TrimSuffix(rest, "/ownership")
		var input struct {
			OwnerName     string `json:"ownerName"`
			OwnerCountry  string `json:"ownerCountry"`
			EffectiveFrom string `json:"effectiveFrom"` // RFC 3339
		}
		if !decodeJSON(response, request, &input) {
			return
		}
		effectiveFrom, err := time.Parse(time.RFC3339, input.EffectiveFrom)
		if err != nil {
			writeError(response, http.StatusBadRequest, "effectiveFrom must be an RFC 3339 timestamp")
			return
		}
		entry, err := server.registry.TransferOwnership(request.Context(), idempotencyKey, vesselID, input.OwnerName, input.OwnerCountry, effectiveFrom, registryPrincipal(claims))
		if err != nil {
			writeRegistryError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, entry)
		return
	}
	writeError(response, http.StatusNotFound, "unknown vessel operation")
}

// registerSeafarer handles POST /v1/registry/seafarers.
func (server *Server) registerSeafarer(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleRegistryOfficer)
	if !ok {
		return
	}
	idempotencyKey, ok := idempotencyHeader(response, request)
	if !ok {
		return
	}
	var input registry.RegisterSeafarerRequest
	if !decodeJSON(response, request, &input) {
		return
	}
	seafarer, err := server.registry.RegisterSeafarer(request.Context(), idempotencyKey, input, registryPrincipal(claims))
	if err != nil {
		writeRegistryError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, seafarer)
}

// registrySeafarerOperation dispatches POST /v1/registry/seafarers/... to
// the certificate-issuance operation.
func (server *Server) registrySeafarerOperation(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleRegistryOfficer)
	if !ok {
		return
	}
	idempotencyKey, ok := idempotencyHeader(response, request)
	if !ok {
		return
	}
	rest := strings.TrimPrefix(request.URL.Path, "/v1/registry/seafarers/")
	if !strings.HasSuffix(rest, "/certificates") {
		writeError(response, http.StatusNotFound, "unknown seafarer operation")
		return
	}
	seafarerID := strings.TrimSuffix(rest, "/certificates")
	var input registry.IssueCertificateRequest
	if !decodeJSON(response, request, &input) {
		return
	}
	// The holder is path-scoped; the body cannot re-target another seafarer.
	input.SeafarerID = seafarerID
	certificate, err := server.registry.IssueCertificate(request.Context(), idempotencyKey, input, registryPrincipal(claims))
	if err != nil {
		writeRegistryError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, certificate)
}

// verifyCertificate handles GET /v1/registry/certificates/verify: the
// metered third-party verification path. Third-party verifiers authenticate
// with the registry-verifier role; the verified subject is the metering
// identity, never a request parameter.
func (server *Server) verifyCertificate(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleRegistryVerifier)
	if !ok {
		return
	}
	certificateNumber := request.URL.Query().Get("certificateNumber")
	verification, err := server.registry.VerifyCertificate(request.Context(), certificateNumber, claims.Subject)
	if err != nil {
		writeRegistryError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, verification)
}

// certificateOperation handles POST /v1/registry/certificates/... status
// transitions (SUSPENDED/ACTIVE reinstatement/REVOKED).
func (server *Server) certificateOperation(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleRegistryOfficer)
	if !ok {
		return
	}
	idempotencyKey, ok := idempotencyHeader(response, request)
	if !ok {
		return
	}
	rest := strings.TrimPrefix(request.URL.Path, "/v1/registry/certificates/")
	if !strings.HasSuffix(rest, "/transitions") {
		writeError(response, http.StatusNotFound, "unknown certificate operation")
		return
	}
	certificateNumber := strings.TrimSuffix(rest, "/transitions")
	var input struct {
		Target registry.CertificateStatus `json:"target"`
	}
	if !decodeJSON(response, request, &input) {
		return
	}
	certificate, err := server.registry.TransitionCertificate(request.Context(), idempotencyKey, certificateNumber, input.Target, registryPrincipal(claims))
	if err != nil {
		writeRegistryError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, certificate)
}

// upsertCabotageRule handles POST /v1/registry/cabotage-rules: install a
// new ACTIVE eligibility rule (retiring the previous one).
func (server *Server) upsertCabotageRule(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleRegistryOfficer)
	if !ok {
		return
	}
	idempotencyKey, ok := idempotencyHeader(response, request)
	if !ok {
		return
	}
	var input registry.CabotageRule
	if !decodeJSON(response, request, &input) {
		return
	}
	rule, err := server.registry.UpsertCabotageRule(request.Context(), idempotencyKey, input, registryPrincipal(claims))
	if err != nil {
		writeRegistryError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, rule)
}

// applyCabotagePermit handles POST /v1/registry/cabotage-permits: evaluate
// eligibility against the ACTIVE rule and open the permit application.
func (server *Server) applyCabotagePermit(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleRegistryOfficer)
	if !ok {
		return
	}
	idempotencyKey, ok := idempotencyHeader(response, request)
	if !ok {
		return
	}
	var input registry.ApplyPermitRequest
	if !decodeJSON(response, request, &input) {
		return
	}
	permit, eligibility, err := server.registry.ApplyPermit(request.Context(), idempotencyKey, input, registryPrincipal(claims))
	if err != nil {
		writeRegistryError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, map[string]any{
		"permit":      permit,
		"eligibility": eligibility,
	})
}

// cabotagePermitOperation handles POST /v1/registry/cabotage-permits/.../decision:
// the maker-checker approve/reject step.
func (server *Server) cabotagePermitOperation(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleRegistryOfficer)
	if !ok {
		return
	}
	idempotencyKey, ok := idempotencyHeader(response, request)
	if !ok {
		return
	}
	rest := strings.TrimPrefix(request.URL.Path, "/v1/registry/cabotage-permits/")
	if !strings.HasSuffix(rest, "/decision") {
		writeError(response, http.StatusNotFound, "unknown cabotage permit operation")
		return
	}
	permitID := strings.TrimSuffix(rest, "/decision")
	var input struct {
		Approve bool `json:"approve"`
	}
	if !decodeJSON(response, request, &input) {
		return
	}
	permit, err := server.registry.DecidePermit(request.Context(), idempotencyKey, permitID, input.Approve, registryPrincipal(claims))
	if err != nil {
		writeRegistryError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, permit)
}

// flagCabotageViolation handles POST /v1/registry/cabotage-violations.
func (server *Server) flagCabotageViolation(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleRegistryOfficer)
	if !ok {
		return
	}
	idempotencyKey, ok := idempotencyHeader(response, request)
	if !ok {
		return
	}
	var input registry.Violation
	if !decodeJSON(response, request, &input) {
		return
	}
	violation, err := server.registry.FlagViolation(request.Context(), idempotencyKey, input, registryPrincipal(claims))
	if err != nil {
		writeRegistryError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, violation)
}

// cabotageViolationOperation handles POST /v1/registry/cabotage-violations/.../resolution:
// maker-checker closure of an open violation.
func (server *Server) cabotageViolationOperation(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleRegistryOfficer)
	if !ok {
		return
	}
	idempotencyKey, ok := idempotencyHeader(response, request)
	if !ok {
		return
	}
	rest := strings.TrimPrefix(request.URL.Path, "/v1/registry/cabotage-violations/")
	if !strings.HasSuffix(rest, "/resolution") {
		writeError(response, http.StatusNotFound, "unknown cabotage violation operation")
		return
	}
	violationID := strings.TrimSuffix(rest, "/resolution")
	violation, err := server.registry.ResolveViolation(request.Context(), idempotencyKey, violationID, registryPrincipal(claims))
	if err != nil {
		writeRegistryError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, violation)
}

func writeRegistryError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, registry.ErrNotFound):
		writeError(response, http.StatusNotFound, err.Error())
	case errors.Is(err, registry.ErrConflict), errors.Is(err, registry.ErrMakerChecker):
		writeError(response, http.StatusConflict, err.Error())
	default:
		// Validation errors are plain errors from the request Validate
		// methods; everything else is an internal failure surfaced opaque.
		message := err.Error()
		if strings.Contains(message, "must") || strings.Contains(message, "fails") || strings.Contains(message, "not an admitted") {
			writeError(response, http.StatusBadRequest, message)
			return
		}
		writeError(response, http.StatusInternalServerError, "registry operation failed")
	}
}
