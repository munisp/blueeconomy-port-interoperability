package server

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/munisp/blueeconomy-port-interoperability/internal/imonumber"
	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs"
	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs/ais"
	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs/linkage"
	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs/tos"
)

// Handlers for the Phase 16 PCS surface (GAP-PCS-AIS, GAP-BERTH-OPS,
// GAP-PORTCALL-LINKAGE): AIS ingestion status and validated positions,
// TOS berth occupancy, and the AIS↔TOS↔NSW port-call linkage. Every
// route is gated on a verified NPA-officer or port-operator-admin token,
// and every leg is config-gated fail-closed: an unconfigured integration
// answers 503 with an honest configured:false status, never fake data.

func (server *Server) pcsService(response http.ResponseWriter) (*pcs.Service, bool) {
	if server.pcs == nil {
		writeError(response, http.StatusServiceUnavailable, "PCS integrations are not configured")
		return nil, false
	}
	return server.pcs, true
}

// requirePCSRole gates PCS routes on either operational role.
func requirePCSRole(response http.ResponseWriter, request *http.Request) bool {
	claims, ok := claimsOf(response, request)
	if !ok {
		return false
	}
	if !claims.HasRole(RoleNPAOfficer) && !claims.HasRole(RolePortOperatorAdmin) {
		writeError(response, http.StatusForbidden, "verified npa-officer or port-operator-admin role is required")
		return false
	}
	return true
}

func (server *Server) pcsStatus(response http.ResponseWriter, request *http.Request) {
	if !requirePCSRole(response, request) {
		return
	}
	service, ok := server.pcsService(response)
	if !ok {
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{
		"ais":     service.AISStatus(),
		"tos":     service.TOSStatus(),
		"linkage": service.LinkageConfigured(),
	})
}

func (server *Server) pcsAISStatus(response http.ResponseWriter, request *http.Request) {
	if !requirePCSRole(response, request) {
		return
	}
	service, ok := server.pcsService(response)
	if !ok {
		return
	}
	writeJSON(response, http.StatusOK, service.AISStatus())
}

func (server *Server) pcsAISPositions(response http.ResponseWriter, request *http.Request) {
	if !requirePCSRole(response, request) {
		return
	}
	service, ok := server.pcsService(response)
	if !ok {
		return
	}
	if service.AIS == nil {
		writeError(response, http.StatusServiceUnavailable, "AIS feed is not configured")
		return
	}
	mmsi := strings.TrimSpace(request.URL.Query().Get("mmsi"))
	limit := 100
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 1000 {
			writeError(response, http.StatusBadRequest, "limit must be between 1 and 1000")
			return
		}
		limit = parsed
	}
	positions, err := service.AIS.Latest(request.Context(), mmsi, limit)
	if err != nil {
		writePCSError(response, err)
		return
	}
	if positions == nil {
		positions = []ais.PositionReport{}
	}
	writeJSON(response, http.StatusOK, map[string]any{"positions": positions})
}

func (server *Server) pcsTOSBerths(response http.ResponseWriter, request *http.Request) {
	if !requirePCSRole(response, request) {
		return
	}
	service, ok := server.pcsService(response)
	if !ok {
		return
	}
	if service.TOS == nil {
		writeError(response, http.StatusServiceUnavailable, "TOS endpoint is not configured")
		return
	}
	portCode := strings.TrimSpace(request.URL.Query().Get("port_code"))
	if portCode == "" || len(portCode) > 8 {
		writeError(response, http.StatusBadRequest, "port_code query parameter is required")
		return
	}
	occupancies, err := service.TOS.BerthOccupancy(request.Context(), portCode)
	if err != nil {
		writePCSError(response, err)
		return
	}
	if occupancies == nil {
		occupancies = []tos.BerthOccupancy{}
	}
	writeJSON(response, http.StatusOK, map[string]any{"berths": occupancies})
}

func (server *Server) pcsPortCalls(response http.ResponseWriter, request *http.Request) {
	if !requirePCSRole(response, request) {
		return
	}
	service, ok := server.pcsService(response)
	if !ok {
		return
	}
	imo := strings.TrimSpace(request.URL.Query().Get("imo"))
	if imo == "" {
		// MMSI-only callers resolve the IMO through the validated AIS
		// identity leg; an unresolvable MMSI is an honest 404.
		mmsi := strings.TrimSpace(request.URL.Query().Get("mmsi"))
		if mmsi == "" {
			writeError(response, http.StatusBadRequest, "imo or mmsi query parameter is required")
			return
		}
		resolved, found := service.ResolveMMSI(request.Context(), mmsi)
		if !found {
			writeError(response, http.StatusNotFound, "no validated AIS identity resolves that MMSI")
			return
		}
		imo = resolved
	}
	if !imonumber.Valid(imo) {
		writeError(response, http.StatusBadRequest, "imo must be a seven-digit IMO number with a valid check digit")
		return
	}
	limit := 25
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			writeError(response, http.StatusBadRequest, "limit must be between 1 and 100")
			return
		}
		limit = parsed
	}
	linked, err := service.LinkByIMO(request.Context(), imo, limit)
	if err != nil {
		writePCSError(response, err)
		return
	}
	if linked == nil {
		linked = []linkage.LinkedPortCall{}
	}
	writeJSON(response, http.StatusOK, map[string]any{"port_calls": linked})
}

func writePCSError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ais.ErrUnconfigured), errors.Is(err, tos.ErrUnconfigured),
		errors.Is(err, tos.ErrCircuitOpen), errors.Is(err, tos.ErrTimeout),
		errors.Is(err, linkage.ErrUnconfigured):
		writeError(response, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, tos.ErrNotFound):
		writeError(response, http.StatusNotFound, err.Error())
	case errors.Is(err, tos.ErrUpstream), errors.Is(err, ais.ErrUpstream), errors.Is(err, tos.ErrMalformed):
		writeError(response, http.StatusBadGateway, err.Error())
	default:
		writeError(response, http.StatusInternalServerError, "internal PCS failure")
	}
}
