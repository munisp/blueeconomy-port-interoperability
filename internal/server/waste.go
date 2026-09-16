package server

import (
	"errors"
	"net/http"
	"strings"

	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
	"github.com/munisp/blueeconomy-port-interoperability/internal/waste"
)

// wastePrincipal derives the waste store principal from the verified
// gateway claims; the role carried into provenance is the verified role,
// never a request parameter.
func wastePrincipal(claims tenantctx.Claims, role string) waste.Principal {
	return waste.Principal{ID: claims.Subject, Role: role}
}

// createWasteReceipt handles POST /v1/port-calls/{id}/waste-receipts:
// open a DRAFT MARPOL waste-delivery receipt on the port call.
func (server *Server) createWasteReceipt(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RolePortOperatorAdmin)
	if !ok {
		return
	}
	idempotencyKey, ok := idempotencyHeader(response, request)
	if !ok {
		return
	}
	portCallID := request.PathValue("id")
	var input waste.CreateRequest
	if !decodeJSON(response, request, &input) {
		return
	}
	// The port call is path-scoped; the body cannot re-target another call.
	input.PortCallID = portCallID
	receipt, err := server.waste.Create(request.Context(), idempotencyKey, input, wastePrincipal(claims, RolePortOperatorAdmin))
	if err != nil {
		writeWasteError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, receipt)
}

// listWasteReceipts handles GET /v1/port-calls/{id}/waste-receipts: the
// waste-delivery record exposed on the port call.
func (server *Server) listWasteReceipts(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RolePortOperatorAdmin)
	if !ok {
		return
	}
	portCallID := request.PathValue("id")
	receipts, err := server.waste.ListByPortCall(request.Context(), portCallID)
	if err != nil {
		writeWasteError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{
		"portCallId": portCallID,
		"receipts":   receipts,
		"requestedBy": claims.Subject,
	})
}

// wasteReceiptOperation handles POST /v1/waste-receipts/{id}/transitions:
// DELIVERED (recorder) and VERIFIED (NPA officer, maker-checker).
func (server *Server) wasteReceiptOperation(response http.ResponseWriter, request *http.Request) {
	rest := strings.TrimPrefix(request.URL.Path, "/v1/waste-receipts/")
	if !strings.HasSuffix(rest, "/transitions") {
		writeError(response, http.StatusNotFound, "unknown waste receipt operation")
		return
	}
	receiptID := strings.TrimSuffix(rest, "/transitions")
	var input struct {
		Target waste.Status `json:"target"`
	}
	if !decodeJSON(response, request, &input) {
		return
	}
	idempotencyKey, ok := idempotencyHeader(response, request)
	if !ok {
		return
	}
	switch input.Target {
	case waste.StatusDelivered:
		claims, ok := requireRole(response, request, RolePortOperatorAdmin)
		if !ok {
			return
		}
		receipt, err := server.waste.MarkDelivered(request.Context(), idempotencyKey, receiptID, wastePrincipal(claims, RolePortOperatorAdmin))
		if err != nil {
			writeWasteError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, receipt)
	case waste.StatusVerified:
		claims, ok := requireRole(response, request, RoleNPAOfficer)
		if !ok {
			return
		}
		receipt, err := server.waste.Verify(request.Context(), idempotencyKey, receiptID, wastePrincipal(claims, RoleNPAOfficer))
		if err != nil {
			writeWasteError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, receipt)
	default:
		writeError(response, http.StatusBadRequest, "target must be DELIVERED or VERIFIED")
	}
}

func writeWasteError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, waste.ErrNotFound):
		writeError(response, http.StatusNotFound, err.Error())
	case errors.Is(err, waste.ErrConflict), errors.Is(err, waste.ErrMakerChecker):
		writeError(response, http.StatusConflict, err.Error())
	default:
		message := err.Error()
		if strings.Contains(message, "must") || strings.Contains(message, "not admitted") {
			writeError(response, http.StatusBadRequest, message)
			return
		}
		writeError(response, http.StatusInternalServerError, "waste receipt operation failed")
	}
}
