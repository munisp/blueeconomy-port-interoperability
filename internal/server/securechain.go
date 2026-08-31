package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/securechain"
	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

// Secure-chain platform roles (WP-7). Shipping lines open chains and hold
// release authority; terminal gate officers consume release tokens. Org
// identity is always the verified token subject — no body field can
// substitute for it.
const (
	RoleShippingLine      = "shipping-line"
	RoleTerminalOperator  = "terminal-operator"
	secureChainPrincipal  = "secure-chain-api"
)

func secureChainPrincipalOf(claims tenantctx.Claims, role string) securechain.Principal {
	return securechain.Principal{ID: claims.Subject, Role: role}
}

func writeSecureChainError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, securechain.ErrNotFound):
		writeError(response, http.StatusNotFound, err.Error())
	case errors.Is(err, securechain.ErrNoReleaseAuthority),
		errors.Is(err, securechain.ErrNotTailHolder),
		errors.Is(err, securechain.ErrNotAuthorizedParty):
		writeError(response, http.StatusForbidden, err.Error())
	case errors.Is(err, securechain.ErrIdempotencyConflict):
		writeError(response, http.StatusConflict, err.Error())
	case errors.Is(err, securechain.ErrVelocityHold),
		errors.Is(err, securechain.ErrInvalidTransition),
		errors.Is(err, securechain.ErrTokenInvalid):
		writeError(response, http.StatusUnprocessableEntity, err.Error())
	default:
		writeError(response, http.StatusBadRequest, err.Error())
	}
}

// registerBLAuthority records the calling shipping line's release authority
// over a B/L digest (shipping-line role only).
func (server *Server) registerBLAuthority(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleShippingLine)
	if !ok {
		return
	}
	var input struct {
		ContainerID string `json:"container_id"`
		BLDigest    string `json:"bl_digest"`
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid B/L authority JSON")
		return
	}
	if err := server.secureChains.RegisterBLAuthority(request.Context(), input.ContainerID, input.BLDigest,
		secureChainPrincipalOf(claims, RoleShippingLine)); err != nil {
		writeSecureChainError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, map[string]string{
		"container_id": strings.ToUpper(strings.TrimSpace(input.ContainerID)),
		"bl_digest":    input.BLDigest,
		"shipping_line": claims.Subject,
	})
}

// createSecureChain opens a release chain (shipping-line role only).
func (server *Server) createSecureChain(response http.ResponseWriter, request *http.Request) {
	claims, ok := requireRole(response, request, RoleShippingLine)
	if !ok {
		return
	}
	idempotencyKey := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		writeError(response, http.StatusBadRequest, "missing Idempotency-Key")
		return
	}
	var input struct {
		ContainerID string `json:"container_id"`
		BLDigest    string `json:"bl_digest"`
		ExpiresAt   string `json:"expires_at"`
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid secure chain JSON")
		return
	}
	expiresAt, err := time.Parse(time.RFC3339, input.ExpiresAt)
	if err != nil {
		writeError(response, http.StatusBadRequest, "expires_at must be RFC 3339")
		return
	}
	chain, err := server.secureChains.CreateChain(request.Context(), idempotencyKey, input.ContainerID, input.BLDigest,
		expiresAt, secureChainPrincipalOf(claims, RoleShippingLine))
	if err != nil {
		writeSecureChainError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, chain)
}

// secureChainRead serves GET /v1/secure-chains/{container} and
// GET /v1/secure-chains/audit/{chain_id}.
func (server *Server) secureChainRead(response http.ResponseWriter, request *http.Request) {
	if _, ok := claimsOf(response, request); !ok {
		return
	}
	path := strings.TrimPrefix(request.URL.Path, "/v1/secure-chains/")
	if strings.HasPrefix(path, "audit/") {
		entries, err := server.secureChains.AuditTrail(request.Context(), strings.TrimPrefix(path, "audit/"))
		if err != nil {
			writeSecureChainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, map[string]any{"entries": entries})
		return
	}
	if path == "" || strings.Contains(path, "/") {
		writeError(response, http.StatusNotFound, "secure chain not found")
		return
	}
	chain, err := server.secureChains.GetByContainer(request.Context(), path)
	if err != nil {
		writeSecureChainError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, chain)
}

// secureChainOperation dispatches POST /v1/secure-chains/{id}/{operation}.
func (server *Server) secureChainOperation(response http.ResponseWriter, request *http.Request) {
	claims, ok := claimsOf(response, request)
	if !ok {
		return
	}
	path := strings.TrimPrefix(request.URL.Path, "/v1/secure-chains/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[0] == "" {
		writeError(response, http.StatusNotFound, "secure chain operation not found")
		return
	}
	chainID := parts[0]
	principal := secureChainPrincipalOf(claims, "chain-party")
	switch parts[1] {
	case "nominations":
		var input struct {
			ToOrg string `json:"to_org"`
		}
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeError(response, http.StatusBadRequest, "invalid nomination JSON")
			return
		}
		link, err := server.secureChains.Nominate(request.Context(), chainID, input.ToOrg, principal)
		if err != nil {
			writeSecureChainError(response, err)
			return
		}
		writeJSON(response, http.StatusCreated, link)
	case "links":
		// /v1/secure-chains/{id}/links/{seq}/accept|decline
		if len(parts) != 4 {
			writeError(response, http.StatusNotFound, "secure chain link operation not found")
			return
		}
		seq, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil || seq < 1 {
			writeError(response, http.StatusBadRequest, "link seq must be a positive integer")
			return
		}
		switch parts[3] {
		case "accept":
			link, err := server.secureChains.Accept(request.Context(), chainID, seq, principal)
			if err != nil {
				writeSecureChainError(response, err)
				return
			}
			writeJSON(response, http.StatusOK, link)
		case "decline":
			var input struct {
				Reason string `json:"reason"`
			}
			decoder := json.NewDecoder(request.Body)
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&input); err != nil {
				writeError(response, http.StatusBadRequest, "invalid decline JSON")
				return
			}
			link, err := server.secureChains.Decline(request.Context(), chainID, seq, input.Reason, principal)
			if err != nil {
				writeSecureChainError(response, err)
				return
			}
			writeJSON(response, http.StatusOK, link)
		default:
			writeError(response, http.StatusNotFound, "secure chain link operation not found")
		}
	case "revoke":
		var input struct {
			Reason string `json:"reason"`
		}
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeError(response, http.StatusBadRequest, "invalid revoke JSON")
			return
		}
		chain, err := server.secureChains.Revoke(request.Context(), chainID, input.Reason, principal)
		if err != nil {
			writeSecureChainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, chain)
	default:
		writeError(response, http.StatusNotFound, "secure chain operation not found")
	}
}

// releaseAuthorization is the terminal-release check: 200 with the signed
// single-use token only for the verified chain tail holder; everyone else
// gets 403/404 — never a hint of who holds the chain.
func (server *Server) releaseAuthorization(response http.ResponseWriter, request *http.Request) {
	claims, ok := claimsOf(response, request)
	if !ok {
		return
	}
	containerID := strings.TrimPrefix(request.URL.Path, "/v1/secure-chain/")
	containerID = strings.TrimSuffix(containerID, "/release-authorization")
	token, err := server.secureChains.ReleaseAuthorization(request.Context(), containerID,
		secureChainPrincipalOf(claims, "chain-tail"))
	if err != nil {
		writeSecureChainError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, token)
}

// consumeRelease redeems a single-use release token at the gate
// (terminal-operator or gate-officer role).
func (server *Server) consumeRelease(response http.ResponseWriter, request *http.Request) {
	claims, ok := claimsOf(response, request)
	if !ok {
		return
	}
	if !claims.HasAnyRole(RoleTerminalOperator, RoleGateOfficer) {
		writeError(response, http.StatusForbidden, "verified terminal-operator or gate-officer role is required")
		return
	}
	var input struct {
		Nonce  string `json:"nonce"`
		GateID string `json:"gate_id"`
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid release consumption JSON")
		return
	}
	token, err := server.secureChains.Consume(request.Context(), input.Nonce, input.GateID,
		secureChainPrincipalOf(claims, "gate"))
	if err != nil {
		writeSecureChainError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, token)
}
