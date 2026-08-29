package server

import (
	"net"
	"net/http"
	"strings"

	"github.com/munisp/blueeconomy-port-interoperability/internal/tenantctx"
)

const AuthModeLoopbackTrustedProxy = "loopback_trusted_proxy"

// Platform PBAC roles. They are only honoured when carried by a verified
// gateway token (tenantctx.Claims.Roles); a token without roles can use the
// unprivileged trader/trucker surface and nothing else.
const (
	RoleTrader            = "trader"
	RoleTrucker           = "trucker"
	RoleGateOfficer       = "gate-officer"
	RoleCustomsOfficer    = "customs-officer"
	RoleNPAOfficer        = "npa-officer"
	RolePortOperatorAdmin = "port-operator-admin"
	// RolePaymentSwitch is the settlement-switch service identity. Only this
	// role may confirm a booking payment; a tenant user can never confirm
	// their own booking.
	RolePaymentSwitch = "payment-switch"
)

// claimsOf extracts the verified tenant claims; the tenant middleware
// guarantees them, so a miss is a server defect surfaced as 401.
func claimsOf(response http.ResponseWriter, request *http.Request) (tenantctx.Claims, bool) {
	claims, err := tenantctx.Tenant(request.Context())
	if err != nil {
		writeError(response, http.StatusUnauthorized, "verified tenant claims are required")
		return tenantctx.Claims{}, false
	}
	return claims, true
}

// requireRole gates a route on a verified platform role from the token
// claims. Absence of the role is a 403 — never a silently downgraded actor.
func requireRole(response http.ResponseWriter, request *http.Request, role string) (tenantctx.Claims, bool) {
	claims, ok := claimsOf(response, request)
	if !ok {
		return tenantctx.Claims{}, false
	}
	if !claims.HasRole(role) {
		writeError(response, http.StatusForbidden, "verified "+role+" role is required")
		return tenantctx.Claims{}, false
	}
	return claims, true
}

func requireAuthentication(mode string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if mode != AuthModeLoopbackTrustedProxy {
			writeError(response, http.StatusInternalServerError, "authentication mode is not approved")
			return
		}
		host, _, err := net.SplitHostPort(request.RemoteAddr)
		if err != nil || !net.ParseIP(host).IsLoopback() {
			writeError(response, http.StatusForbidden, "request is not from the trusted local edge")
			return
		}
		if strings.TrimSpace(request.Header.Get("X-Trusted-Proxy")) != "loopback" ||
			strings.TrimSpace(request.Header.Get("X-Authenticated-Principal")) == "" {
			writeError(response, http.StatusUnauthorized, "verified caller identity is required")
			return
		}
		next.ServeHTTP(response, request)
	})
}
