package tenantctx

import (
	"testing"
	"time"
)

func TestVerifierCarriesAndSanitizesVerifiedRoles(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	verifier := Verifier{Key: key, Issuer: "gateway", Audience: "s1", Now: func() time.Time { return time.Unix(100, 0) }}
	claims, err := verifier.Verify(token(t, key, Claims{
		Issuer: "gateway", Audience: "s1", TenantID: "tenant-ministry-a", Subject: "operator", Expires: 101,
		Roles: []string{"payment-switch", "payment-switch", "Port-Operator-Admin", "trucker", "", "x"},
	}))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !claims.HasRole("payment-switch") || !claims.HasRole("trucker") {
		t.Fatalf("verified roles lost: %#v", claims.Roles)
	}
	if claims.HasRole("Port-Operator-Admin") || claims.HasRole("") || claims.HasRole("x") {
		t.Fatalf("malformed roles must be dropped fail-closed: %#v", claims.Roles)
	}
	if len(claims.Roles) != 2 {
		t.Fatalf("roles must be deduplicated: %#v", claims.Roles)
	}
	if claims.HasAnyRole("npa-officer", "customs-officer") {
		t.Fatal("HasAnyRole must be false when no listed role is held")
	}
	if !claims.HasAnyRole("npa-officer", "trucker") {
		t.Fatal("HasAnyRole must be true when any listed role is held")
	}
}

func TestTokenWithoutRolesHasNoPrivileges(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	verifier := Verifier{Key: key, Issuer: "gateway", Audience: "s1", Now: func() time.Time { return time.Unix(100, 0) }}
	claims, err := verifier.Verify(token(t, key, Claims{
		Issuer: "gateway", Audience: "s1", TenantID: "tenant-ministry-a", Subject: "operator", Expires: 101,
	}))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if claims.HasRole("payment-switch") || claims.HasRole("port-operator-admin") {
		t.Fatal("a role-less token must never satisfy a role gate")
	}
}
