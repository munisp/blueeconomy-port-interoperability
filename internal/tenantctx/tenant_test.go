package tenantctx

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func token(t *testing.T, key []byte, claims Claims) string {
	t.Helper()
	head, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	payload, _ := json.Marshal(claims)
	encoded := base64.RawURLEncoding.EncodeToString(head) + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(encoded))
	return encoded + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestMiddlewareDerivesTenantOnlyFromValidatedGatewayToken(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	verifier := Verifier{Key: key, Issuer: "gateway", Audience: "s1", Now: func() time.Time { return time.Unix(100, 0) }}
	handler := Middleware(verifier, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		claims, err := Tenant(request.Context())
		if err != nil {
			t.Fatal(err)
		}
		if claims.TenantID != "tenant-ministry-a" {
			t.Fatalf("tenant=%q", claims.TenantID)
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/v1/port-calls", nil)
	request.Header.Set("Authorization", "Bearer "+token(t, key, Claims{Issuer: "gateway", Audience: "s1", TenantID: "tenant-ministry-a", Subject: "operator", Expires: 101}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status=%d", response.Code)
	}
}

func TestMiddlewareRejectsManipulationAndCallerTenantHeader(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	verifier := Verifier{Key: key, Issuer: "gateway", Audience: "s1", Now: func() time.Time { return time.Unix(100, 0) }}
	handler := Middleware(verifier, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("downstream must not run") }))
	valid := token(t, key, Claims{Issuer: "gateway", Audience: "s1", TenantID: "tenant-ministry-a", Subject: "operator", Expires: 101})
	for name, mutate := range map[string]func(*http.Request){
		"tampered token": func(request *http.Request) { request.Header.Set("Authorization", "Bearer "+valid[:len(valid)-1]+"x") },
		"caller tenant header": func(request *http.Request) {
			request.Header.Set("Authorization", "Bearer "+valid)
			request.Header.Set("X-Tenant-ID", "tenant-ministry-b")
		},
		"wrong audience": func(request *http.Request) {
			request.Header.Set("Authorization", "Bearer "+token(t, key, Claims{Issuer: "gateway", Audience: "s2", TenantID: "tenant-ministry-a", Subject: "operator", Expires: 101}))
		},
		"expired": func(request *http.Request) {
			request.Header.Set("Authorization", "Bearer "+token(t, key, Claims{Issuer: "gateway", Audience: "s1", TenantID: "tenant-ministry-a", Subject: "operator", Expires: 100}))
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			mutate(request)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized && response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d", response.Code)
			}
		})
	}
}
