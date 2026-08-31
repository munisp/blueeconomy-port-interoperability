package server

import (
	"context"
	"net/http"
	"net/http/httptest"

	"github.com/jackc/pgx/v5/pgxpool"
	"testing"

	"github.com/munisp/blueeconomy-port-interoperability/internal/pushtokens"
)

// The push-token routes are tenant-middleware protected like every other
// /v1 route; these tests exercise the handler surface with the in-memory
// store seam (no database). Storage semantics are covered by the real-PG
// tests in internal/pushtokens.

func pushTokenRequest(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	handler := newWiredHandler(t)
	request := loopbackRequest(method, path, body)
	request.Header.Set("Authorization", "Bearer "+mintToken(t, "driver-1", RoleTrucker))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestRegisterPushToken(t *testing.T) {
	response := pushTokenRequest(t, http.MethodPost, "/v1/push-tokens",
		`{"deviceId":"device-1","token":"fcm-token-abcdef123456","platform":"android"}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("register push token = %d, want 201 (%s)", response.Code, response.Body.String())
	}
}

func TestRegisterPushTokenValidation(t *testing.T) {
	for name, body := range map[string]string{
		"missing deviceId": `{"token":"fcm-token-abcdef123456","platform":"android"}`,
		"short token":      `{"deviceId":"d","token":"short","platform":"android"}`,
		"bad platform":     `{"deviceId":"d","token":"fcm-token-abcdef123456","platform":"smarttv"}`,
		"malformed JSON":   `{not json`,
		"unknown field":    `{"deviceId":"d","token":"fcm-token-abcdef123456","platform":"ios","userId":"forged"}`,
	} {
		response := pushTokenRequest(t, http.MethodPost, "/v1/push-tokens", body)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s: register = %d, want 400 (%s)", name, response.Code, response.Body.String())
		}
	}
}

func TestRegisterPushTokenRequiresTenantToken(t *testing.T) {
	handler := newWiredHandler(t)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, loopbackRequest(http.MethodPost, "/v1/push-tokens",
		`{"deviceId":"d","token":"fcm-token-abcdef123456","platform":"android"}`))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("register without tenant token = %d, want 401", response.Code)
	}
}

func TestRevokePushToken(t *testing.T) {
	response := pushTokenRequest(t, http.MethodPost, "/v1/push-tokens/revoke", `{"deviceId":"device-1"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("revoke push token = %d, want 200 (%s)", response.Code, response.Body.String())
	}
}

func TestRevokePushTokenNotFound(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://127.0.0.1:1/unreachable")
	if err != nil {
		t.Fatalf("build test pool: %v", err)
	}
	t.Cleanup(pool.Close)
	config := testConfig()
	config.Pool = pool
	config.PushTokens = fakePushTokens{revokeErr: pushtokens.ErrNotFound}
	handler, err := New(config)
	if err != nil {
		t.Fatalf("wire server: %v", err)
	}
	request := loopbackRequest(http.MethodPost, "/v1/push-tokens/revoke", `{"deviceId":"absent"}`)
	request.Header.Set("Authorization", "Bearer "+mintToken(t, "driver-1", RoleTrucker))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("revoke unknown device = %d, want 404 (%s)", response.Code, response.Body.String())
	}
}
