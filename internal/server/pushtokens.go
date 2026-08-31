package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/munisp/blueeconomy-port-interoperability/internal/pushtokens"
)

// registerPushToken handles POST /v1/push-tokens: upsert the caller's
// device push-token registration. The user scope is the verified gateway
// subject (tenant middleware), never the request body.
func (server *Server) registerPushToken(response http.ResponseWriter, request *http.Request) {
	var input pushtokens.RegisterRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid push-token JSON")
		return
	}
	if err := input.Validate(); err != nil {
		writeError(response, http.StatusBadRequest, err.Error())
		return
	}
	token, err := server.pushTokens.Register(request.Context(), input)
	if err != nil {
		writePushTokenError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, token)
}

// revokePushToken handles POST /v1/push-tokens/revoke: revoke the caller's
// device registration (logout / token rollover).
func (server *Server) revokePushToken(response http.ResponseWriter, request *http.Request) {
	var input struct {
		DeviceID string `json:"deviceId"`
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid push-token revoke JSON")
		return
	}
	token, err := server.pushTokens.Revoke(request.Context(), input.DeviceID)
	if err != nil {
		writePushTokenError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, token)
}

func writePushTokenError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, pushtokens.ErrNotFound):
		writeError(response, http.StatusNotFound, err.Error())
	default:
		writeError(response, http.StatusInternalServerError, "push-token operation failed")
	}
}
