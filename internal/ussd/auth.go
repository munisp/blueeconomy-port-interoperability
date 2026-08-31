package ussd

import (
	"crypto/subtle"
	"errors"
	"net/http"
)

// CallbackSecretHeader carries the carrier/gateway shared secret that
// authenticates a USSD callback. The value is provisioned env-only and
// compared in constant time.
const CallbackSecretHeader = "X-USSD-Callback-Secret"

// AuthenticateCallback wraps the USSD callback handler with shared-secret
// authentication. The secret is mandatory: booting the gateway without one
// would expose an unauthenticated booking channel, so a missing secret is an
// error (fail closed) rather than a disabled check.
func AuthenticateCallback(secret string, next http.Handler) (http.Handler, error) {
	if secret == "" || next == nil {
		return nil, errors.New("USSD callback authentication requires a shared secret and a handler")
	}
	expected := []byte(secret)
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		presented := []byte(request.Header.Get(CallbackSecretHeader))
		if len(presented) != len(expected) || subtle.ConstantTimeCompare(presented, expected) != 1 {
			http.Error(response, "unauthorized carrier callback", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(response, request)
	}), nil
}
