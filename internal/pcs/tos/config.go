// Package tos implements the Terminal Operating System berth/operations
// adapter for the PCS surface: an env-gated HTTPS client exposing berth
// occupancy and vessel berth-assignment queries, typed errors and a
// circuit breaker so a degraded terminal system never stalls the API.
// When TOS_ENDPOINT is not configured every call fails closed with
// ErrUnconfigured — the adapter never fabricates berth state.
package tos

import (
	"errors"
	"net/url"
	"strings"
	"time"
)

const defaultMaxBodyBytes = 1 << 20

// Config is the fail-closed TOS adapter configuration sourced from the
// environment (TOS_ENDPOINT, TOS_API_KEY, TOS_TIMEOUT,
// TOS_BREAKER_THRESHOLD, TOS_BREAKER_COOLDOWN).
type Config struct {
	Endpoint         string        // HTTPS only, redirects are never followed
	APIKey           string        // bearer credential, env-only, never logged
	Timeout          time.Duration // per-request timeout
	MaxBodyBytes     int64         // response body bound (default 1 MiB)
	BreakerThreshold int           // consecutive failures before the breaker opens
	BreakerCooldown  time.Duration // open-state dwell before a half-open trial
}

// Validate enforces the fail-closed invariants and fills defaults.
func (config *Config) Validate() error {
	parsed, err := url.Parse(config.Endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return errors.New("TOS_ENDPOINT must be an HTTPS URL")
	}
	if strings.TrimSpace(config.APIKey) == "" {
		return errors.New("TOS_API_KEY must be set")
	}
	if config.Timeout == 0 {
		config.Timeout = 10 * time.Second
	}
	if config.Timeout <= 0 {
		return errors.New("TOS_TIMEOUT must be a positive duration")
	}
	if config.MaxBodyBytes == 0 {
		config.MaxBodyBytes = defaultMaxBodyBytes
	}
	if config.MaxBodyBytes < 1024 {
		return errors.New("TOS_MAX_BODY_BYTES must be at least 1024")
	}
	if config.BreakerThreshold == 0 {
		config.BreakerThreshold = 5
	}
	if config.BreakerThreshold < 1 || config.BreakerThreshold > 100 {
		return errors.New("TOS_BREAKER_THRESHOLD must be between 1 and 100")
	}
	if config.BreakerCooldown == 0 {
		config.BreakerCooldown = 30 * time.Second
	}
	if config.BreakerCooldown <= 0 {
		return errors.New("TOS_BREAKER_COOLDOWN must be a positive duration")
	}
	return nil
}
