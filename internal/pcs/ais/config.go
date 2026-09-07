package ais

import (
	"errors"
	"net/url"
	"strings"
	"time"
)

const defaultMaxBodyBytes = 4 << 20

// Config is the fail-closed AIS feed configuration. The feed is HTTPS-only
// and authenticated with a bearer API key sourced exclusively from the
// environment (AIS_FEED_URL, AIS_API_KEY, AIS_POLL_INTERVAL, AIS_TIMEOUT).
type Config struct {
	FeedURL      string        // HTTPS only, redirects are never followed
	APIKey       string        // bearer credential, env-only, never logged
	PollInterval time.Duration // delay between ingestion cycles
	Timeout      time.Duration // per-request feed timeout
	MaxBodyBytes int64         // response body bound (default 4 MiB)
}

// Validate enforces the fail-closed invariants and fills defaults.
func (config *Config) Validate() error {
	parsed, err := url.Parse(config.FeedURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return errors.New("AIS_FEED_URL must be an HTTPS URL")
	}
	if strings.TrimSpace(config.APIKey) == "" {
		return errors.New("AIS_API_KEY must be set")
	}
	if config.PollInterval == 0 {
		config.PollInterval = time.Minute
	}
	if config.PollInterval < 5*time.Second {
		return errors.New("AIS_POLL_INTERVAL must be at least 5s")
	}
	if config.Timeout == 0 {
		config.Timeout = 10 * time.Second
	}
	if config.Timeout <= 0 {
		return errors.New("AIS_TIMEOUT must be a positive duration")
	}
	if config.MaxBodyBytes == 0 {
		config.MaxBodyBytes = defaultMaxBodyBytes
	}
	if config.MaxBodyBytes < 1024 {
		return errors.New("AIS_MAX_BODY_BYTES must be at least 1024")
	}
	return nil
}
