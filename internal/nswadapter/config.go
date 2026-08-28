// Package nswadapter drains NSW-relevant platform outbox events (booking
// created/paid, gate decisions, port-call clearance decisions, queue
// call-ups, cleared customs declarations) and delivers them to the NSW
// operator endpoint at-least-once as
// RS256-signed messages over pinned-CA HTTPS. Delivery state is persisted per
// event in the nsw_delivery ledger: PENDING -> DELIVERED, or
// PENDING -> FAILED_PERMANENT after the configured attempt budget. Nothing is
// silently dropped.
package nswadapter

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	ContentTypeJSON = "application/json"
	ContentTypeXML  = "application/xml"

	// SignatureHeader carries the outbound RS256 JWS, mirroring the inbound
	// NSW ingress contract.
	SignatureHeader = "X-NSW-Signature"

	defaultMaxBodyBytes = 1 << 20
)

// Config is the fail-closed adapter configuration.
type Config struct {
	EndpointURL  string        // HTTPS only, redirects are never followed
	CACertFile   string        // pinned CA bundle for the NSW endpoint (required)
	ContentType  string        // application/json (default) or application/xml
	Timeout      time.Duration // per-attempt request timeout
	MaxAttempts  int           // attempt budget before FAILED_PERMANENT
	BackoffBase  time.Duration // first retry delay; doubles per attempt
	BackoffMax   time.Duration // retry delay ceiling
	MaxBodyBytes int64         // response body bound (default 1 MiB)
	BatchSize    int           // outbox rows claimed per drain cycle
	PollInterval time.Duration // idle delay between drain cycles
}

// Validate enforces the fail-closed invariants and fills defaults.
func (config *Config) Validate() error {
	parsed, err := url.Parse(config.EndpointURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return errors.New("NSW_ENDPOINT_URL must be an HTTPS URL")
	}
	if strings.TrimSpace(config.CACertFile) == "" {
		return errors.New("NSW_CA_CERT_FILE must pin the NSW endpoint CA")
	}
	if _, err := config.pinnedCAPool(); err != nil {
		return err
	}
	if config.ContentType == "" {
		config.ContentType = ContentTypeJSON
	}
	if config.ContentType != ContentTypeJSON && config.ContentType != ContentTypeXML {
		return errors.New("NSW_CONTENT_TYPE must be application/json or application/xml")
	}
	if config.Timeout <= 0 {
		return errors.New("NSW_TIMEOUT must be a positive duration")
	}
	if config.MaxAttempts < 1 || config.MaxAttempts > 32 {
		return errors.New("NSW_MAX_ATTEMPTS must be between 1 and 32")
	}
	if config.BackoffBase <= 0 || config.BackoffMax < config.BackoffBase {
		return errors.New("NSW_BACKOFF_BASE must be positive and NSW_BACKOFF_MAX must be >= base")
	}
	if config.MaxBodyBytes == 0 {
		config.MaxBodyBytes = defaultMaxBodyBytes
	}
	if config.MaxBodyBytes < 1024 {
		return errors.New("NSW_MAX_BODY_BYTES must be at least 1024")
	}
	if config.BatchSize == 0 {
		config.BatchSize = 100
	}
	if config.BatchSize < 1 || config.BatchSize > 1000 {
		return errors.New("NSW_BATCH_SIZE must be between 1 and 1000")
	}
	if config.PollInterval <= 0 {
		return errors.New("NSW_POLL_INTERVAL must be a positive duration")
	}
	return nil
}

func (config *Config) pinnedCAPool() (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(filepath.Clean(config.CACertFile))
	if err != nil {
		return nil, fmt.Errorf("read NSW pinned CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, errors.New("NSW pinned CA file contains no PEM certificates")
	}
	return pool, nil
}
