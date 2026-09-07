package ais

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// ErrUpstream marks a feed rejection or transport failure. ErrUnconfigured
// is returned by the zero-value client so unconfigured deployments fail
// closed instead of fabricating positions.
var (
	ErrUpstream     = errors.New("AIS feed request failed")
	ErrUnconfigured = errors.New("AIS feed is not configured")
)

// Client pulls position reports from the AIS feed. It is HTTPS-only with
// system roots, never follows redirects, bounds the response body and
// applies a per-request timeout.
type Client struct {
	endpoint    string
	apiKey      string
	http        *http.Client
	maxBodyByte int64
}

// NewClient builds the feed client from a validated config.
func NewClient(config Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Client{
		endpoint: config.FeedURL,
		apiKey:   config.APIKey,
		http: &http.Client{
			Timeout: config.Timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
			},
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		maxBodyByte: config.MaxBodyBytes,
	}, nil
}

// Fetch retrieves one batch of position reports. The feed contract is a
// JSON array of report objects. A non-2xx response or an undecodable body
// is an error — partial or malformed feeds are never silently accepted.
func (client *Client) Fetch(ctx context.Context) ([]PositionReport, error) {
	if client == nil || client.http == nil {
		return nil, ErrUnconfigured
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build AIS feed request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+client.apiKey)
	request.Header.Set("Accept", "application/json")
	response, err := client.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUpstream, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, client.maxBodyByte+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read response: %v", ErrUpstream, err)
	}
	if int64(len(body)) > client.maxBodyByte {
		return nil, fmt.Errorf("%w: response exceeds %d bytes", ErrUpstream, client.maxBodyByte)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("%w: feed returned status %d", ErrUpstream, response.StatusCode)
	}
	var reports []PositionReport
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reports); err != nil {
		return nil, fmt.Errorf("%w: undecodable feed body", ErrUpstream)
	}
	return reports, nil
}
