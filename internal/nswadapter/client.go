package nswadapter

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Client posts signed NSW handoff messages to the operator endpoint. It is
// HTTPS-only with a pinned CA pool, never follows redirects, bounds the
// response body and applies a per-attempt timeout.
type Client struct {
	endpoint    string
	http        *http.Client
	maxBodyByte int64
}

// NewClient builds the outbound HTTP client from a validated config.
func NewClient(config Config) (*Client, error) {
	pool, err := config.pinnedCAPool()
	if err != nil {
		return nil, err
	}
	return &Client{
		endpoint: config.EndpointURL,
		http: &http.Client{
			Timeout: config.Timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool},
			},
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				// Fail closed: a redirect could steer signed payloads to an
				// unintended host, so redirects are surfaced as responses.
				return http.ErrUseLastResponse
			},
		},
		maxBodyByte: config.MaxBodyBytes,
	}, nil
}

// Send delivers one signed message. Any 2xx is a delivery; 409 CONFLICT is
// also accepted because the NSW replay store deduplicates on the jti, so a
// conflict proves an earlier attempt of this same event already landed.
// Every other outcome is an error and consumes an attempt.
func (client *Client) Send(ctx context.Context, body []byte, contentType, signature string) error {
	if strings.TrimSpace(signature) == "" {
		return errors.New("refusing to send an unsigned NSW message")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build NSW delivery request: %w", err)
	}
	request.Header.Set("Content-Type", contentType)
	request.Header.Set(SignatureHeader, signature)
	response, err := client.http.Do(request)
	if err != nil {
		return fmt.Errorf("NSW delivery POST: %w", err)
	}
	defer response.Body.Close()
	// The response body is bounded and discarded; only the status matters.
	if _, err := io.Copy(io.Discard, io.LimitReader(response.Body, client.maxBodyByte)); err != nil {
		return fmt.Errorf("drain NSW response: %w", err)
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}
	if response.StatusCode == http.StatusConflict {
		return nil
	}
	return fmt.Errorf("NSW endpoint rejected delivery with status %d", response.StatusCode)
}
