package tos

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Typed adapter errors. Handlers map them to honest status codes: 503 for
// unconfigured/open-circuit/unavailable upstream, 404 for unknown vessels,
// 502 for upstream rejections.
var (
	ErrUnconfigured = errors.New("TOS endpoint is not configured")
	ErrCircuitOpen  = errors.New("TOS circuit breaker is open")
	ErrTimeout      = errors.New("TOS request timed out")
	ErrUpstream     = errors.New("TOS endpoint rejected the request")
	ErrNotFound     = errors.New("TOS has no record for the vessel")
	ErrMalformed    = errors.New("TOS response failed schema validation")
)

// BerthOccupancy is one berth's operational state as reported by the TOS.
type BerthOccupancy struct {
	BerthID    string    `json:"berth_id"`
	Terminal   string    `json:"terminal"`
	PortCode   string    `json:"port_code"`
	State      string    `json:"state"` // OCCUPIED, RESERVED, FREE
	VesselIMO  string    `json:"vessel_imo,omitempty"`
	VesselMMSI string    `json:"vessel_mmsi,omitempty"`
	Since      time.Time `json:"since"`
}

// BerthAssignment is the TOS berth plan for one vessel.
type BerthAssignment struct {
	BerthID    string    `json:"berth_id"`
	Terminal   string    `json:"terminal"`
	PortCode   string    `json:"port_code"`
	VesselIMO  string    `json:"vessel_imo"`
	VesselMMSI string    `json:"vessel_mmsi,omitempty"`
	Operation  string    `json:"operation"` // DISCHARGE, LOAD, BOTH
	ETA        time.Time `json:"eta"`
	ETD        time.Time `json:"etd"`
}

var berthStatePattern = map[string]bool{"OCCUPIED": true, "RESERVED": true, "FREE": true}

// validateText enforces canonical bounded text on upstream fields; the TOS
// is an external system, so its payload is never trusted blindly.
func validateText(value string, max int) bool {
	return value != "" && strings.TrimSpace(value) == value && len(value) <= max
}

func (occupancy BerthOccupancy) validate() error {
	if !validateText(occupancy.BerthID, 64) || !validateText(occupancy.Terminal, 128) || !validateText(occupancy.PortCode, 8) {
		return fmt.Errorf("%w: berth occupancy identity fields are invalid", ErrMalformed)
	}
	if !berthStatePattern[occupancy.State] {
		return fmt.Errorf("%w: berth state %q is unknown", ErrMalformed, occupancy.State)
	}
	return nil
}

func (assignment BerthAssignment) validate() error {
	if !validateText(assignment.BerthID, 64) || !validateText(assignment.Terminal, 128) || !validateText(assignment.PortCode, 8) {
		return fmt.Errorf("%w: berth assignment identity fields are invalid", ErrMalformed)
	}
	if !validateText(assignment.VesselIMO, 7) || len(assignment.VesselIMO) != 7 {
		return fmt.Errorf("%w: berth assignment vessel_imo is invalid", ErrMalformed)
	}
	return nil
}

// Client queries the TOS over pinned-HTTPS semantics: HTTPS-only, no
// redirects, bounded bodies, per-request timeout and a circuit breaker.
type Client struct {
	endpoint    string
	apiKey      string
	http        *http.Client
	maxBodyByte int64

	mu         sync.Mutex
	threshold  int
	cooldown   time.Duration
	failures   int
	openedAt   time.Time
	open       bool
	halfOpenIn bool
	now        func() time.Time
}

// NewClient builds the TOS client from a validated config.
func NewClient(config Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Client{
		endpoint:    strings.TrimRight(config.Endpoint, "/"),
		apiKey:      config.APIKey,
		maxBodyByte: config.MaxBodyBytes,
		threshold:   config.BreakerThreshold,
		cooldown:    config.BreakerCooldown,
		now:         func() time.Time { return time.Now().UTC() },
		http: &http.Client{
			Timeout: config.Timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
			},
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// CircuitState reports the honest breaker state for the status surface:
// "closed", "open" or "half-open".
func (client *Client) CircuitState() string {
	if client == nil {
		return "unconfigured"
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if !client.open {
		return "closed"
	}
	if client.now().Sub(client.openedAt) >= client.cooldown {
		return "half-open"
	}
	return "open"
}

// allow enforces the breaker: open circuits fail fast until the cooldown
// elapses, then admit exactly one half-open trial request.
func (client *Client) allow() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if !client.open {
		return nil
	}
	if client.now().Sub(client.openedAt) < client.cooldown {
		return ErrCircuitOpen
	}
	if client.halfOpenIn {
		return ErrCircuitOpen
	}
	client.halfOpenIn = true
	return nil
}

func (client *Client) record(err error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if err == nil {
		client.failures = 0
		client.open = false
		client.halfOpenIn = false
		return
	}
	client.halfOpenIn = false
	client.failures++
	if client.failures >= client.threshold {
		client.open = true
		client.openedAt = client.now()
	}
}

// get performs one bounded GET against the TOS and decodes into out.
func (client *Client) get(ctx context.Context, path string, out any) error {
	if client == nil || client.http == nil {
		return ErrUnconfigured
	}
	if err := client.allow(); err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.endpoint+path, nil)
	if err != nil {
		return fmt.Errorf("build TOS request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+client.apiKey)
	request.Header.Set("Accept", "application/json")
	response, err := client.http.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "Client.Timeout") {
			client.record(err)
			return fmt.Errorf("%w: %v", ErrTimeout, err)
		}
		client.record(err)
		return fmt.Errorf("%w: %v", ErrUpstream, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, client.maxBodyByte+1))
	if err != nil {
		client.record(err)
		return fmt.Errorf("%w: read response: %v", ErrUpstream, err)
	}
	switch {
	case response.StatusCode == http.StatusNotFound:
		client.record(nil) // a well-formed 404 is not an upstream failure
		return ErrNotFound
	case response.StatusCode < 200 || response.StatusCode >= 300:
		client.record(errors.New("upstream status"))
		return fmt.Errorf("%w: status %d", ErrUpstream, response.StatusCode)
	case int64(len(body)) > client.maxBodyByte:
		client.record(errors.New("oversize body"))
		return fmt.Errorf("%w: response exceeds %d bytes", ErrMalformed, client.maxBodyByte)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		client.record(err)
		return fmt.Errorf("%w: undecodable body", ErrMalformed)
	}
	client.record(nil)
	return nil
}

// BerthOccupancy lists the berth states for a port code.
func (client *Client) BerthOccupancy(ctx context.Context, portCode string) ([]BerthOccupancy, error) {
	if strings.TrimSpace(portCode) == "" || len(portCode) > 8 {
		return nil, fmt.Errorf("%w: port_code query parameter is required", ErrMalformed)
	}
	var occupancies []BerthOccupancy
	if err := client.get(ctx, "/berths?port_code="+portCode, &occupancies); err != nil {
		return nil, err
	}
	for _, occupancy := range occupancies {
		if err := occupancy.validate(); err != nil {
			return nil, err
		}
	}
	return occupancies, nil
}

// BerthAssignment returns the current berth plan for a vessel IMO.
func (client *Client) BerthAssignment(ctx context.Context, imo string) (*BerthAssignment, error) {
	if len(imo) != 7 {
		return nil, fmt.Errorf("%w: imo must be seven digits", ErrMalformed)
	}
	var assignment BerthAssignment
	if err := client.get(ctx, "/vessels/"+imo+"/berth-assignment", &assignment); err != nil {
		return nil, err
	}
	if err := assignment.validate(); err != nil {
		return nil, err
	}
	return &assignment, nil
}
