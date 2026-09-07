package tos

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfigValidateFailsClosed(t *testing.T) {
	if err := (&Config{}).Validate(); err == nil {
		t.Fatal("empty config must fail")
	}
	config := Config{Endpoint: "http://tos.internal", APIKey: "k"}
	if err := config.Validate(); err == nil {
		t.Fatal("plain-HTTP endpoint must fail closed")
	}
	config = Config{Endpoint: "https://tos.internal", APIKey: "k"}
	if err := config.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if config.BreakerThreshold != 5 || config.BreakerCooldown != 30*time.Second {
		t.Fatal("breaker defaults not applied")
	}
}

// newTestClient wires a client to a local mock TOS. Mock servers are a
// test facility only; production clients are built via NewClient.
func newTestClient(server *httptest.Server, threshold int, cooldown time.Duration) *Client {
	return &Client{
		endpoint:    server.URL,
		apiKey:      "test-key",
		http:        server.Client(),
		maxBodyByte: 1 << 20,
		threshold:   threshold,
		cooldown:    cooldown,
		now:         func() time.Time { return time.Now().UTC() },
	}
}

func TestBerthOccupancyHappyPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/berths" || r.URL.Query().Get("port_code") != "APAPA" {
			t.Errorf("unexpected request %s", r.URL)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("missing bearer credential")
		}
		_, _ = w.Write([]byte(`[{"berth_id":"AP-1","terminal":"APM Apapa","port_code":"APAPA","state":"OCCUPIED","vessel_imo":"9074729","since":"` +
			time.Now().UTC().Format(time.RFC3339Nano) + `"}]`))
	}))
	defer server.Close()
	occupancies, err := newTestClient(server, 3, time.Second).BerthOccupancy(context.Background(), "APAPA")
	if err != nil {
		t.Fatalf("occupancy: %v", err)
	}
	if len(occupancies) != 1 || occupancies[0].State != "OCCUPIED" {
		t.Fatalf("unexpected occupancies: %+v", occupancies)
	}
}

func TestBerthAssignmentNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	_, err := newTestClient(server, 3, time.Second).BerthAssignment(context.Background(), "9074729")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestUpstreamSchemaValidationRejectsGarbage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"berth_id":"  ","terminal":"T","port_code":"APAPA","state":"WEIRD","since":"2024-01-01T00:00:00Z"}]`))
	}))
	defer server.Close()
	_, err := newTestClient(server, 3, time.Second).BerthOccupancy(context.Background(), "APAPA")
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("expected ErrMalformed, got %v", err)
	}
}

func TestCircuitBreakerOpensAndFailsFast(t *testing.T) {
	var calls int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&calls, 1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()
	client := newTestClient(server, 3, 50*time.Millisecond)
	for i := 0; i < 3; i++ {
		if _, err := client.BerthOccupancy(context.Background(), "APAPA"); !errors.Is(err, ErrUpstream) {
			t.Fatalf("attempt %d: expected ErrUpstream, got %v", i, err)
		}
	}
	if state := client.CircuitState(); state != "open" {
		t.Fatalf("breaker should be open, got %q", state)
	}
	if _, err := client.BerthOccupancy(context.Background(), "APAPA"); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected fail-fast ErrCircuitOpen, got %v", err)
	}
	if got := atomic.LoadInt64(&calls); got != 3 {
		t.Fatalf("open breaker must not hit upstream, calls=%d", got)
	}
	// After cooldown the breaker admits exactly one half-open trial.
	time.Sleep(60 * time.Millisecond)
	if state := client.CircuitState(); state != "half-open" {
		t.Fatalf("expected half-open, got %q", state)
	}
	if _, err := client.BerthOccupancy(context.Background(), "APAPA"); !errors.Is(err, ErrUpstream) {
		t.Fatalf("half-open trial should reach upstream, got %v", err)
	}
	if got := atomic.LoadInt64(&calls); got != 4 {
		t.Fatalf("half-open trial must reach upstream exactly once, calls=%d", got)
	}
}

func TestUnconfiguredClientFailsClosed(t *testing.T) {
	var zero *Client
	if _, err := zero.BerthOccupancy(context.Background(), "APAPA"); !errors.Is(err, ErrUnconfigured) {
		t.Fatalf("expected ErrUnconfigured, got %v", err)
	}
	if _, err := zero.BerthAssignment(context.Background(), "9074729"); !errors.Is(err, ErrUnconfigured) {
		t.Fatalf("expected ErrUnconfigured, got %v", err)
	}
	if zero.CircuitState() != "unconfigured" {
		t.Fatal("unconfigured client must report unconfigured circuit")
	}
}

func TestRequestRejectsBlankPortCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("request must not reach upstream")
	}))
	defer server.Close()
	if _, err := newTestClient(server, 3, time.Second).BerthOccupancy(context.Background(), "  "); err == nil ||
		!strings.Contains(err.Error(), "port_code") {
		t.Fatalf("blank port code must be rejected client-side, got %v", err)
	}
}
