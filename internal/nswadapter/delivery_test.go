package nswadapter

import (
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestBackoffDoublesAndCaps(t *testing.T) {
	base, max := 5*time.Second, time.Minute
	for failedAttempt, want := range map[int]time.Duration{
		1: 5 * time.Second,
		2: 10 * time.Second,
		3: 20 * time.Second,
		4: 40 * time.Second,
		5: time.Minute, // capped
		6: time.Minute,
	} {
		if got := Backoff(failedAttempt, base, max); got != want {
			t.Fatalf("Backoff(%d) = %s, want %s", failedAttempt, got, want)
		}
	}
}

func pendingDelivery(attempts, maxAttempts int) Delivery {
	return Delivery{
		DeliveryID:    "delivery-0001",
		TenantID:      "tenant-apapa-port",
		Source:        "platform_outbox",
		EventID:       "event-0001",
		EventType:     "booking.paid",
		CallReference: "booking-0001",
		ContentType:   ContentTypeJSON,
		Payload:       `{"envelopeVersion":"1.0"}`,
		PayloadSHA256: "sha256:" + strings.Repeat("ab", 32),
		Status:        StatusPending,
		Attempts:      attempts,
		MaxAttempts:   maxAttempts,
	}
}

func TestSettleAttemptDeliversOnSuccess(t *testing.T) {
	now := time.Now().UTC()
	outcome := settleAttempt(now, pendingDelivery(0, 3), nil, time.Second, time.Minute)
	if outcome.Status != StatusDelivered || outcome.Attempts != 1 || outcome.DeliveredAt == nil || outcome.LastError != "" {
		t.Fatalf("outcome = %#v, want DELIVERED at attempt 1", outcome)
	}
}

func TestSettleAttemptRetriesWithBackoff(t *testing.T) {
	now := time.Now().UTC()
	outcome := settleAttempt(now, pendingDelivery(0, 3), errors.New("connection refused"), 5*time.Second, time.Minute)
	if outcome.Status != StatusPending || outcome.Attempts != 1 {
		t.Fatalf("outcome = %#v, want PENDING retry at attempt 1", outcome)
	}
	if !outcome.NextAttemptAt.Equal(now.Add(5 * time.Second)) {
		t.Fatalf("next attempt = %s, want now+5s", outcome.NextAttemptAt)
	}
	if outcome.LastError != "connection refused" {
		t.Fatalf("last error = %q", outcome.LastError)
	}
	second := pendingDelivery(1, 3)
	outcome = settleAttempt(now, second, errors.New("timeout"), 5*time.Second, time.Minute)
	if outcome.Status != StatusPending || !outcome.NextAttemptAt.Equal(now.Add(10*time.Second)) {
		t.Fatalf("second failure outcome = %#v, want PENDING now+10s", outcome)
	}
}

func TestSettleAttemptFailsPermanentlyAfterBudget(t *testing.T) {
	now := time.Now().UTC()
	outcome := settleAttempt(now, pendingDelivery(2, 3), errors.New("status 500"), 5*time.Second, time.Minute)
	if outcome.Status != StatusFailedPermanent || outcome.Attempts != 3 {
		t.Fatalf("outcome = %#v, want FAILED_PERMANENT at attempt 3", outcome)
	}
	if outcome.LastError != "status 500" {
		t.Fatalf("permanent failure must retain the terminal error, got %q", outcome.LastError)
	}
	if outcome.DeliveredAt != nil {
		t.Fatal("permanent failure must not carry a delivery timestamp")
	}
}

func TestMarshalPortCallEventWellFormed(t *testing.T) {
	document, err := MarshalPortCallEvent(PortCallEvent{
		EventID:       "7d9e6679-7425-40de-944b-e07fc1f90ae7",
		CallReference: "CALL-LAGOS-0001",
		EventType:     "port_call.clearance_decided",
		OccurredAt:    "2026-03-01T12:00:00Z",
		TenantID:      "tenant-apapa-port",
		PayloadSHA256: "sha256:" + strings.Repeat("cd", 32),
		Payload:       `{"decision":"APPROVED","reason":"docs <verified> & \"complete\""}`,
	})
	if err != nil {
		t.Fatalf("marshal XML handoff: %v", err)
	}
	if !strings.HasPrefix(string(document), xml.Header) {
		t.Fatal("document must start with the XML declaration")
	}
	var decoded PortCallEvent
	if err := xml.Unmarshal(document, &decoded); err != nil {
		t.Fatalf("document is not well-formed XML: %v", err)
	}
	if decoded.Version != "1.0" {
		t.Fatalf("schema version = %q, want 1.0", decoded.Version)
	}
	if decoded.EventID != "7d9e6679-7425-40de-944b-e07fc1f90ae7" ||
		decoded.CallReference != "CALL-LAGOS-0001" ||
		decoded.EventType != "port_call.clearance_decided" ||
		decoded.OccurredAt != "2026-03-01T12:00:00Z" ||
		decoded.TenantID != "tenant-apapa-port" ||
		decoded.PayloadSHA256 != "sha256:"+strings.Repeat("cd", 32) {
		t.Fatalf("decoded document = %#v", decoded)
	}
	if decoded.Payload != `{"decision":"APPROVED","reason":"docs <verified> & \"complete\""}` {
		t.Fatalf("payload escaping did not round-trip: %q", decoded.Payload)
	}
}

func TestMarshalPortCallEventFailsClosedOnMissingFields(t *testing.T) {
	base := PortCallEvent{
		EventID:       "event",
		CallReference: "ref",
		EventType:     "type",
		OccurredAt:    "2026-03-01T12:00:00Z",
		TenantID:      "tenant-a",
		PayloadSHA256: "sha256:" + strings.Repeat("ab", 32),
		Payload:       "{}",
	}
	for name, mutate := range map[string]func(*PortCallEvent){
		"missing event id": func(e *PortCallEvent) { e.EventID = "" },
		"missing call ref": func(e *PortCallEvent) { e.CallReference = "" },
		"missing type":     func(e *PortCallEvent) { e.EventType = "" },
		"missing tenant":   func(e *PortCallEvent) { e.TenantID = "" },
		"bad timestamp":    func(e *PortCallEvent) { e.OccurredAt = "yesterday" },
		"missing digest":   func(e *PortCallEvent) { e.PayloadSHA256 = "" },
		"missing payload":  func(e *PortCallEvent) { e.Payload = "" },
	} {
		t.Run(name, func(t *testing.T) {
			event := base
			mutate(&event)
			if _, err := MarshalPortCallEvent(event); err == nil {
				t.Fatalf("%s must fail closed", name)
			}
		})
	}
}

func TestConfigValidateFailsClosed(t *testing.T) {
	valid := func() Config {
		return Config{
			EndpointURL:  "https://nsw.operator.ng/v1/events",
			CACertFile:   writeTestCA(t),
			Timeout:      10 * time.Second,
			MaxAttempts:  8,
			BackoffBase:  5 * time.Second,
			BackoffMax:   10 * time.Minute,
			PollInterval: 5 * time.Second,
		}
	}
	config := valid()
	if err := config.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if config.ContentType != ContentTypeJSON || config.MaxBodyBytes == 0 || config.BatchSize == 0 {
		t.Fatalf("defaults not applied: %#v", config)
	}
	for name, mutate := range map[string]func(*Config){
		"http endpoint":      func(c *Config) { c.EndpointURL = "http://nsw.operator.ng" },
		"missing endpoint":   func(c *Config) { c.EndpointURL = "" },
		"missing CA pin":     func(c *Config) { c.CACertFile = "" },
		"bad content type":   func(c *Config) { c.ContentType = "text/plain" },
		"zero timeout":       func(c *Config) { c.Timeout = 0 },
		"zero attempts":      func(c *Config) { c.MaxAttempts = 0 },
		"backoff max < base": func(c *Config) { c.BackoffMax = time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			config := valid()
			mutate(&config)
			if err := config.Validate(); err == nil {
				t.Fatalf("%s must fail closed", name)
			}
		})
	}
}
