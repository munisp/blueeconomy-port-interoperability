package ais

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func validReport() PositionReport {
	return PositionReport{
		MMSI:          "657123456",
		IMO:           "9074729",
		Latitude:      6.4311,
		Longitude:     3.4047,
		SpeedKnots:    12.4,
		CourseDegrees: 181.5,
		Timestamp:     time.Now().UTC().Add(-time.Minute),
	}
}

func TestValidateAcceptsWellFormedReport(t *testing.T) {
	report := validReport()
	if err := report.Validate(time.Now().UTC()); err != nil {
		t.Fatalf("valid report rejected: %v", err)
	}
}

func TestValidateRejectsMalformed(t *testing.T) {
	cases := map[string]func(*PositionReport){
		"bad mmsi":        func(r *PositionReport) { r.MMSI = "12345" },
		"coast mmsi":      func(r *PositionReport) { r.MMSI = "001234567" },
		"bad imo check":   func(r *PositionReport) { r.IMO = "9074728" },
		"nan latitude":    func(r *PositionReport) { r.Latitude = math.NaN() },
		"polar latitude":  func(r *PositionReport) { r.Latitude = 91 },
		"excess lon":      func(r *PositionReport) { r.Longitude = 180.1 },
		"nan speed":       func(r *PositionReport) { r.SpeedKnots = math.NaN() },
		"bad heading":     func(r *PositionReport) { h := 360; r.Heading = &h },
		"zero timestamp":  func(r *PositionReport) { r.Timestamp = time.Time{} },
		"future skew":     func(r *PositionReport) { r.Timestamp = time.Now().UTC().Add(time.Hour) },
		"stale timestamp": func(r *PositionReport) { r.Timestamp = time.Now().UTC().Add(-45 * 24 * time.Hour) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			report := validReport()
			mutate(&report)
			if err := report.Validate(time.Now().UTC()); !errors.Is(err, ErrInvalidReport) {
				t.Fatalf("expected ErrInvalidReport, got %v", err)
			}
		})
	}
}

func TestValidateClampsAbsurdSpeedAndCourse(t *testing.T) {
	report := validReport()
	report.SpeedKnots = 400
	report.CourseDegrees = 725
	if err := report.Validate(time.Now().UTC()); err != nil {
		t.Fatalf("clampable report rejected: %v", err)
	}
	if report.SpeedKnots != MaxSpeedKnots {
		t.Fatalf("speed not clamped: %v", report.SpeedKnots)
	}
	if report.CourseDegrees != 5 {
		t.Fatalf("course not normalized: %v", report.CourseDegrees)
	}
	report = validReport()
	report.SpeedKnots = -3
	if err := report.Validate(time.Now().UTC()); err != nil {
		t.Fatalf("negative speed rejected instead of clamped: %v", err)
	}
	if report.SpeedKnots != 0 {
		t.Fatalf("negative speed not clamped to 0: %v", report.SpeedKnots)
	}
}

func TestConfigValidateFailsClosed(t *testing.T) {
	config := Config{}
	if err := config.Validate(); err == nil {
		t.Fatal("empty config must fail")
	}
	config = Config{FeedURL: "http://feed.example/ais", APIKey: "key"}
	if err := config.Validate(); err == nil {
		t.Fatal("plain-HTTP feed URL must fail closed")
	}
	config = Config{FeedURL: "https://feed.example/ais"}
	if err := config.Validate(); err == nil {
		t.Fatal("missing API key must fail closed")
	}
	config = Config{FeedURL: "https://feed.example/ais", APIKey: "key"}
	if err := config.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if config.PollInterval != time.Minute || config.Timeout != 10*time.Second {
		t.Fatal("defaults were not applied")
	}
}

// newTestClient wires a client to a local mock server. Mock servers are a
// test facility only; production clients are built exclusively through
// NewClient from validated HTTPS configuration.
func newTestClient(server *httptest.Server) *Client {
	return &Client{endpoint: server.URL, apiKey: "test-key", http: server.Client(), maxBodyByte: 1 << 20}
}

func TestClientFetchAndAuthHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("missing bearer credential, got %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"mmsi":"657123456","latitude":6.4,"longitude":3.4,"speed_knots":10,"course_degrees":90,"timestamp":"` +
			time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano) + `"}]`))
	}))
	defer server.Close()
	reports, err := newTestClient(server).Fetch(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(reports) != 1 || reports[0].MMSI != "657123456" {
		t.Fatalf("unexpected reports: %+v", reports)
	}
}

func TestClientFetchUpstreamFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()
	if _, err := newTestClient(server).Fetch(context.Background()); !errors.Is(err, ErrUpstream) {
		t.Fatalf("expected ErrUpstream, got %v", err)
	}
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"not":"an array"}`))
	}))
	defer bad.Close()
	if _, err := newTestClient(bad).Fetch(context.Background()); !errors.Is(err, ErrUpstream) {
		t.Fatalf("expected ErrUpstream for malformed body, got %v", err)
	}
	var zero *Client
	if _, err := zero.Fetch(context.Background()); !errors.Is(err, ErrUnconfigured) {
		t.Fatalf("expected ErrUnconfigured, got %v", err)
	}
}

func TestIngestOnceValidatesAndCountsHonestly(t *testing.T) {
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[` +
			`{"mmsi":"657123456","latitude":6.4,"longitude":3.4,"speed_knots":10,"course_degrees":90,"timestamp":"` + now.Add(-time.Minute).Format(time.RFC3339Nano) + `"},` +
			`{"mmsi":"bad","latitude":6.4,"longitude":3.4,"speed_knots":10,"course_degrees":90,"timestamp":"` + now.Add(-time.Minute).Format(time.RFC3339Nano) + `"},` +
			`{"mmsi":"657123456","latitude":95,"longitude":3.4,"speed_knots":10,"course_degrees":90,"timestamp":"` + now.Add(-time.Minute).Format(time.RFC3339Nano) + `"}` +
			`]`))
	}))
	defer server.Close()
	ingester, err := NewIngester(newTestClient(server), NewMemoryStore(), time.Second)
	if err != nil {
		t.Fatalf("new ingester: %v", err)
	}
	if err := ingester.IngestOnce(context.Background()); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	stats := ingester.Stats()
	if !stats.Configured || stats.MessagesReceived != 3 || stats.MessagesAccepted != 1 || stats.MessagesRejected != 2 {
		t.Fatalf("dishonest stats: %+v", stats)
	}
	if stats.LastIngestAt == nil || stats.LastError != "" {
		t.Fatalf("expected successful ingest status, got %+v", stats)
	}
}

func TestIngestionIsIdempotent(t *testing.T) {
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"mmsi":"657123456","latitude":6.4,"longitude":3.4,"speed_knots":10,"course_degrees":90,"timestamp":"` +
			now.Add(-time.Minute).Format(time.RFC3339Nano) + `"}]`))
	}))
	defer server.Close()
	store := NewMemoryStore()
	ingester, err := NewIngester(newTestClient(server), store, time.Second)
	if err != nil {
		t.Fatalf("new ingester: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := ingester.IngestOnce(context.Background()); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}
	count, err := store.Count(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("replayed batches must not duplicate positions: count=%d err=%v", count, err)
	}
	if ingester.Stats().PersistedTotal != 1 {
		t.Fatalf("persisted_total inflated by replays: %+v", ingester.Stats())
	}
}

func TestUnconfiguredIngesterFailsClosed(t *testing.T) {
	ingester, err := NewIngester(nil, NewMemoryStore(), time.Second)
	if err != nil {
		t.Fatalf("new ingester: %v", err)
	}
	if err := ingester.IngestOnce(context.Background()); !errors.Is(err, ErrUnconfigured) {
		t.Fatalf("expected ErrUnconfigured, got %v", err)
	}
	if _, err := ingester.Latest(context.Background(), "", 10); !errors.Is(err, ErrUnconfigured) {
		t.Fatalf("expected ErrUnconfigured, got %v", err)
	}
	if ingester.Stats().Configured {
		t.Fatal("unconfigured ingester must report configured:false")
	}
}
