package ais

import (
	"context"
	"sync"
	"time"
)

// Stats is the honest ingestion status surface reported by
// /v1/pcs/ais/status. When the feed is unconfigured, Configured is false
// and every counter is zero — no fabricated activity.
type Stats struct {
	Configured       bool       `json:"configured"`
	IngestRuns       int64      `json:"ingest_runs"`
	MessagesReceived int64      `json:"messages_received"`
	MessagesAccepted int64      `json:"messages_accepted"`
	MessagesRejected int64      `json:"messages_rejected"`
	PersistedTotal   int64      `json:"persisted_total"`
	LastIngestAt     *time.Time `json:"last_ingest_at,omitempty"`
	LastError        string     `json:"last_error,omitempty"`
}

// Ingester polls the feed, validates every message fail closed and
// persists the accepted reports idempotently. A nil client means the feed
// is unconfigured: the ingester stays honest (configured:false) and
// IngestOnce fails closed.
type Ingester struct {
	client *Client
	store  Store
	poll   time.Duration
	now    func() time.Time

	mu    sync.Mutex
	stats Stats
}

func NewIngester(client *Client, store Store, pollInterval time.Duration) (*Ingester, error) {
	if store == nil {
		return nil, ErrUnconfigured
	}
	if pollInterval <= 0 {
		pollInterval = time.Minute
	}
	return &Ingester{
		client: client,
		store:  store,
		poll:   pollInterval,
		now:    func() time.Time { return time.Now().UTC() },
		stats:  Stats{Configured: client != nil},
	}, nil
}

// IngestOnce performs one poll cycle: fetch, validate, persist. Malformed
// messages are rejected and counted individually; an upstream failure
// aborts the cycle and is surfaced in Stats.LastError.
func (ingester *Ingester) IngestOnce(ctx context.Context) error {
	if ingester.client == nil {
		return ErrUnconfigured
	}
	reports, err := ingester.client.Fetch(ctx)
	ingester.mu.Lock()
	ingester.stats.IngestRuns++
	if err != nil {
		ingester.stats.LastError = err.Error()
		ingester.mu.Unlock()
		return err
	}
	ingester.stats.LastError = ""
	ingester.stats.MessagesReceived += int64(len(reports))
	ingester.mu.Unlock()
	accepted := make([]PositionReport, 0, len(reports))
	var rejected int64
	for index := range reports {
		reports[index].MMSI = sanitizeMMSI(reports[index].MMSI)
		if err := reports[index].Validate(ingester.now()); err != nil {
			rejected++
			continue
		}
		accepted = append(accepted, reports[index])
	}
	fresh, err := ingester.store.UpsertPositions(ctx, accepted)
	if err != nil {
		ingester.mu.Lock()
		ingester.stats.LastError = err.Error()
		ingester.mu.Unlock()
		return err
	}
	count, err := ingester.store.Count(ctx)
	if err != nil {
		return err
	}
	ingester.mu.Lock()
	ingester.stats.MessagesAccepted += int64(len(accepted))
	ingester.stats.MessagesRejected += rejected
	ingester.stats.PersistedTotal = count
	_ = fresh // upsert idempotency: replays do not inflate persisted_total
	now := ingester.now()
	ingester.stats.LastIngestAt = &now
	ingester.mu.Unlock()
	return nil
}

// Run polls until ctx is cancelled. It is started by main only when the
// feed is configured.
func (ingester *Ingester) Run(ctx context.Context) {
	if ingester.client == nil {
		return
	}
	ticker := time.NewTicker(ingester.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = ingester.IngestOnce(ctx)
		}
	}
}

// Stats returns a copy of the current ingestion status.
func (ingester *Ingester) Stats() Stats {
	ingester.mu.Lock()
	defer ingester.mu.Unlock()
	return ingester.stats
}

// Latest exposes the newest persisted positions for the API surface.
func (ingester *Ingester) Latest(ctx context.Context, mmsi string, limit int) ([]PositionReport, error) {
	if ingester.client == nil {
		return nil, ErrUnconfigured
	}
	return ingester.store.Latest(ctx, mmsi, limit)
}
