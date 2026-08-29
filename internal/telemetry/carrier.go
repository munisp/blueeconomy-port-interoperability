package telemetry

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
)

// FranzCarrier adapts franz-go record headers to a TextMapCarrier so the W3C
// traceparent/baggage context rides every produced record and is recovered
// on consume (OTEL_DESIGN §2 — franz-go has no auto-instrumentation in this
// platform, so carriers are manual).
type FranzCarrier struct {
	Headers *[]kgo.RecordHeader
}

// Get returns the first header value for key.
func (carrier FranzCarrier) Get(key string) string {
	if carrier.Headers == nil {
		return ""
	}
	for _, header := range *carrier.Headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

// Set appends (or replaces) the header for key.
func (carrier FranzCarrier) Set(key, value string) {
	if carrier.Headers == nil {
		return
	}
	for index, header := range *carrier.Headers {
		if header.Key == key {
			(*carrier.Headers)[index].Value = []byte(value)
			return
		}
	}
	*carrier.Headers = append(*carrier.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

// Keys lists the header keys present.
func (carrier FranzCarrier) Keys() []string {
	if carrier.Headers == nil {
		return nil
	}
	keys := make([]string, 0, len(*carrier.Headers))
	for _, header := range *carrier.Headers {
		keys = append(keys, header.Key)
	}
	return keys
}

// InjectRecordHeaders returns record headers with the live trace context
// injected. The input slice is never mutated; pass its result to the produce
// call.
func InjectRecordHeaders(ctx context.Context, headers []kgo.RecordHeader) []kgo.RecordHeader {
	carrier := FranzCarrier{Headers: &headers}
	propagator().Inject(ctx, carrier)
	return headers
}

// ExtractRecordHeaders recovers the remote trace context carried by a
// consumed record. The returned context is the parent for every consumer
// span.
func ExtractRecordHeaders(ctx context.Context, headers []kgo.RecordHeader) context.Context {
	return propagator().Extract(ctx, FranzCarrier{Headers: &headers})
}

// propagator resolves the platform propagation contract. Setup installs the
// same composite globally; carriers use the contract directly so injection
// behaves identically before/without Setup (tests, one-shot binaries) — the
// otel global default is an empty no-op composite and must not be consulted.
func propagator() propagation.TextMapPropagator {
	return Propagator()
}
