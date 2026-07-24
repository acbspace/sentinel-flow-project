package kafkax_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/kafkax"
)

const (
	topic     = "telemetry.events.v1"
	validUUID = "6f1c4d5e-8b3a-4c2d-9e7f-1a2b3c4d5e6f"
)

func validEvent() event.Event {
	return event.Event{
		EventID:       validUUID,
		SchemaVersion: event.SchemaVersion10,
		TenantID:      "demo-tenant",
		ServiceName:   "payment-service",
		Environment:   "local",
		EventType:     "request.completed",
		Severity:      event.SeverityWarn,
		Timestamp:     event.NewTimestamp(time.Date(2026, 7, 22, 10, 30, 0, 0, time.UTC)),
		TraceID:       "0af7651916cd43dd8448eb211c80319c",
		Attributes: event.Attributes{
			"http_method":      "POST",
			"http_route":       "/demo/payments",
			"http_status_code": 402,
			"latency_ms":       125,
		},
	}
}

func TestNewRecordSerialization(t *testing.T) {
	t.Parallel()

	ev := validEvent()
	payload, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	rec := kafkax.NewRecord(topic, ev, payload)

	if rec.Topic != topic {
		t.Errorf("Topic = %q, want %q", rec.Topic, topic)
	}

	// The key is the contract that keeps one tenant+service stream ordered.
	if want := "demo-tenant:payment-service"; string(rec.Key) != want {
		t.Errorf("Key = %q, want %q", rec.Key, want)
	}

	headers := headerMap(rec)
	if got := headers[kafkax.HeaderSchemaVersion]; got != event.SchemaVersion10 {
		t.Errorf("%s header = %q, want %q", kafkax.HeaderSchemaVersion, got, event.SchemaVersion10)
	}
	if got := headers[kafkax.HeaderContentType]; got != "application/json" {
		t.Errorf("%s header = %q, want application/json", kafkax.HeaderContentType, got)
	}

	// The value must decode back into an equivalent, still-valid event.
	var decoded event.Event
	if err := json.Unmarshal(rec.Value, &decoded); err != nil {
		t.Fatalf("decode record value: %v", err)
	}
	if err := decoded.Validate(); err != nil {
		t.Errorf("decoded event failed validation: %v", err)
	}

	if decoded.EventID != ev.EventID {
		t.Errorf("EventID = %q, want %q", decoded.EventID, ev.EventID)
	}
	if decoded.TenantID != ev.TenantID {
		t.Errorf("TenantID = %q, want %q", decoded.TenantID, ev.TenantID)
	}
	if decoded.ServiceName != ev.ServiceName {
		t.Errorf("ServiceName = %q, want %q", decoded.ServiceName, ev.ServiceName)
	}
	if decoded.Severity != ev.Severity {
		t.Errorf("Severity = %q, want %q", decoded.Severity, ev.Severity)
	}
	if !decoded.Timestamp.Equal(ev.Timestamp.Time) {
		t.Errorf("Timestamp = %v, want %v", decoded.Timestamp.Time, ev.Timestamp.Time)
	}
	if decoded.TraceID != ev.TraceID {
		t.Errorf("TraceID = %q, want %q", decoded.TraceID, ev.TraceID)
	}
	if decoded.PartitionKey() != string(rec.Key) {
		t.Errorf("decoded PartitionKey() = %q, does not match record key %q", decoded.PartitionKey(), rec.Key)
	}

	// JSON numbers decode as float64; assert the attribute survives the trip.
	if got := decoded.Attributes["http_status_code"]; got != float64(402) {
		t.Errorf("attributes[http_status_code] = %v (%T), want 402", got, got)
	}
	if got := decoded.Attributes["http_route"]; got != "/demo/payments" {
		t.Errorf("attributes[http_route] = %v, want /demo/payments", got)
	}
}

func TestNewRecordKeyGroupsByTenantAndService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tenant  string
		service string
		wantKey string
	}{
		{"payment events of one tenant", "demo-tenant", "payment-service", "demo-tenant:payment-service"},
		{"order events of the same tenant use a different key", "demo-tenant", "order-service", "demo-tenant:order-service"},
		{"another tenant is isolated", "other-tenant", "payment-service", "other-tenant:payment-service"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ev := validEvent()
			ev.TenantID = tc.tenant
			ev.ServiceName = tc.service

			rec := kafkax.NewRecord(topic, ev, []byte("{}"))
			if string(rec.Key) != tc.wantKey {
				t.Errorf("Key = %q, want %q", rec.Key, tc.wantKey)
			}
		})
	}
}

func TestRecordCarrierRoundTripsTraceContext(t *testing.T) {
	t.Parallel()

	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)

	traceID, err := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	if err != nil {
		t.Fatalf("parse trace id: %v", err)
	}
	spanID, err := trace.SpanIDFromHex("b7ad6b7169203331")
	if err != nil {
		t.Fatalf("parse span id: %v", err)
	}

	producerCtx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     false,
	}))

	rec := kafkax.NewRecord(topic, validEvent(), []byte("{}"))
	propagator.Inject(producerCtx, kafkax.NewRecordCarrier(rec))

	// The W3C header must actually be on the wire, not just in memory.
	if headerMap(rec)["traceparent"] == "" {
		t.Fatalf("traceparent header was not injected; headers: %v", headerMap(rec))
	}

	// A fresh consumer-side context, as the engine would have.
	consumerCtx := propagator.Extract(context.Background(), kafkax.NewRecordCarrier(rec))
	extracted := trace.SpanContextFromContext(consumerCtx)

	if !extracted.IsValid() {
		t.Fatal("extracted span context is invalid")
	}
	if extracted.TraceID() != traceID {
		t.Errorf("TraceID = %s, want %s", extracted.TraceID(), traceID)
	}
	if extracted.SpanID() != spanID {
		t.Errorf("SpanID = %s, want %s", extracted.SpanID(), spanID)
	}
	if !extracted.IsSampled() {
		t.Error("sampling decision was not propagated")
	}
	if !extracted.IsRemote() {
		t.Error("extracted span context should be marked remote")
	}
}

func TestRecordCarrier(t *testing.T) {
	t.Parallel()

	t.Run("Get returns an empty string for a missing key", func(t *testing.T) {
		t.Parallel()

		carrier := kafkax.NewRecordCarrier(&kgo.Record{})
		if got := carrier.Get("absent"); got != "" {
			t.Errorf("Get(absent) = %q, want empty", got)
		}
	})

	t.Run("Set replaces rather than appending a duplicate key", func(t *testing.T) {
		t.Parallel()

		rec := &kgo.Record{}
		carrier := kafkax.NewRecordCarrier(rec)

		carrier.Set("traceparent", "first")
		carrier.Set("traceparent", "second")

		if got := carrier.Get("traceparent"); got != "second" {
			t.Errorf("Get(traceparent) = %q, want %q", got, "second")
		}

		// Kafka permits duplicate header keys, so a naive append would leave two
		// traceparent values and make extraction order-dependent.
		count := 0
		for _, h := range rec.Headers {
			if h.Key == "traceparent" {
				count++
			}
		}
		if count != 1 {
			t.Errorf("record carries %d traceparent headers, want 1", count)
		}
	})

	t.Run("Keys lists every header", func(t *testing.T) {
		t.Parallel()

		rec := &kgo.Record{Headers: []kgo.RecordHeader{
			{Key: "a", Value: []byte("1")},
			{Key: "b", Value: []byte("2")},
		}}

		keys := kafkax.NewRecordCarrier(rec).Keys()
		if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
			t.Errorf("Keys() = %v, want [a b]", keys)
		}
	})
}

func headerMap(rec *kgo.Record) map[string]string {
	out := make(map[string]string, len(rec.Headers))
	for _, h := range rec.Headers {
		out[h.Key] = string(h.Value)
	}
	return out
}
