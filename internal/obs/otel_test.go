package obs_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/acbspace/sentinel-flow-project/internal/obs"
)

func baseConfig() obs.Config {
	return obs.Config{
		ServiceName:     "test-service",
		ServiceVersion:  "test",
		Environment:     "test",
		TracesExporter:  "none",
		MetricsExporter: "none",
		MetricInterval:  time.Minute,
	}
}

// TestTraceContextPropagatesWithoutAnExporter guards a subtle failure mode:
// disabling the exporter must not disable tracing. If "none" installed a no-op
// tracer provider, spans would carry invalid contexts, trace IDs would vanish
// from logs and stored events, and W3C propagation between services and across
// Kafka headers would silently stop working.
func TestTraceContextPropagatesWithoutAnExporter(t *testing.T) {
	ctx := context.Background()

	providers, err := obs.Setup(ctx, baseConfig())
	if err != nil {
		t.Fatalf("Setup() = %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := providers.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Shutdown() = %v", err)
		}
	})

	spanCtx, span := providers.Tracer("test").Start(ctx, "operation")
	defer span.End()

	sc := trace.SpanContextFromContext(spanCtx)
	if !sc.IsValid() {
		t.Fatal("span context is invalid; trace IDs would be empty everywhere")
	}
	if !sc.TraceID().IsValid() {
		t.Error("trace ID is invalid")
	}
	if !sc.IsSampled() {
		t.Error("span is not sampled; propagation would drop the sampling decision")
	}

	// The context must survive a real inject/extract round trip, which is what
	// the HTTP client and the Kafka record carrier both rely on.
	carrier := propagation.MapCarrier{}
	providers.Propagator.Inject(spanCtx, carrier)

	if carrier["traceparent"] == "" {
		t.Fatalf("no traceparent was injected; carrier: %v", carrier)
	}

	extracted := trace.SpanContextFromContext(
		providers.Propagator.Extract(context.Background(), carrier),
	)
	if extracted.TraceID() != sc.TraceID() {
		t.Errorf("extracted TraceID = %s, want %s", extracted.TraceID(), sc.TraceID())
	}
}

func TestSetupRejectsUnknownExporters(t *testing.T) {
	tests := []struct {
		name string
		cfg  obs.Config
	}{
		{
			name: "unknown traces exporter",
			cfg: func() obs.Config {
				c := baseConfig()
				c.TracesExporter = "carrier-pigeon"
				return c
			}(),
		},
		{
			name: "unknown metrics exporter",
			cfg: func() obs.Config {
				c := baseConfig()
				c.MetricsExporter = "smoke-signal"
				return c
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			providers, err := obs.Setup(context.Background(), tc.cfg)
			if err == nil {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = providers.Shutdown(shutdownCtx)
				t.Fatal("Setup() = nil error, want a configuration failure")
			}
		})
	}
}

func TestNoopProvidersAreUsable(t *testing.T) {
	providers := obs.NoopProviders()

	// The test helper must not panic when handed to real wiring.
	if _, err := obs.NewHTTPMetrics(providers.MeterProvider); err != nil {
		t.Errorf("NewHTTPMetrics() = %v", err)
	}
	if _, err := obs.NewKafkaMetrics(providers.MeterProvider); err != nil {
		t.Errorf("NewKafkaMetrics() = %v", err)
	}
	if _, err := obs.NewDBMetrics(providers.MeterProvider); err != nil {
		t.Errorf("NewDBMetrics() = %v", err)
	}

	_, span := providers.Tracer("test").Start(context.Background(), "operation")
	span.End()
}
