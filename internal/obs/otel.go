package obs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	metricapi "go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	traceapi "go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// Config selects the exporters and identifies the service in telemetry.
type Config struct {
	ServiceName     string
	ServiceVersion  string
	Environment     string
	TracesExporter  string // stdout, otlp or none
	MetricsExporter string // stdout, otlp or none
	OTLPEndpoint    string
	OTLPInsecure    bool
	MetricInterval  time.Duration
}

// Providers owns the configured telemetry pipeline for one process.
//
// The providers are handed to the components that need them rather than read
// from OTel's globals, so a test can supply no-op providers without touching
// process wide state. The globals are still set because third party
// instrumentation libraries fall back to them.
type Providers struct {
	TracerProvider traceapi.TracerProvider
	MeterProvider  metricapi.MeterProvider
	Propagator     propagation.TextMapPropagator

	shutdownFuncs []func(context.Context) error
}

// Tracer returns a named tracer from the configured provider.
func (p *Providers) Tracer(name string) traceapi.Tracer {
	return p.TracerProvider.Tracer(name)
}

// Shutdown flushes and stops every exporter. It is safe to call once.
func (p *Providers) Shutdown(ctx context.Context) error {
	var errs []error
	for i := len(p.shutdownFuncs) - 1; i >= 0; i-- {
		if err := p.shutdownFuncs[i](ctx); err != nil {
			errs = append(errs, err)
		}
	}
	p.shutdownFuncs = nil
	return errors.Join(errs...)
}

// Setup builds the tracer and meter providers described by cfg.
//
// The stdout exporters are the default so the pipeline is visible with no extra
// infrastructure. Setting OTEL_EXPORTER_OTLP_ENDPOINT and switching the
// exporters to "otlp" points the same code at an OpenTelemetry Collector; no
// application change is needed to adopt one.
//
// Note that "none" disables *export*, not tracing: spans are still created and
// trace context still propagates, so trace IDs appear in logs and in stored
// events regardless of where (or whether) spans are shipped.
func Setup(ctx context.Context, cfg Config) (*Providers, error) {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
			semconv.DeploymentEnvironmentNameKey.String(cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("build otel resource: %w", err)
	}

	providers := &Providers{
		Propagator: propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
		TracerProvider: tracenoop.NewTracerProvider(),
		MeterProvider:  metricnoop.NewMeterProvider(),
	}

	tp, err := newTracerProvider(ctx, cfg, res)
	if err != nil {
		return nil, err
	}
	if tp != nil {
		providers.TracerProvider = tp
		providers.shutdownFuncs = append(providers.shutdownFuncs, tp.Shutdown)
	}

	mp, err := newMeterProvider(ctx, cfg, res)
	if err != nil {
		// Do not leak the tracer pipeline if the metric pipeline fails.
		_ = providers.Shutdown(ctx)
		return nil, err
	}
	if mp != nil {
		providers.MeterProvider = mp
		providers.shutdownFuncs = append(providers.shutdownFuncs, mp.Shutdown)
	}

	otel.SetTracerProvider(providers.TracerProvider)
	otel.SetMeterProvider(providers.MeterProvider)
	otel.SetTextMapPropagator(providers.Propagator)

	return providers, nil
}

func newTracerProvider(ctx context.Context, cfg Config, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	var exporter sdktrace.SpanExporter

	switch normalizeExporter(cfg.TracesExporter) {
	case "none":
		// Still a real SDK provider, just one with nowhere to send spans.
		//
		// A no-op provider would be wrong here: it hands out invalid span
		// contexts, so trace IDs would be empty and W3C context would stop
		// propagating between services and across Kafka headers. Sampling and
		// context propagation must keep working even when nothing is exported,
		// because "do not export" and "do not trace" are different requests.
		return sdktrace.NewTracerProvider(sdktrace.WithResource(res)), nil
	case "stdout":
		exp, err := stdouttrace.New(stdouttrace.WithWriter(os.Stdout))
		if err != nil {
			return nil, fmt.Errorf("create stdout trace exporter: %w", err)
		}
		exporter = exp
	case "otlp":
		opts := []otlptracehttp.Option{}
		if cfg.OTLPEndpoint != "" {
			opts = append(opts, otlptracehttp.WithEndpointURL(cfg.OTLPEndpoint))
		}
		if cfg.OTLPInsecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		exp, err := otlptracehttp.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("create otlp trace exporter: %w", err)
		}
		exporter = exp
	default:
		return nil, fmt.Errorf("unsupported traces exporter %q (want stdout, otlp or none)", cfg.TracesExporter)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	), nil
}

func newMeterProvider(ctx context.Context, cfg Config, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	var reader sdkmetric.Reader

	interval := cfg.MetricInterval
	if interval <= 0 {
		interval = 60 * time.Second
	}

	switch normalizeExporter(cfg.MetricsExporter) {
	case "none":
		return nil, nil
	case "stdout":
		exp, err := stdoutmetric.New(stdoutmetric.WithWriter(os.Stdout))
		if err != nil {
			return nil, fmt.Errorf("create stdout metric exporter: %w", err)
		}
		reader = sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(interval))
	case "otlp":
		opts := []otlpmetrichttp.Option{}
		if cfg.OTLPEndpoint != "" {
			opts = append(opts, otlpmetrichttp.WithEndpointURL(cfg.OTLPEndpoint))
		}
		if cfg.OTLPInsecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}
		exp, err := otlpmetrichttp.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("create otlp metric exporter: %w", err)
		}
		reader = sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(interval))
	default:
		return nil, fmt.Errorf("unsupported metrics exporter %q (want stdout, otlp or none)", cfg.MetricsExporter)
	}

	return sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	), nil
}

func normalizeExporter(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return "none"
	}
	return v
}

// NoopProviders returns providers that discard all telemetry. Tests use this to
// exercise production wiring without configuring exporters.
func NoopProviders() *Providers {
	return &Providers{
		TracerProvider: tracenoop.NewTracerProvider(),
		MeterProvider:  metricnoop.NewMeterProvider(),
		Propagator: propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	}
}
