package demo

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/acbspace/sentinel-flow-project/internal/config"
	"github.com/acbspace/sentinel-flow-project/internal/httpx"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
)

// ServiceSpec describes what makes one demo service different from the other.
// Everything else — telemetry setup, routing, probes, shutdown — is identical,
// so the two binaries share this bootstrap instead of duplicating it.
type ServiceSpec struct {
	Route                   string
	IDPrefix                string
	SuccessStatus           int
	FailureStatus           int
	DownstreamFailureStatus int
	// DownstreamPath is called on config.DownstreamURL when that URL is set.
	DownstreamPath string
}

// Run starts a demo service and blocks until SIGINT or SIGTERM.
func Run(cfg config.DemoService, spec ServiceSpec) error {
	log := obs.NewLogger(os.Stdout, cfg.Observability.ServiceName, cfg.Observability.Environment, cfg.Observability.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	providers, err := obs.Setup(ctx, obs.Config{
		ServiceName:     cfg.Observability.ServiceName,
		ServiceVersion:  cfg.Observability.ServiceVersion,
		Environment:     cfg.Observability.Environment,
		TracesExporter:  cfg.Observability.TracesExporter,
		MetricsExporter: cfg.Observability.MetricsExporter,
		OTLPEndpoint:    cfg.Observability.OTLPEndpoint,
		OTLPInsecure:    cfg.Observability.OTLPInsecure,
		MetricInterval:  cfg.Observability.MetricInterval,
	})
	if err != nil {
		return fmt.Errorf("set up telemetry: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := providers.Shutdown(shutdownCtx); err != nil {
			log.Error("telemetry shutdown", slog.String("error", err.Error()))
		}
	}()

	httpMetrics, err := obs.NewHTTPMetrics(providers.MeterProvider)
	if err != nil {
		return fmt.Errorf("create http metrics: %w", err)
	}

	var downstream Downstream
	if cfg.DownstreamURL != "" && spec.DownstreamPath != "" {
		downstream = HTTPDownstream(cfg.DownstreamURL, spec.DownstreamPath, providers, cfg.EmitTimeout)
	}

	handler := NewHandler(HandlerConfig{
		ServiceName:             cfg.Observability.ServiceName,
		Route:                   spec.Route,
		TenantID:                cfg.TenantID,
		Environment:             cfg.Observability.Environment,
		IDPrefix:                spec.IDPrefix,
		SuccessStatus:           spec.SuccessStatus,
		FailureStatus:           spec.FailureStatus,
		DownstreamFailureStatus: spec.DownstreamFailureStatus,
		Simulator: NewSimulator(SimulatorConfig{
			FailureRate: cfg.FailureRate,
			MinLatency:  cfg.MinLatency,
			MaxLatency:  cfg.MaxLatency,
		}),
		Sink:       NewEmitter(cfg.IngestionURL, providers, log, cfg.EmitTimeout),
		Downstream: downstream,
		Logger:     log,
	})

	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(middleware.Recoverer)
	router.Use(httpx.Observe(httpMetrics, log))

	router.Get("/health", httpx.HealthHandler())
	router.Get("/ready", httpx.ReadinessHandler(log, 3*time.Second))
	router.Post(spec.Route, handler.ServeHTTP)

	instrumented := otelhttp.NewHandler(router, cfg.Observability.ServiceName,
		otelhttp.WithTracerProvider(providers.TracerProvider),
		otelhttp.WithMeterProvider(providers.MeterProvider),
		otelhttp.WithPropagators(providers.Propagator),
	)

	srv := httpx.NewServer(httpx.ServerOptions{
		Addr:    cfg.HTTPAddr,
		Handler: instrumented,
		// A simulated request sleeps for up to MaxLatency and then emits an
		// event, so the write timeout must cover both.
		WriteTimeout: cfg.MaxLatency + cfg.EmitTimeout + 15*time.Second,
		ReadTimeout:  cfg.MaxLatency + 15*time.Second,
	})

	log.Info(cfg.Observability.ServiceName+" starting",
		slog.String("addr", cfg.HTTPAddr),
		slog.String("route", spec.Route),
		slog.String("ingestion_api", cfg.IngestionURL),
		slog.Float64("failure_rate", cfg.FailureRate),
		slog.Bool("downstream_enabled", downstream != nil),
	)

	if err := httpx.Serve(ctx, srv, cfg.ShutdownGrace, log); err != nil {
		return fmt.Errorf("http server: %w", err)
	}

	log.Info(cfg.Observability.ServiceName + " stopped")
	return nil
}

// interface assertion: chi's Post takes an http.HandlerFunc.
var _ http.HandlerFunc = (&Handler{}).ServeHTTP
