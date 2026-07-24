// Command incidents-api is the read and lifecycle HTTP surface of SentinelFlow.
// It serves incident queries, acknowledge/resolve transitions, and a read API
// over stored telemetry events, all backed by PostgreSQL.
//
// It is deliberately separate from the ingestion API: that service is a
// write-only front door, this one is read-mostly and never touches Kafka.
package main

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

	"github.com/acbspace/sentinel-flow-project/internal/alerting"
	"github.com/acbspace/sentinel-flow-project/internal/config"
	"github.com/acbspace/sentinel-flow-project/internal/httpx"
	"github.com/acbspace/sentinel-flow-project/internal/incidentapi"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
	"github.com/acbspace/sentinel-flow-project/internal/remediate"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, `{"level":"error","service":"incidents-api","msg":"fatal","error":%q}`+"\n", err.Error())
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadIncidentsAPI()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

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
	defer shutdownTelemetry(providers, log)

	httpMetrics, err := obs.NewHTTPMetrics(providers.MeterProvider)
	if err != nil {
		return fmt.Errorf("create http metrics: %w", err)
	}
	dbMetrics, err := obs.NewDBMetrics(providers.MeterProvider)
	if err != nil {
		return fmt.Errorf("create db metrics: %w", err)
	}

	pool, err := store.NewPool(ctx, store.PoolConfig{
		DSN:            cfg.PostgresDSN,
		MaxConns:       10,
		MinConns:       2,
		ConnectTimeout: 10 * time.Second,
	}, log)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() {
		pool.Close()
		log.Info("postgres pool closed")
	}()

	incidentStore := store.NewIncidentStore(pool, dbMetrics, cfg.DBTimeout)
	eventStore := store.NewEventStore(pool, dbMetrics, cfg.DBTimeout)
	notificationStore := store.NewNotificationStore(pool, dbMetrics, cfg.DBTimeout)

	// Signalling alert workflows on acknowledge/resolve is optional: with no
	// Temporal address configured the read API runs unchanged, just without the
	// fast path that stops escalation immediately (the workflow's own DB re-check
	// still catches up).
	var (
		signaler incidentapi.Signaler
		approver incidentapi.Approver
	)
	if cfg.TemporalAddress != "" {
		temporalClient, err := alerting.DialTemporal(ctx, cfg.TemporalAddress, cfg.TemporalNamespace, log)
		if err != nil {
			return fmt.Errorf("connect to temporal: %w", err)
		}
		defer temporalClient.Close()
		signaler = alerting.NewTemporalSignaler(temporalClient)
		approver = remediate.NewTemporalSignaler(temporalClient)
	}

	handler := incidentapi.NewHandler(incidentapi.Options{
		Incidents:     incidentStore,
		Events:        eventStore,
		Notifications: notificationStore,
		Remediation:   store.NewRemediationStore(pool, dbMetrics, cfg.DBTimeout),
		Signaler:      signaler,
		Approver:      approver,
		Logger:        log,
	})

	srv := httpx.NewServer(httpx.ServerOptions{
		Addr:    cfg.HTTPAddr,
		Handler: newRouter(handler, eventStore, httpMetrics, providers, log),
	})

	log.Info("incidents-api starting",
		slog.String("addr", cfg.HTTPAddr),
		slog.Bool("temporal_signalling", cfg.TemporalAddress != ""),
	)

	if err := httpx.Serve(ctx, srv, cfg.ShutdownGrace, log); err != nil {
		return fmt.Errorf("http server: %w", err)
	}

	log.Info("incidents-api stopped")
	return nil
}

func newRouter(
	handler *incidentapi.Handler,
	eventStore *store.EventStore,
	httpMetrics *obs.HTTPMetrics,
	providers *obs.Providers,
	log *slog.Logger,
) http.Handler {
	router := chi.NewRouter()

	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(middleware.Recoverer)
	router.Use(httpx.Observe(httpMetrics, log))

	router.Get("/health", httpx.HealthHandler())
	// Readiness rides on the shared pool, so the event store's Ping speaks for
	// the incident store too.
	router.Get("/ready", httpx.ReadinessHandler(log, 3*time.Second, httpx.ReadinessCheck{
		Name:  "postgres",
		Check: eventStore.Ping,
	}))

	handler.Mount(router)

	return otelhttp.NewHandler(router, "incidents-api",
		otelhttp.WithTracerProvider(providers.TracerProvider),
		otelhttp.WithMeterProvider(providers.MeterProvider),
		otelhttp.WithPropagators(providers.Propagator),
	)
}

func shutdownTelemetry(providers *obs.Providers, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := providers.Shutdown(ctx); err != nil {
		log.Error("telemetry shutdown", slog.String("error", err.Error()))
	}
}
