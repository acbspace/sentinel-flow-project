// Command alerting hosts the incident alert worker. It runs a Temporal worker
// for the escalation workflow and a poller that starts one workflow per newly
// opened incident, so an incident that nobody acknowledges climbs the on-call
// escalation policy until someone owns it.
//
// It shares the database with the incident engine and incidents-api but reaches
// Kafka not at all; its only new dependency is Temporal.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/worker"
	"golang.org/x/sync/errgroup"

	"github.com/acbspace/sentinel-flow-project/internal/alerting"
	"github.com/acbspace/sentinel-flow-project/internal/config"
	"github.com/acbspace/sentinel-flow-project/internal/httpx"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
	"github.com/acbspace/sentinel-flow-project/internal/oncall"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, `{"level":"error","service":"alerting","msg":"fatal","error":%q}`+"\n", err.Error())
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAlerting()
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
	alertingMetrics, err := obs.NewAlertingMetrics(providers.MeterProvider)
	if err != nil {
		return fmt.Errorf("create alerting metrics: %w", err)
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
	notificationStore := store.NewNotificationStore(pool, dbMetrics, cfg.DBTimeout)

	policy, err := loadPolicy(cfg.PolicyPath)
	if err != nil {
		return fmt.Errorf("load escalation policy: %w", err)
	}

	temporalClient, err := alerting.DialTemporal(ctx, cfg.TemporalAddress, cfg.TemporalNamespace, log)
	if err != nil {
		return fmt.Errorf("connect to temporal: %w", err)
	}
	defer temporalClient.Close()

	notifier := alerting.NewNotifier(notificationStore, cfg.WebhookURL, cfg.WebhookTimeout, log)
	activities := alerting.NewActivities(incidentStore, notifier, log)

	w := worker.New(temporalClient, cfg.TaskQueue, worker.Options{})
	w.RegisterWorkflow(alerting.IncidentAlertWorkflow)
	w.RegisterActivity(activities)

	starter := alerting.NewStarter(alerting.StarterOptions{
		Incidents: incidentStore,
		Workflows: alerting.NewTemporalStarter(temporalClient, cfg.TaskQueue),
		Policy:    policy,
		Metrics:   alertingMetrics,
		Logger:    log,
		Interval:  cfg.PollInterval,
		BatchSize: cfg.BatchSize,
	})

	srv := httpx.NewServer(httpx.ServerOptions{
		Addr:    cfg.HTTPAddr,
		Handler: newRouter(pool, cfg.DBTimeout, httpMetrics, log),
	})

	log.Info("alerting starting",
		slog.String("addr", cfg.HTTPAddr),
		slog.String("temporal_address", cfg.TemporalAddress),
		slog.String("task_queue", cfg.TaskQueue),
		slog.Int("policy_levels", len(policy.Levels)),
		slog.Duration("poll_interval", cfg.PollInterval),
		slog.Bool("webhook_configured", cfg.WebhookURL != ""),
	)

	// The worker, the poller and the probe server share a lifetime: if any stops,
	// the group context is cancelled and the others unwind too.
	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		if err := httpx.Serve(groupCtx, srv, cfg.ShutdownGrace, log); err != nil {
			return fmt.Errorf("probe server: %w", err)
		}
		return nil
	})

	group.Go(func() error {
		if err := w.Start(); err != nil {
			return fmt.Errorf("start temporal worker: %w", err)
		}
		<-groupCtx.Done()
		w.Stop() // blocks until in-flight activities drain
		log.Info("temporal worker stopped")
		return nil
	})

	group.Go(func() error {
		if err := starter.Run(groupCtx); err != nil {
			return fmt.Errorf("alert starter: %w", err)
		}
		return nil
	})

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	log.Info("alerting stopped")
	return nil
}

func newRouter(pool *pgxpool.Pool, dbTimeout time.Duration, httpMetrics *obs.HTTPMetrics, log *slog.Logger) http.Handler {
	router := chi.NewRouter()

	router.Use(middleware.RequestID)
	router.Use(middleware.Recoverer)
	router.Use(httpx.Observe(httpMetrics, log))

	router.Get("/health", httpx.HealthHandler())
	router.Get("/ready", httpx.ReadinessHandler(log, 3*time.Second, httpx.ReadinessCheck{
		Name:  "postgres",
		Check: poolPing(pool, dbTimeout),
	}))

	return router
}

// poolPing adapts a pool into a readiness check. Temporal connectivity is proven
// at startup by the dial-retry loop; readiness here speaks for the database.
func poolPing(pool *pgxpool.Pool, timeout time.Duration) func(context.Context) error {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			return fmt.Errorf("ping postgres: %w", err)
		}
		return nil
	}
}

// loadPolicy reads the escalation policy from a file, or falls back to the one
// embedded in the binary.
func loadPolicy(path string) (oncall.EscalationPolicy, error) {
	if path == "" {
		return oncall.DefaultPolicy()
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return oncall.EscalationPolicy{}, fmt.Errorf("read policy file %s: %w", path, err)
	}
	return oncall.ParsePolicy(data)
}

func shutdownTelemetry(providers *obs.Providers, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := providers.Shutdown(ctx); err != nil {
		log.Error("telemetry shutdown", slog.String("error", err.Error()))
	}
}
