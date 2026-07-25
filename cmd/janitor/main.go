// Command janitor maintains the telemetry_events partitions: it creates the
// days about to be written to, drops the days past the retention horizon, and
// reports anything that has landed in the default partition.
//
// It is deliberately its own service. It is the only component in SentinelFlow
// that destroys data, which earns it a separate blast radius, a separate
// deployment cadence, and the ability to be scaled to zero — stopping retention
// without stopping ingestion — the same way remediation can be turned off
// without stopping paging.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"golang.org/x/sync/errgroup"

	"github.com/acbspace/sentinel-flow-project/internal/config"
	"github.com/acbspace/sentinel-flow-project/internal/httpx"
	"github.com/acbspace/sentinel-flow-project/internal/janitor"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, `{"level":"error","service":"janitor","msg":"fatal","error":%q}`+"\n", err.Error())
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadJanitor()
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

	// Two connections is plenty: this service runs one cycle an hour, and the
	// drain path holds a single transaction at a time.
	pool, err := store.NewPool(ctx, store.PoolConfig{
		DSN:            cfg.PostgresDSN,
		MaxConns:       2,
		MinConns:       1,
		ConnectTimeout: 10 * time.Second,
	}, log)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() {
		pool.Close()
		log.Info("postgres pool closed")
	}()

	partitions := store.NewPartitionStore(pool, dbMetrics, cfg.DBTimeout)

	maintenance := janitor.New(janitor.Options{
		Partitions: partitions,
		DayOf:      store.PartitionDay,
		NameOf:     store.PartitionName,
		Logger:     log,
		Lookahead:  cfg.Lookahead,
		Retention:  cfg.Retention,
		Now:        time.Now,
	})

	runner := janitor.NewRunner(maintenance, cfg.Interval, log)

	srv := httpx.NewServer(httpx.ServerOptions{
		Addr:    cfg.HTTPAddr,
		Handler: newRouter(pool, dbMetrics, cfg.DBTimeout, httpMetrics, log),
	})

	log.Info("janitor starting",
		slog.String("addr", cfg.HTTPAddr),
		slog.Duration("interval", cfg.Interval),
		slog.Duration("lookahead", cfg.Lookahead),
		slog.Duration("retention", cfg.Retention),
	)

	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		if err := httpx.Serve(groupCtx, srv, cfg.ShutdownGrace, log); err != nil {
			return fmt.Errorf("probe server: %w", err)
		}
		return nil
	})

	group.Go(func() error {
		if err := runner.Run(groupCtx); err != nil {
			return fmt.Errorf("janitor loop: %w", err)
		}
		return nil
	})

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	log.Info("janitor stopped")
	return nil
}

func newRouter(
	pool interface {
		Ping(context.Context) error
	},
	_ *obs.DBMetrics,
	timeout time.Duration,
	httpMetrics *obs.HTTPMetrics,
	log *slog.Logger,
) *chi.Mux {
	router := chi.NewRouter()

	router.Use(middleware.RequestID)
	router.Use(middleware.Recoverer)
	router.Use(httpx.Observe(httpMetrics, log))

	router.Get("/health", httpx.HealthHandler())
	router.Get("/ready", httpx.ReadinessHandler(log, timeout,
		httpx.ReadinessCheck{Name: "postgres", Check: pool.Ping},
	))

	return router
}

func shutdownTelemetry(providers *obs.Providers, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := providers.Shutdown(ctx); err != nil {
		log.Error("telemetry shutdown", slog.String("error", err.Error()))
	}
}
