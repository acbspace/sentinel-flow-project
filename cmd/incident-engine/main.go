// Command incident-engine consumes telemetry events from Kafka and stores the
// normalized records in PostgreSQL.
//
// Correlation rules are a later milestone; this binary's only job today is to
// move events from the log into the database exactly once per event ID.
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
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/acbspace/sentinel-flow-project/internal/config"
	"github.com/acbspace/sentinel-flow-project/internal/correlate"
	"github.com/acbspace/sentinel-flow-project/internal/engine"
	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/httpx"
	"github.com/acbspace/sentinel-flow-project/internal/kafkax"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, `{"level":"error","service":"incident-engine","msg":"fatal","error":%q}`+"\n", err.Error())
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadIncidentEngine()
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
	kafkaMetrics, err := obs.NewKafkaMetrics(providers.MeterProvider)
	if err != nil {
		return fmt.Errorf("create kafka metrics: %w", err)
	}
	dbMetrics, err := obs.NewDBMetrics(providers.MeterProvider)
	if err != nil {
		return fmt.Errorf("create db metrics: %w", err)
	}
	correlationMetrics, err := obs.NewCorrelationMetrics(providers.MeterProvider)
	if err != nil {
		return fmt.Errorf("create correlation metrics: %w", err)
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
	// Close drains in-flight queries before releasing sockets.
	defer func() {
		pool.Close()
		log.Info("postgres pool closed")
	}()

	eventStore := store.NewEventStore(pool, dbMetrics, cfg.DBTimeout)
	incidentStore := store.NewIncidentStore(pool, dbMetrics, cfg.DBTimeout)

	consumer, err := kafkax.NewConsumer(kafkax.ConsumerConfig{
		Brokers:        cfg.Kafka.Brokers,
		Topic:          cfg.Kafka.Topic,
		Group:          cfg.ConsumerGroup,
		MaxPollRecords: cfg.MaxPollRecords,
		ClientID:       cfg.Observability.ServiceName,
	}, log)
	if err != nil {
		return fmt.Errorf("create kafka consumer: %w", err)
	}
	defer func() {
		consumer.Close()
		log.Info("kafka consumer closed")
	}()

	processor := engine.NewProcessor(engine.ProcessorOptions{
		Store: eventStore,
		Retry: engine.RetryPolicy{
			MaxAttempts: cfg.RetryAttempts,
			BaseDelay:   cfg.RetryBaseDelay,
			MaxDelay:    cfg.RetryMaxDelay,
		},
		Logger: log,
	})

	runner := engine.NewRunner(engine.RunnerOptions{
		Consumer:   consumer,
		Processor:  processor,
		Metrics:    kafkaMetrics,
		Providers:  providers,
		Logger:     log,
		Topic:      cfg.Kafka.Topic,
		MaxRecords: cfg.MaxPollRecords,
	})

	srv := httpx.NewServer(httpx.ServerOptions{
		Addr:    cfg.HTTPAddr,
		Handler: newRouter(eventStore, consumer, httpMetrics, log),
	})

	// Correlation turns the stored event stream into incidents. It runs beside
	// the consume loop, on its own cadence, over the same database pool and under
	// the same shutdown lifetime. It is left nil (and no goroutine started) when
	// disabled, so the engine can run as a pure ingester.
	var correlationRunner *correlate.Runner
	if cfg.Correlation.Enabled {
		rules := []correlate.Rule{{
			// An error-rate spike is worth an incident at error severity. The rule
			// id is part of every resulting incident's fingerprint, so it is stable.
			ID:               "error_rate",
			Name:             "elevated error rate",
			Kind:             correlate.RuleKindErrorRate,
			Window:           cfg.Correlation.Window,
			Threshold:        cfg.Correlation.ErrorRateThreshold,
			MinEvents:        int64(cfg.Correlation.ErrorRateMinEvents),
			IncidentSeverity: event.SeverityError,
		}}
		evaluator := correlate.NewEvaluator(correlate.EvaluatorOptions{
			Source:       eventStore,
			Sink:         incidentStore,
			Rules:        rules,
			Metrics:      correlationMetrics,
			Logger:       log,
			ResolveAfter: cfg.Correlation.ResolveAfter,
			Now:          time.Now,
			NewID:        uuid.NewString,
		})
		correlationRunner = correlate.NewRunner(evaluator, cfg.Correlation.Interval, log)
	}

	log.Info("incident-engine starting",
		slog.String("addr", cfg.HTTPAddr),
		slog.Any("kafka_brokers", cfg.Kafka.Brokers),
		slog.String("kafka_topic", cfg.Kafka.Topic),
		slog.String("consumer_group", cfg.ConsumerGroup),
		slog.Int("db_retry_attempts", cfg.RetryAttempts),
		slog.Bool("correlation_enabled", cfg.Correlation.Enabled),
		slog.Duration("correlation_interval", cfg.Correlation.Interval),
		slog.Duration("correlation_window", cfg.Correlation.Window),
	)

	// The probe server and the consume loop share a lifetime: if either stops,
	// the group's context is cancelled and the other unwinds too.
	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		if err := httpx.Serve(groupCtx, srv, cfg.ShutdownGrace, log); err != nil {
			return fmt.Errorf("probe server: %w", err)
		}
		return nil
	})

	group.Go(func() error {
		if err := runner.Run(groupCtx); err != nil {
			return fmt.Errorf("consumer loop: %w", err)
		}
		return nil
	})

	if correlationRunner != nil {
		group.Go(func() error {
			if err := correlationRunner.Run(groupCtx); err != nil {
				return fmt.Errorf("correlation loop: %w", err)
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	log.Info("incident-engine stopped")
	return nil
}

func newRouter(
	eventStore *store.EventStore,
	consumer *kafkax.Consumer,
	httpMetrics *obs.HTTPMetrics,
	log *slog.Logger,
) *chi.Mux {
	router := chi.NewRouter()

	router.Use(middleware.RequestID)
	router.Use(middleware.Recoverer)
	router.Use(httpx.Observe(httpMetrics, log))

	router.Get("/health", httpx.HealthHandler())
	router.Get("/ready", httpx.ReadinessHandler(log, 3*time.Second,
		httpx.ReadinessCheck{Name: "postgres", Check: eventStore.Ping},
		httpx.ReadinessCheck{Name: "kafka", Check: consumer.Ping},
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
