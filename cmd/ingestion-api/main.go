// Command ingestion-api is the HTTP front door of the SentinelFlow pipeline.
// It validates telemetry events and publishes the accepted ones to Kafka.
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

	"github.com/acbspace/sentinel-flow-project/internal/config"
	"github.com/acbspace/sentinel-flow-project/internal/httpx"
	"github.com/acbspace/sentinel-flow-project/internal/ingest"
	"github.com/acbspace/sentinel-flow-project/internal/kafkax"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
)

func main() {
	if err := run(); err != nil {
		// The logger may not exist yet when configuration fails, so fall back to
		// stderr rather than losing the reason the process refused to start.
		fmt.Fprintf(os.Stderr, `{"level":"error","service":"ingestion-api","msg":"fatal","error":%q}`+"\n", err.Error())
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadIngestionAPI()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	log := obs.NewLogger(os.Stdout, cfg.Observability.ServiceName, cfg.Observability.Environment, cfg.Observability.LogLevel)

	// SIGINT and SIGTERM cancel this context, which unwinds every component.
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

	producer, err := kafkax.NewProducer(kafkax.ProducerConfig{
		Brokers:        cfg.Kafka.Brokers,
		Topic:          cfg.Kafka.Topic,
		ProduceTimeout: cfg.ProduceTimeout,
		ClientID:       cfg.Observability.ServiceName,
	}, providers, kafkaMetrics, log)
	if err != nil {
		return fmt.Errorf("create kafka producer: %w", err)
	}
	defer closeProducer(producer, log, cfg.ShutdownGrace)

	handler := ingest.NewHandler(ingest.Options{
		Publisher:    producer,
		Logger:       log,
		MaxBodyBytes: cfg.MaxBodyBytes,
	})

	router := newRouter(handler, producer, httpMetrics, providers, log)

	srv := httpx.NewServer(httpx.ServerOptions{
		Addr:    cfg.HTTPAddr,
		Handler: router,
		// Publishing waits on a broker acknowledgement, so the write timeout
		// must comfortably exceed the produce timeout.
		WriteTimeout: cfg.ProduceTimeout + 15*time.Second,
	})

	log.Info("ingestion-api starting",
		slog.String("addr", cfg.HTTPAddr),
		slog.Any("kafka_brokers", cfg.Kafka.Brokers),
		slog.String("kafka_topic", cfg.Kafka.Topic),
		slog.Int64("max_body_bytes", cfg.MaxBodyBytes),
	)

	if err := httpx.Serve(ctx, srv, cfg.ShutdownGrace, log); err != nil {
		return fmt.Errorf("http server: %w", err)
	}

	log.Info("ingestion-api stopped")
	return nil
}

func newRouter(
	handler *ingest.Handler,
	producer *kafkax.Producer,
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
	router.Get("/ready", httpx.ReadinessHandler(log, 3*time.Second, httpx.ReadinessCheck{
		Name:  "kafka",
		Check: producer.Ping,
	}))
	router.Post("/v1/events", handler.PostEvent)

	// otelhttp wraps the whole router so that trace context arriving from a
	// caller is extracted before any handler runs.
	return otelhttp.NewHandler(router, "ingestion-api",
		otelhttp.WithTracerProvider(providers.TracerProvider),
		otelhttp.WithMeterProvider(providers.MeterProvider),
		otelhttp.WithPropagators(providers.Propagator),
	)
}

func closeProducer(producer *kafkax.Producer, log *slog.Logger, grace time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()

	if err := producer.Close(ctx); err != nil {
		log.Error("kafka producer shutdown", slog.String("error", err.Error()))
		return
	}
	log.Info("kafka producer closed")
}

func shutdownTelemetry(providers *obs.Providers, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := providers.Shutdown(ctx); err != nil {
		log.Error("telemetry shutdown", slog.String("error", err.Error()))
	}
}
