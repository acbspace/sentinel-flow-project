// Package config loads per-service settings from the environment.
//
// Every service builds its own config struct explicitly and passes it down; no
// package here reads the environment after startup and nothing is cached in a
// package level variable.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Observability holds settings shared by every service's telemetry pipeline.
type Observability struct {
	ServiceName     string
	ServiceVersion  string
	Environment     string
	LogLevel        string
	TracesExporter  string
	MetricsExporter string
	OTLPEndpoint    string
	OTLPInsecure    bool
	MetricInterval  time.Duration
}

// Kafka holds the broker coordinates shared by producers and consumers.
type Kafka struct {
	Brokers []string
	Topic   string
}

// IngestionAPI configures the HTTP ingestion front door.
type IngestionAPI struct {
	Observability  Observability
	Kafka          Kafka
	HTTPAddr       string
	MaxBodyBytes   int64
	ProduceTimeout time.Duration

	// Bounds on the batch endpoint, kept separate from MaxBodyBytes: a batch is
	// legitimately much larger than one event.
	MaxBatchBytes  int64
	MaxBatchEvents int

	// MaxEventFutureSkew and MaxEventBackdate reject events whose timestamp is
	// implausible. The ingestion API is the only place that can tell a producer
	// its clock is wrong while the producer is still there to hear it, so it is
	// the one boundary that enforces both. Either may be zero to disable that
	// bound; see event.TimeBounds for why the engine enforces only the first.
	MaxEventFutureSkew time.Duration
	MaxEventBackdate   time.Duration

	ShutdownGrace time.Duration
}

// IncidentEngine configures the Kafka consumer that persists events.
type IncidentEngine struct {
	Observability  Observability
	Kafka          Kafka
	ConsumerGroup  string
	HTTPAddr       string
	PostgresDSN    string
	MaxPollRecords int

	// MaxEventFutureSkew is the only time bound the engine applies. Re-checking
	// how old a record is would turn any Kafka backlog into discarded telemetry;
	// see event.TimeBounds for the argument.
	MaxEventFutureSkew time.Duration

	DBTimeout      time.Duration
	RetryAttempts  int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	Correlation    Correlation
	ShutdownGrace  time.Duration
}

// Correlation configures the incident engine's correlation loop: how often it
// evaluates, how far back each evaluation looks, when the error-rate rule fires,
// and how long an incident may go quiet before it auto-resolves.
type Correlation struct {
	Enabled            bool
	Interval           time.Duration
	Window             time.Duration
	ErrorRateThreshold float64
	ErrorRateMinEvents int
	ResolveAfter       time.Duration
}

// IncidentsAPI configures the read/lifecycle HTTP API over incidents and stored
// events. It is read-mostly and touches only PostgreSQL, deliberately separate
// from the write-only ingestion front door.
type IncidentsAPI struct {
	Observability Observability
	HTTPAddr      string
	PostgresDSN   string
	DBTimeout     time.Duration
	// TemporalAddress is optional: when set, acknowledge/resolve also signal the
	// incident's alert workflow. Empty disables signalling (the API still works).
	TemporalAddress   string
	TemporalNamespace string
	ShutdownGrace     time.Duration
}

// Alerting configures the alert worker: the Temporal connection, the poller that
// starts alert workflows for new incidents, and how notifications are delivered.
type Alerting struct {
	Observability     Observability
	HTTPAddr          string
	PostgresDSN       string
	DBTimeout         time.Duration
	TemporalAddress   string
	TemporalNamespace string
	TaskQueue         string
	PollInterval      time.Duration
	BatchSize         int
	WebhookURL        string
	WebhookTimeout    time.Duration
	PolicyPath        string
	ShutdownGrace     time.Duration
}

// Remediation configures the remediation worker: the Temporal connection, the
// poller that starts runbook runs, and where the runbook catalog comes from.
type Remediation struct {
	Observability     Observability
	HTTPAddr          string
	PostgresDSN       string
	DBTimeout         time.Duration
	TemporalAddress   string
	TemporalNamespace string
	TaskQueue         string
	PollInterval      time.Duration
	BatchSize         int
	ActionTimeout     time.Duration
	CatalogPath       string
	ShutdownGrace     time.Duration
}

// DemoService configures one of the synthetic traffic generators.
type DemoService struct {
	Observability Observability
	HTTPAddr      string
	IngestionURL  string
	TenantID      string
	FailureRate   float64
	MinLatency    time.Duration
	MaxLatency    time.Duration
	EmitTimeout   time.Duration
	DownstreamURL string
	ShutdownGrace time.Duration
}

// Janitor configures the partition maintenance service.
type Janitor struct {
	Observability Observability
	HTTPAddr      string
	PostgresDSN   string
	DBTimeout     time.Duration

	// Interval is how often a maintenance cycle runs.
	Interval time.Duration

	// Lookahead is how far ahead partitions are created. It must exceed any
	// plausible outage of this service: a day with no partition does not fail,
	// but its events land in the default partition and have to be drained back
	// out later.
	Lookahead time.Duration

	// Retention is how long telemetry is kept. This is the one setting in the
	// system that destroys data, so it has no clever default and is validated
	// rather than silently coerced.
	Retention time.Duration

	ShutdownGrace time.Duration
}

// Migrate configures the schema migration tool.
type Migrate struct {
	PostgresDSN string
	Timeout     time.Duration
}

// LoadIngestionAPI reads the ingestion API configuration.
func LoadIngestionAPI() (IngestionAPI, error) {
	var errs []error
	cfg := IngestionAPI{
		Observability:  loadObservability("ingestion-api", &errs),
		Kafka:          loadKafka(&errs),
		HTTPAddr:       stringVar("HTTP_ADDR", ":8080"),
		MaxBodyBytes:   int64Var("MAX_BODY_BYTES", 64*1024, &errs),
		ProduceTimeout: durationVar("KAFKA_PRODUCE_TIMEOUT", 10*time.Second, &errs),
		MaxBatchBytes:  int64Var("MAX_BATCH_BYTES", 5*1024*1024, &errs),
		MaxBatchEvents: intVar("MAX_BATCH_EVENTS", 1000, &errs),

		// Five minutes of forward skew tolerates an unsynchronised host without
		// admitting an event that would sit in every correlation window forever.
		// Seven days of backdate accepts a genuine replay or a producer catching
		// up after an outage, while keeping rows inside the retention horizon.
		MaxEventFutureSkew: durationVar("MAX_EVENT_FUTURE_SKEW", 5*time.Minute, &errs),
		MaxEventBackdate:   durationVar("MAX_EVENT_BACKDATE", 7*24*time.Hour, &errs),

		ShutdownGrace: durationVar("SHUTDOWN_GRACE", 15*time.Second, &errs),
	}
	if cfg.MaxBodyBytes <= 0 {
		errs = append(errs, errors.New("MAX_BODY_BYTES must be greater than zero"))
	}
	if cfg.MaxBatchBytes <= 0 {
		errs = append(errs, errors.New("MAX_BATCH_BYTES must be greater than zero"))
	}
	if cfg.MaxBatchEvents <= 0 {
		errs = append(errs, errors.New("MAX_BATCH_EVENTS must be greater than zero"))
	}
	if cfg.MaxEventFutureSkew < 0 {
		errs = append(errs, errors.New("MAX_EVENT_FUTURE_SKEW must not be negative (use 0 to disable the check)"))
	}
	if cfg.MaxEventBackdate < 0 {
		errs = append(errs, errors.New("MAX_EVENT_BACKDATE must not be negative (use 0 to disable the check)"))
	}
	return cfg, errors.Join(errs...)
}

// LoadIncidentEngine reads the incident engine configuration.
func LoadIncidentEngine() (IncidentEngine, error) {
	var errs []error
	cfg := IncidentEngine{
		Observability:  loadObservability("incident-engine", &errs),
		Kafka:          loadKafka(&errs),
		ConsumerGroup:  stringVar("KAFKA_CONSUMER_GROUP", "incident-engine-v1"),
		HTTPAddr:       stringVar("HTTP_ADDR", ":8081"),
		PostgresDSN:    stringVar("POSTGRES_DSN", ""),
		MaxPollRecords: intVar("KAFKA_MAX_POLL_RECORDS", 250, &errs),

		// Matches the ingestion API's default so a record accepted there is never
		// then rejected here. Note the absence of a backdate bound.
		MaxEventFutureSkew: durationVar("MAX_EVENT_FUTURE_SKEW", 5*time.Minute, &errs),

		DBTimeout:      durationVar("DB_TIMEOUT", 5*time.Second, &errs),
		RetryAttempts:  intVar("DB_RETRY_ATTEMPTS", 5, &errs),
		RetryBaseDelay: durationVar("DB_RETRY_BASE_DELAY", 100*time.Millisecond, &errs),
		RetryMaxDelay:  durationVar("DB_RETRY_MAX_DELAY", 5*time.Second, &errs),
		Correlation:    loadCorrelation(&errs),
		ShutdownGrace:  durationVar("SHUTDOWN_GRACE", 30*time.Second, &errs),
	}
	if cfg.PostgresDSN == "" {
		errs = append(errs, errors.New("POSTGRES_DSN is required"))
	}
	if cfg.ConsumerGroup == "" {
		errs = append(errs, errors.New("KAFKA_CONSUMER_GROUP must not be empty"))
	}
	if cfg.RetryAttempts < 1 {
		errs = append(errs, errors.New("DB_RETRY_ATTEMPTS must be at least 1"))
	}
	if cfg.MaxPollRecords < 1 {
		errs = append(errs, errors.New("KAFKA_MAX_POLL_RECORDS must be at least 1"))
	}
	if cfg.MaxEventFutureSkew < 0 {
		errs = append(errs, errors.New("MAX_EVENT_FUTURE_SKEW must not be negative (use 0 to disable the check)"))
	}
	return cfg, errors.Join(errs...)
}

// LoadIncidentsAPI reads the incidents read/lifecycle API configuration.
func LoadIncidentsAPI() (IncidentsAPI, error) {
	var errs []error
	cfg := IncidentsAPI{
		Observability:     loadObservability("incidents-api", &errs),
		HTTPAddr:          stringVar("HTTP_ADDR", ":8084"),
		PostgresDSN:       stringVar("POSTGRES_DSN", ""),
		DBTimeout:         durationVar("DB_TIMEOUT", 5*time.Second, &errs),
		TemporalAddress:   stringVar("TEMPORAL_ADDRESS", ""),
		TemporalNamespace: stringVar("TEMPORAL_NAMESPACE", "default"),
		ShutdownGrace:     durationVar("SHUTDOWN_GRACE", 15*time.Second, &errs),
	}
	if cfg.PostgresDSN == "" {
		errs = append(errs, errors.New("POSTGRES_DSN is required"))
	}
	return cfg, errors.Join(errs...)
}

// LoadAlerting reads the alert worker configuration.
func LoadAlerting() (Alerting, error) {
	var errs []error
	cfg := Alerting{
		Observability:     loadObservability("alerting", &errs),
		HTTPAddr:          stringVar("HTTP_ADDR", ":8085"),
		PostgresDSN:       stringVar("POSTGRES_DSN", ""),
		DBTimeout:         durationVar("DB_TIMEOUT", 5*time.Second, &errs),
		TemporalAddress:   stringVar("TEMPORAL_ADDRESS", "localhost:7233"),
		TemporalNamespace: stringVar("TEMPORAL_NAMESPACE", "default"),
		TaskQueue:         stringVar("TEMPORAL_TASK_QUEUE", "incident-alerts"),
		PollInterval:      durationVar("ALERT_POLL_INTERVAL", 10*time.Second, &errs),
		BatchSize:         intVar("ALERT_BATCH_SIZE", 50, &errs),
		WebhookURL:        stringVar("ALERT_WEBHOOK_URL", ""),
		WebhookTimeout:    durationVar("ALERT_WEBHOOK_TIMEOUT", 5*time.Second, &errs),
		PolicyPath:        stringVar("ESCALATION_POLICY_PATH", ""),
		ShutdownGrace:     durationVar("SHUTDOWN_GRACE", 15*time.Second, &errs),
	}
	if cfg.PostgresDSN == "" {
		errs = append(errs, errors.New("POSTGRES_DSN is required"))
	}
	if cfg.TemporalAddress == "" {
		errs = append(errs, errors.New("TEMPORAL_ADDRESS must not be empty"))
	}
	if cfg.TaskQueue == "" {
		errs = append(errs, errors.New("TEMPORAL_TASK_QUEUE must not be empty"))
	}
	if cfg.PollInterval <= 0 {
		errs = append(errs, errors.New("ALERT_POLL_INTERVAL must be greater than zero"))
	}
	if cfg.BatchSize < 1 {
		errs = append(errs, errors.New("ALERT_BATCH_SIZE must be at least 1"))
	}
	return cfg, errors.Join(errs...)
}

// LoadRemediation reads the remediation worker configuration.
func LoadRemediation() (Remediation, error) {
	var errs []error
	cfg := Remediation{
		Observability:     loadObservability("remediation", &errs),
		HTTPAddr:          stringVar("HTTP_ADDR", ":8086"),
		PostgresDSN:       stringVar("POSTGRES_DSN", ""),
		DBTimeout:         durationVar("DB_TIMEOUT", 5*time.Second, &errs),
		TemporalAddress:   stringVar("TEMPORAL_ADDRESS", "localhost:7233"),
		TemporalNamespace: stringVar("TEMPORAL_NAMESPACE", "default"),
		TaskQueue:         stringVar("TEMPORAL_TASK_QUEUE", "incident-remediation"),
		PollInterval:      durationVar("REMEDIATION_POLL_INTERVAL", 10*time.Second, &errs),
		BatchSize:         intVar("REMEDIATION_BATCH_SIZE", 50, &errs),
		ActionTimeout:     durationVar("REMEDIATION_ACTION_TIMEOUT", 10*time.Second, &errs),
		CatalogPath:       stringVar("RUNBOOK_CATALOG_PATH", ""),
		ShutdownGrace:     durationVar("SHUTDOWN_GRACE", 15*time.Second, &errs),
	}
	if cfg.PostgresDSN == "" {
		errs = append(errs, errors.New("POSTGRES_DSN is required"))
	}
	if cfg.TemporalAddress == "" {
		errs = append(errs, errors.New("TEMPORAL_ADDRESS must not be empty"))
	}
	if cfg.TaskQueue == "" {
		errs = append(errs, errors.New("TEMPORAL_TASK_QUEUE must not be empty"))
	}
	if cfg.PollInterval <= 0 {
		errs = append(errs, errors.New("REMEDIATION_POLL_INTERVAL must be greater than zero"))
	}
	if cfg.BatchSize < 1 {
		errs = append(errs, errors.New("REMEDIATION_BATCH_SIZE must be at least 1"))
	}
	return cfg, errors.Join(errs...)
}

// LoadDemoService reads the configuration shared by the demo producers.
// defaultAddr and defaultFailureRate differ per service, so the caller supplies
// them rather than the package guessing from the service name.
func LoadDemoService(serviceName, defaultAddr string, defaultFailureRate float64) (DemoService, error) {
	var errs []error
	cfg := DemoService{
		Observability: loadObservability(serviceName, &errs),
		HTTPAddr:      stringVar("HTTP_ADDR", defaultAddr),
		IngestionURL:  stringVar("INGESTION_API_URL", "http://localhost:8080"),
		TenantID:      stringVar("TENANT_ID", "demo-tenant"),
		FailureRate:   floatVar("FAILURE_RATE", defaultFailureRate, &errs),
		MinLatency:    durationVar("MIN_LATENCY", 20*time.Millisecond, &errs),
		MaxLatency:    durationVar("MAX_LATENCY", 250*time.Millisecond, &errs),
		EmitTimeout:   durationVar("EMIT_TIMEOUT", 5*time.Second, &errs),
		DownstreamURL: stringVar("DOWNSTREAM_URL", ""),
		ShutdownGrace: durationVar("SHUTDOWN_GRACE", 15*time.Second, &errs),
	}
	if cfg.FailureRate < 0 || cfg.FailureRate > 1 {
		errs = append(errs, fmt.Errorf("FAILURE_RATE must be between 0 and 1, got %v", cfg.FailureRate))
	}
	if cfg.MinLatency < 0 || cfg.MaxLatency < cfg.MinLatency {
		errs = append(errs, errors.New("MIN_LATENCY must be non-negative and MAX_LATENCY must be at least MIN_LATENCY"))
	}
	if cfg.TenantID == "" {
		errs = append(errs, errors.New("TENANT_ID must not be empty"))
	}
	return cfg, errors.Join(errs...)
}

// LoadMigrate reads the migration tool configuration.
// LoadJanitor reads the partition janitor configuration.
func LoadJanitor() (Janitor, error) {
	var errs []error
	cfg := Janitor{
		Observability: loadObservability("janitor", &errs),
		HTTPAddr:      stringVar("HTTP_ADDR", ":8087"),
		PostgresDSN:   stringVar("POSTGRES_DSN", ""),
		DBTimeout:     durationVar("DB_TIMEOUT", 30*time.Second, &errs),
		Interval:      durationVar("JANITOR_INTERVAL", time.Hour, &errs),
		Lookahead:     durationVar("PARTITION_LOOKAHEAD", 7*24*time.Hour, &errs),
		Retention:     durationVar("TELEMETRY_RETENTION", 30*24*time.Hour, &errs),
		ShutdownGrace: durationVar("SHUTDOWN_GRACE", 15*time.Second, &errs),
	}
	if cfg.PostgresDSN == "" {
		errs = append(errs, errors.New("POSTGRES_DSN is required"))
	}
	if cfg.Interval <= 0 {
		errs = append(errs, errors.New("JANITOR_INTERVAL must be greater than zero"))
	}
	if cfg.Lookahead < 24*time.Hour {
		errs = append(errs, errors.New("PARTITION_LOOKAHEAD must be at least 24h; a shorter horizon sends events to the default partition"))
	}
	// The floor is not arbitrary: MAX_EVENT_BACKDATE defaults to 7 days, so a
	// retention shorter than that would drop a partition the ingestion API is
	// still willing to accept events for.
	if cfg.Retention < 7*24*time.Hour {
		errs = append(errs, errors.New("TELEMETRY_RETENTION must be at least 168h, matching the backdate the ingestion API accepts"))
	}
	if cfg.Lookahead >= cfg.Retention {
		errs = append(errs, errors.New("PARTITION_LOOKAHEAD must be shorter than TELEMETRY_RETENTION"))
	}
	return cfg, errors.Join(errs...)
}

func LoadMigrate() (Migrate, error) {
	var errs []error
	cfg := Migrate{
		PostgresDSN: stringVar("POSTGRES_DSN", ""),
		Timeout:     durationVar("MIGRATE_TIMEOUT", 60*time.Second, &errs),
	}
	if cfg.PostgresDSN == "" {
		errs = append(errs, errors.New("POSTGRES_DSN is required"))
	}
	return cfg, errors.Join(errs...)
}

func loadObservability(serviceName string, errs *[]error) Observability {
	return Observability{
		ServiceName:     stringVar("OTEL_SERVICE_NAME", serviceName),
		ServiceVersion:  stringVar("SERVICE_VERSION", "dev"),
		Environment:     stringVar("ENVIRONMENT", "local"),
		LogLevel:        stringVar("LOG_LEVEL", "info"),
		TracesExporter:  stringVar("OTEL_TRACES_EXPORTER", "stdout"),
		MetricsExporter: stringVar("OTEL_METRICS_EXPORTER", "stdout"),
		OTLPEndpoint:    stringVar("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		OTLPInsecure:    boolVar("OTEL_EXPORTER_OTLP_INSECURE", true, errs),
		MetricInterval:  durationVar("OTEL_METRIC_EXPORT_INTERVAL", 60*time.Second, errs),
	}
}

func loadCorrelation(errs *[]error) Correlation {
	c := Correlation{
		Enabled:            boolVar("CORRELATION_ENABLED", true, errs),
		Interval:           durationVar("CORRELATION_INTERVAL", 15*time.Second, errs),
		Window:             durationVar("CORRELATION_WINDOW", 60*time.Second, errs),
		ErrorRateThreshold: floatVar("CORRELATION_ERROR_RATE_THRESHOLD", 0.5, errs),
		ErrorRateMinEvents: intVar("CORRELATION_ERROR_RATE_MIN_EVENTS", 5, errs),
		ResolveAfter:       durationVar("CORRELATION_RESOLVE_AFTER", 5*time.Minute, errs),
	}
	if c.Interval <= 0 {
		*errs = append(*errs, errors.New("CORRELATION_INTERVAL must be greater than zero"))
	}
	if c.Window <= 0 {
		*errs = append(*errs, errors.New("CORRELATION_WINDOW must be greater than zero"))
	}
	if c.ErrorRateThreshold < 0 || c.ErrorRateThreshold > 1 {
		*errs = append(*errs, fmt.Errorf("CORRELATION_ERROR_RATE_THRESHOLD must be between 0 and 1, got %v", c.ErrorRateThreshold))
	}
	if c.ErrorRateMinEvents < 1 {
		*errs = append(*errs, errors.New("CORRELATION_ERROR_RATE_MIN_EVENTS must be at least 1"))
	}
	if c.ResolveAfter <= 0 {
		*errs = append(*errs, errors.New("CORRELATION_RESOLVE_AFTER must be greater than zero"))
	}
	return c
}

func loadKafka(errs *[]error) Kafka {
	brokers := splitAndTrim(stringVar("KAFKA_BROKERS", "localhost:9092"))
	if len(brokers) == 0 {
		*errs = append(*errs, errors.New("KAFKA_BROKERS must list at least one broker"))
	}
	topic := stringVar("KAFKA_TOPIC", "telemetry.events.v1")
	if topic == "" {
		*errs = append(*errs, errors.New("KAFKA_TOPIC must not be empty"))
	}
	return Kafka{Brokers: brokers, Topic: topic}
}

func splitAndTrim(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func stringVar(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}

func intVar(key string, fallback int, errs *[]error) int {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a valid integer", key, raw))
		return fallback
	}
	return v
}

func int64Var(key string, fallback int64, errs *[]error) int64 {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback
	}
	v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a valid integer", key, raw))
		return fallback
	}
	return v
}

func floatVar(key string, fallback float64, errs *[]error) float64 {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a valid number", key, raw))
		return fallback
	}
	return v
}

func boolVar(key string, fallback bool, errs *[]error) bool {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback
	}
	v, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a valid boolean", key, raw))
		return fallback
	}
	return v
}

func durationVar(key string, fallback time.Duration, errs *[]error) time.Duration {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback
	}
	v, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a valid duration (for example 250ms or 5s)", key, raw))
		return fallback
	}
	return v
}
