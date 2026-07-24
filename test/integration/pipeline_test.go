//go:build integration

// Package integration exercises the whole milestone 1 pipeline against real
// infrastructure: an HTTP request to the ingestion handler must travel through
// Kafka and arrive as a row in PostgreSQL, and a redelivery of the same event
// must not create a second row.
//
// Run it with a live Kafka and PostgreSQL:
//
//	make up
//	make test-integration
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/acbspace/sentinel-flow-project/internal/engine"
	"github.com/acbspace/sentinel-flow-project/internal/ingest"
	"github.com/acbspace/sentinel-flow-project/internal/kafkax"
	"github.com/acbspace/sentinel-flow-project/internal/migrate"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
	"github.com/acbspace/sentinel-flow-project/internal/store"
	"github.com/acbspace/sentinel-flow-project/migrations"
)

const (
	defaultDSN     = "postgres://sentinelflow:sentinelflow@localhost:5432/sentinelflow?sslmode=disable"
	defaultBrokers = "localhost:29092"

	// How long to wait for an event to traverse the whole pipeline.
	pipelineTimeout = 45 * time.Second
)

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()

	if testing.Verbose() {
		return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// pipeline holds everything the test needs to drive one end-to-end run.
type pipeline struct {
	handler *ingest.Handler
	pool    *pgxpool.Pool
	topic   string
}

// newPipeline wires a real producer, consumer, store and handler against the
// running infrastructure, then starts the engine's consume loop.
//
// Each run gets its own topic and consumer group so that repeated runs, and
// whatever the demo services have already produced, cannot affect the result.
func newPipeline(ctx context.Context, t *testing.T) *pipeline {
	t.Helper()

	log := testLogger(t)
	providers := obs.NoopProviders()

	kafkaMetrics, err := obs.NewKafkaMetrics(providers.MeterProvider)
	if err != nil {
		t.Fatalf("create kafka metrics: %v", err)
	}
	dbMetrics, err := obs.NewDBMetrics(providers.MeterProvider)
	if err != nil {
		t.Fatalf("create db metrics: %v", err)
	}

	dsn := envOr("POSTGRES_DSN", defaultDSN)
	pool, err := store.NewPool(ctx, store.PoolConfig{DSN: dsn, MaxConns: 4, ConnectTimeout: 10 * time.Second}, log)
	if err != nil {
		t.Fatalf("connect to postgres at %s: %v\nis the stack running? try: make up", dsn, err)
	}
	t.Cleanup(pool.Close)

	loaded, err := migrate.Load(migrations.FS)
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if err := migrate.Up(ctx, pool, loaded, log); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	brokers := strings.Split(envOr("KAFKA_BROKERS", defaultBrokers), ",")
	suffix := uuid.NewString()[:8]
	topic := "telemetry.events.itest." + suffix
	group := "incident-engine-itest-" + suffix

	// Create the topic explicitly, exactly as the kafka-init job does for the
	// real topic. The producer deliberately does not enable auto topic creation:
	// topics are provisioned deliberately, not conjured by whoever writes first.
	createTopic(ctx, t, brokers, topic)

	producer, err := kafkax.NewProducer(kafkax.ProducerConfig{
		Brokers:        brokers,
		Topic:          topic,
		ProduceTimeout: 15 * time.Second,
		ClientID:       "integration-test-producer",
	}, providers, kafkaMetrics, log)
	if err != nil {
		t.Fatalf("create kafka producer for %v: %v\nis the stack running? try: make up", brokers, err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := producer.Close(closeCtx); err != nil {
			t.Logf("close producer: %v", err)
		}
	})

	consumer, err := kafkax.NewConsumer(kafkax.ConsumerConfig{
		Brokers:        brokers,
		Topic:          topic,
		Group:          group,
		MaxPollRecords: 50,
		ClientID:       "integration-test-consumer",
	}, log)
	if err != nil {
		t.Fatalf("create kafka consumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	eventStore := store.NewEventStore(pool, dbMetrics, 10*time.Second)

	runner := engine.NewRunner(engine.RunnerOptions{
		Consumer: consumer,
		Processor: engine.NewProcessor(engine.ProcessorOptions{
			Store:  eventStore,
			Retry:  engine.RetryPolicy{MaxAttempts: 3, BaseDelay: 50 * time.Millisecond, MaxDelay: time.Second},
			Logger: log,
		}),
		Metrics:    kafkaMetrics,
		Providers:  providers,
		Logger:     log,
		Topic:      topic,
		MaxRecords: 50,
	})

	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	t.Cleanup(func() {
		select {
		case err := <-done:
			if err != nil && !strings.Contains(err.Error(), "context canceled") {
				t.Logf("consumer loop returned: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Log("consumer loop did not stop within 20s")
		}
	})

	return &pipeline{
		handler: ingest.NewHandler(ingest.Options{Publisher: producer, Logger: log}),
		pool:    pool,
		topic:   topic,
	}
}

// createTopic provisions a dedicated topic for one test run and removes it
// afterwards, so repeated runs never inherit each other's records.
func createTopic(ctx context.Context, t *testing.T, brokers []string, topic string) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("create kafka admin client for %v: %v\nis the stack running? try: make up", brokers, err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)

	createCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Three partitions, matching the real topic, so the partitioning behaviour
	// under test is the same one production uses.
	resp, err := admin.CreateTopics(createCtx, 3, 1, nil, topic)
	if err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	for _, result := range resp {
		if result.Err != nil {
			t.Fatalf("create topic %s: %v", result.Topic, result.Err)
		}
	}

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if _, err := admin.DeleteTopics(cleanupCtx, topic); err != nil {
			t.Logf("delete topic %s: %v", topic, err)
		}
	})
}

// submit posts body to the ingestion handler and returns the recorder.
func (p *pipeline) submit(t *testing.T, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	p.handler.PostEvent(rec, req)
	return rec
}

// storedEvent is the subset of a persisted row the assertions care about.
type storedEvent struct {
	SchemaVersion string
	TenantID      string
	ServiceName   string
	Environment   string
	EventType     string
	Severity      string
	EventTime     time.Time
	TraceID       string
	Attributes    map[string]any
	ReceivedAt    time.Time
	ProcessedAt   time.Time
}

// waitForEvent polls until the event with eventID is persisted or time runs out.
// Polling is the honest way to test an asynchronous pipeline: the HTTP response
// only promises the event reached Kafka, not that the engine has consumed it.
func (p *pipeline) waitForEvent(ctx context.Context, t *testing.T, eventID string) storedEvent {
	t.Helper()

	deadline := time.Now().Add(pipelineTimeout)
	var lastErr error

	for time.Now().Before(deadline) {
		got, err := p.fetchEvent(ctx, eventID)
		if err == nil {
			return got
		}
		lastErr = err

		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled while waiting for event %s: %v", eventID, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}

	t.Fatalf("event %s did not reach postgres within %s (last error: %v)", eventID, pipelineTimeout, lastErr)
	return storedEvent{}
}

func (p *pipeline) fetchEvent(ctx context.Context, eventID string) (storedEvent, error) {
	const query = `
SELECT schema_version, tenant_id, service_name, environment, event_type,
       severity, event_timestamp, trace_id, attributes, received_at, processed_at
FROM telemetry_events
WHERE event_id = $1`

	var (
		got      storedEvent
		rawAttrs []byte
	)

	err := p.pool.QueryRow(ctx, query, eventID).Scan(
		&got.SchemaVersion, &got.TenantID, &got.ServiceName, &got.Environment, &got.EventType,
		&got.Severity, &got.EventTime, &got.TraceID, &rawAttrs, &got.ReceivedAt, &got.ProcessedAt,
	)
	if err != nil {
		return storedEvent{}, err
	}

	if err := json.Unmarshal(rawAttrs, &got.Attributes); err != nil {
		return storedEvent{}, fmt.Errorf("decode attributes: %w", err)
	}
	return got, nil
}

func (p *pipeline) countEvent(ctx context.Context, t *testing.T, eventID string) int {
	t.Helper()

	var count int
	err := p.pool.QueryRow(ctx, `SELECT count(*) FROM telemetry_events WHERE event_id = $1`, eventID).Scan(&count)
	if err != nil {
		t.Fatalf("count rows for %s: %v", eventID, err)
	}
	return count
}

// TestPipelinePersistsIngestedEvent covers the milestone's core promise: an
// event accepted over HTTP is published to Kafka, consumed by the engine, and
// stored in PostgreSQL with its fields intact.
func TestPipelinePersistsIngestedEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	p := newPipeline(ctx, t)

	eventID := uuid.NewString()
	traceID := strings.ReplaceAll(uuid.NewString(), "-", "")
	eventTime := time.Now().UTC().Truncate(time.Millisecond)

	body := map[string]any{
		"event_id":       eventID,
		"schema_version": "1.0",
		"tenant_id":      "demo-tenant",
		"service_name":   "payment-service",
		"environment":    "test",
		"event_type":     "request.completed",
		"severity":       "error",
		"timestamp":      eventTime.Format(time.RFC3339Nano),
		"trace_id":       traceID,
		"attributes": map[string]any{
			"http_method":      "POST",
			"http_route":       "/demo/payments",
			"http_status_code": 500,
			"latency_ms":       125,
		},
	}
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}

	submitted := time.Now().UTC()

	rec := p.submit(t, payload)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /v1/events = %d, want %d; body: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	got := p.waitForEvent(ctx, t, eventID)

	if got.SchemaVersion != "1.0" {
		t.Errorf("schema_version = %q, want %q", got.SchemaVersion, "1.0")
	}
	if got.TenantID != "demo-tenant" {
		t.Errorf("tenant_id = %q, want %q", got.TenantID, "demo-tenant")
	}
	if got.ServiceName != "payment-service" {
		t.Errorf("service_name = %q, want %q", got.ServiceName, "payment-service")
	}
	if got.Environment != "test" {
		t.Errorf("environment = %q, want %q", got.Environment, "test")
	}
	if got.EventType != "request.completed" {
		t.Errorf("event_type = %q, want %q", got.EventType, "request.completed")
	}
	if got.Severity != "error" {
		t.Errorf("severity = %q, want %q", got.Severity, "error")
	}
	if got.TraceID != traceID {
		t.Errorf("trace_id = %q, want %q", got.TraceID, traceID)
	}
	if !got.EventTime.UTC().Equal(eventTime) {
		t.Errorf("event_timestamp = %v, want %v", got.EventTime.UTC(), eventTime)
	}

	// Attributes must survive the JSON to JSONB round trip.
	if got.Attributes["http_method"] != "POST" {
		t.Errorf("attributes.http_method = %v, want POST", got.Attributes["http_method"])
	}
	if got.Attributes["http_status_code"] != float64(500) {
		t.Errorf("attributes.http_status_code = %v, want 500", got.Attributes["http_status_code"])
	}
	if got.Attributes["latency_ms"] != float64(125) {
		t.Errorf("attributes.latency_ms = %v, want 125", got.Attributes["latency_ms"])
	}

	// received_at comes from the Kafka record timestamp set at produce time, and
	// processed_at from the database clock; both must bracket this test run.
	if got.ReceivedAt.Before(submitted.Add(-time.Minute)) {
		t.Errorf("received_at = %v, which predates this test run", got.ReceivedAt)
	}
	if got.ProcessedAt.Before(got.ReceivedAt.Add(-time.Second)) {
		t.Errorf("processed_at %v is before received_at %v", got.ProcessedAt, got.ReceivedAt)
	}
}

// TestPipelineIgnoresDuplicateEvents covers the idempotency guarantee: because
// Kafka delivery is at-least-once, the same event ID may be processed more than
// once, and the primary key must collapse those into a single row.
func TestPipelineIgnoresDuplicateEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	p := newPipeline(ctx, t)

	eventID := uuid.NewString()
	body := map[string]any{
		"event_id":       eventID,
		"schema_version": "1.0",
		"tenant_id":      "demo-tenant",
		"service_name":   "order-service",
		"environment":    "test",
		"event_type":     "request.completed",
		"severity":       "info",
		"timestamp":      time.Now().UTC().Format(time.RFC3339Nano),
		"attributes":     map[string]any{"attempt": 1},
	}
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}

	// Submit the identical event three times. Each submission is a separate
	// Kafka record, so the engine really does process the same event ID
	// repeatedly; only the database constraint prevents duplicate rows.
	for i := range 3 {
		rec := p.submit(t, payload)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("submission %d: POST /v1/events = %d, want %d; body: %s",
				i+1, rec.Code, http.StatusAccepted, rec.Body.String())
		}
	}

	p.waitForEvent(ctx, t, eventID)

	// Give the engine time to consume the two redeliveries before counting, so
	// that a passing result cannot come from having simply looked too early.
	time.Sleep(3 * time.Second)

	if count := p.countEvent(ctx, t, eventID); count != 1 {
		t.Errorf("stored %d rows for event %s, want exactly 1", count, eventID)
	}
}

// TestPipelineRejectsInvalidEventsBeforeKafka confirms the ingestion API is a
// real gate: a malformed event never reaches the topic and therefore never
// reaches the database.
func TestPipelineRejectsInvalidEventsBeforeKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	p := newPipeline(ctx, t)

	eventID := uuid.NewString()
	body := map[string]any{
		"event_id":       eventID,
		"schema_version": "1.0",
		"tenant_id":      "demo-tenant",
		"service_name":   "order-service",
		"environment":    "test",
		"event_type":     "request.completed",
		"severity":       "catastrophic", // not a supported severity
		"timestamp":      time.Now().UTC().Format(time.RFC3339Nano),
	}
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}

	rec := p.submit(t, payload)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST /v1/events = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	// Allow time for a row to appear if the gate had leaked.
	time.Sleep(2 * time.Second)

	if count := p.countEvent(ctx, t, eventID); count != 0 {
		t.Errorf("stored %d rows for a rejected event, want 0", count)
	}
}
