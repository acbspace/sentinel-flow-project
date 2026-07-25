package kafkax

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
)

// HeaderSchemaVersion lets a consumer route a record without decoding its body.
const HeaderSchemaVersion = "sentinelflow-schema-version"

// HeaderContentType documents the payload encoding for future non-JSON formats.
const HeaderContentType = "content-type"

// ProducerConfig describes how the producer connects and how hard it tries.
type ProducerConfig struct {
	Brokers        []string
	Topic          string
	ProduceTimeout time.Duration
	// ClientID identifies this producer in broker-side metrics and logs.
	ClientID string
}

// Producer publishes telemetry events to Kafka and waits for the broker's
// acknowledgement before reporting success.
type Producer struct {
	client     *kgo.Client
	topic      string
	timeout    time.Duration
	propagator propagation.TextMapPropagator
	tracer     trace.Tracer
	metrics    *obs.KafkaMetrics
	log        *slog.Logger
}

// NewProducer builds a Kafka producer.
//
// The client is configured for durability over throughput: acks=all means the
// broker only acknowledges once every in-sync replica has the record, and
// franz-go's idempotent producer (enabled by default alongside acks=all)
// prevents an internal retry from writing the record twice.
func NewProducer(cfg ProducerConfig, providers *obs.Providers, metrics *obs.KafkaMetrics, log *slog.Logger) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka producer requires at least one broker")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka producer requires a topic")
	}
	if cfg.ProduceTimeout <= 0 {
		cfg.ProduceTimeout = 10 * time.Second
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression(), kgo.NoCompression()),
		// Bound how long a single record may block before the caller is told
		// the write failed, so an HTTP request never hangs on a dead broker.
		kgo.RecordDeliveryTimeout(cfg.ProduceTimeout),
		kgo.WithLogger(newKgoLogger(log)),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	return &Producer{
		client:     client,
		topic:      cfg.Topic,
		timeout:    cfg.ProduceTimeout,
		propagator: providers.Propagator,
		tracer:     providers.Tracer("sentinelflow/kafka-producer"),
		metrics:    metrics,
		log:        log,
	}, nil
}

// Publish serialises ev and writes it to the configured topic.
//
// It blocks until the broker acknowledges the write or the produce timeout
// expires. Returning the delivery error to the caller is what allows the
// ingestion API to answer 503 instead of pretending an event was accepted.
func (p *Producer) Publish(ctx context.Context, ev event.Event) error {
	ctx, span := p.tracer.Start(ctx, "publish "+p.topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", p.topic),
			attribute.String("messaging.operation.name", "publish"),
			attribute.String("sentinelflow.event.id", ev.EventID),
			attribute.String("sentinelflow.tenant.id", ev.TenantID),
		),
	)
	defer span.End()

	payload, err := json.Marshal(ev)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "encode event")
		return fmt.Errorf("encode event %s: %w", ev.EventID, err)
	}

	record := NewRecord(p.topic, ev, payload)

	// Inject the current trace context so the consumer's span links back to the
	// HTTP request that produced the event.
	p.propagator.Inject(ctx, NewRecordCarrier(record))

	produceCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	start := time.Now()
	results := p.client.ProduceSync(produceCtx, record)
	elapsed := time.Since(start)

	if err := results.FirstErr(); err != nil {
		p.metrics.RecordPublish(ctx, p.topic, "error", elapsed)
		span.RecordError(err)
		span.SetStatus(codes.Error, "produce failed")
		p.log.ErrorContext(ctx, "kafka produce failed",
			slog.String("topic", p.topic),
			slog.String("event_id", ev.EventID),
			slog.String("partition_key", ev.PartitionKey()),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("produce event %s to %s: %w", ev.EventID, p.topic, err)
	}

	p.metrics.RecordPublish(ctx, p.topic, "success", elapsed)

	// The acknowledged record carries the partition and offset the broker chose.
	acked := results[0].Record
	span.SetAttributes(
		attribute.Int("messaging.destination.partition.id", int(acked.Partition)),
		attribute.Int64("messaging.kafka.offset", acked.Offset),
	)

	p.log.InfoContext(ctx, "event published",
		slog.String("topic", p.topic),
		slog.String("event_id", ev.EventID),
		slog.String("tenant_id", ev.TenantID),
		slog.String("service_name", ev.ServiceName),
		slog.String("partition_key", ev.PartitionKey()),
		slog.Int("partition", int(acked.Partition)),
		slog.Int64("offset", acked.Offset),
		slog.Int64("duration_ms", elapsed.Milliseconds()),
	)

	return nil
}

// PublishBatch writes every event in one produce call and blocks until the
// broker has acknowledged all of them.
//
// The durability contract is unchanged and deliberately all-or-nothing: this
// returns an error if any single record failed, so a 202 still means every event
// in the request is durable. The caller retries the whole batch, and the
// engine's idempotent insert collapses whatever was already written.
func (p *Producer) PublishBatch(ctx context.Context, events []event.Event) error {
	if len(events) == 0 {
		return nil
	}

	ctx, span := p.tracer.Start(ctx, "publish batch "+p.topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", p.topic),
			attribute.String("messaging.operation.name", "publish"),
			attribute.Int("messaging.batch.message_count", len(events)),
		),
	)
	defer span.End()

	records := make([]*kgo.Record, 0, len(events))
	for _, ev := range events {
		payload, err := json.Marshal(ev)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "encode event")
			return fmt.Errorf("encode event %s: %w", ev.EventID, err)
		}

		record := NewRecord(p.topic, ev, payload)
		p.propagator.Inject(ctx, NewRecordCarrier(record))
		records = append(records, record)
	}

	produceCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	start := time.Now()
	results := p.client.ProduceSync(produceCtx, records...)
	elapsed := time.Since(start)

	if err := results.FirstErr(); err != nil {
		p.metrics.RecordPublish(ctx, p.topic, "error", elapsed)
		span.RecordError(err)
		span.SetStatus(codes.Error, "produce failed")
		p.log.ErrorContext(ctx, "kafka batch produce failed",
			slog.String("topic", p.topic),
			slog.Int("events", len(events)),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("produce %d events to %s: %w", len(events), p.topic, err)
	}

	p.metrics.RecordPublish(ctx, p.topic, "success", elapsed)

	p.log.InfoContext(ctx, "event batch published",
		slog.String("topic", p.topic),
		slog.Int("events", len(events)),
		slog.Int64("duration_ms", elapsed.Milliseconds()),
	)
	return nil
}

// NewRecord builds the Kafka record for an event.
//
// The key is the event's partition key, which is what makes every event from a
// given tenant and service land on one partition and stay in order.
func NewRecord(topic string, ev event.Event, payload []byte) *kgo.Record {
	return &kgo.Record{
		Topic: topic,
		Key:   []byte(ev.PartitionKey()),
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: HeaderSchemaVersion, Value: []byte(ev.SchemaVersion)},
			{Key: HeaderContentType, Value: []byte("application/json")},
		},
	}
}

// Ping verifies broker connectivity; it backs the readiness probe.
func (p *Producer) Ping(ctx context.Context) error {
	if err := p.client.Ping(ctx); err != nil {
		return fmt.Errorf("ping kafka: %w", err)
	}
	return nil
}

// Close flushes buffered records and releases the client.
//
// Flush is bounded by ctx so shutdown cannot hang on an unreachable broker; any
// record that still cannot be delivered is reported rather than dropped
// silently.
func (p *Producer) Close(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		p.client.Close()
		return fmt.Errorf("flush kafka producer: %w", err)
	}
	p.client.Close()
	return nil
}
