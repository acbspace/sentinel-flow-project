package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/acbspace/sentinel-flow-project/internal/kafkax"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
)

// Runner owns the consume loop: poll, process, commit, repeat.
type Runner struct {
	consumer   *kafkax.Consumer
	processor  *Processor
	metrics    *obs.KafkaMetrics
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	log        *slog.Logger
	topic      string
	maxRecords int
}

// RunnerOptions configures a Runner.
type RunnerOptions struct {
	Consumer   *kafkax.Consumer
	Processor  *Processor
	Metrics    *obs.KafkaMetrics
	Providers  *obs.Providers
	Logger     *slog.Logger
	Topic      string
	MaxRecords int
}

// NewRunner builds the consume loop.
func NewRunner(opts RunnerOptions) *Runner {
	if opts.MaxRecords < 1 {
		opts.MaxRecords = 250
	}
	return &Runner{
		consumer:   opts.Consumer,
		processor:  opts.Processor,
		metrics:    opts.Metrics,
		tracer:     opts.Providers.Tracer("sentinelflow/incident-engine"),
		propagator: opts.Providers.Propagator,
		log:        opts.Logger,
		topic:      opts.Topic,
		maxRecords: opts.MaxRecords,
	}
}

// Run consumes until ctx is cancelled.
//
// The loop's contract is that offsets advance only past records whose rows are
// already committed in PostgreSQL. On any error that leaves that in doubt, Run
// returns without committing; the records are redelivered to whichever member
// takes over the partition.
func (r *Runner) Run(ctx context.Context) error {
	r.log.Info("consumer loop starting",
		slog.String("topic", r.topic),
		slog.Int("max_poll_records", r.maxRecords),
	)

	for {
		if ctx.Err() != nil {
			r.log.Info("consumer loop stopping", slog.String("reason", "context cancelled"))
			return nil
		}

		records, fetchErrs, closed := r.consumer.Poll(ctx, r.maxRecords)
		if closed {
			r.log.Info("consumer loop stopping", slog.String("reason", "client closed"))
			return nil
		}

		for _, err := range fetchErrs {
			// Fetch errors are surfaced, not swallowed. franz-go retries the
			// underlying request, so these are informational unless persistent.
			r.log.Error("kafka fetch error", slog.String("error", err.Error()))
		}

		if len(records) == 0 {
			r.consumer.AllowRebalance()
			continue
		}

		if err := r.processBatch(ctx, records); err != nil {
			r.consumer.AllowRebalance()
			if errors.Is(err, context.Canceled) {
				r.log.Info("consumer loop stopping", slog.String("reason", "context cancelled during processing"))
				return nil
			}
			return err
		}

		// Commit only after every record in the batch is durable.
		if err := r.commit(ctx, records); err != nil {
			r.consumer.AllowRebalance()
			if errors.Is(err, context.Canceled) {
				r.log.Warn("shutdown before offsets were committed; records will be redelivered")
				return nil
			}
			return err
		}

		// Release the rebalance block taken by Poll now that the batch is done.
		r.consumer.AllowRebalance()
	}
}

// processBatch handles every record, stopping at the first record that must not
// be committed.
func (r *Runner) processBatch(ctx context.Context, records []*kgo.Record) error {
	for _, rec := range records {
		if err := r.processRecord(ctx, rec); err != nil {
			return err
		}
	}
	return nil
}

// processRecord processes one record inside a span linked to the producer.
func (r *Runner) processRecord(ctx context.Context, rec *kgo.Record) error {
	// Continue the trace that started in the demo service's HTTP handler.
	msgCtx := r.propagator.Extract(ctx, kafkax.NewRecordCarrier(rec))

	msgCtx, span := r.tracer.Start(msgCtx, "process "+rec.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.source.name", rec.Topic),
			attribute.String("messaging.operation.name", "process"),
			attribute.Int("messaging.destination.partition.id", int(rec.Partition)),
			attribute.Int64("messaging.kafka.offset", rec.Offset),
		),
	)
	defer span.End()

	start := time.Now()
	outcome, err := r.processor.Process(msgCtx, Message{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Key:       rec.Key,
		Value:     rec.Value,
		Timestamp: rec.Timestamp,
	})
	elapsed := time.Since(start)

	r.metrics.RecordConsume(msgCtx, rec.Topic, string(outcome), elapsed)
	span.SetAttributes(attribute.String("sentinelflow.outcome", string(outcome)))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "processing failed")
		return fmt.Errorf("process %s[%d]@%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
	}

	return nil
}

// commit persists the batch's offsets, using a deadline independent of ctx so
// that a shutdown still gets the chance to record work already done.
func (r *Runner) commit(ctx context.Context, records []*kgo.Record) error {
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()

	if err := r.consumer.Commit(commitCtx, records); err != nil {
		r.log.Error("offset commit failed; records will be redelivered",
			slog.String("error", err.Error()),
			slog.Int("records", len(records)),
		)
		return err
	}

	last := records[len(records)-1]
	r.log.Debug("offsets committed",
		slog.Int("records", len(records)),
		slog.Int("last_partition", int(last.Partition)),
		slog.Int64("last_offset", last.Offset),
	)
	return nil
}
