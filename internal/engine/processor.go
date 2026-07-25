// Package engine consumes telemetry events from Kafka and persists them.
//
// It stops at normalized storage. Turning that stream into incidents belongs to
// the correlate package, kept deliberately separate so a correlation failure
// cannot stall the consume loop.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

// EventStore persists a normalized event and reports whether it was new.
type EventStore interface {
	Insert(ctx context.Context, ev event.Event, receivedAt time.Time) (bool, error)
}

// Message is one Kafka record, decoupled from the Kafka client type so that the
// processor can be tested without constructing broker plumbing.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// Outcome is what the processor decided to do with a message.
type Outcome string

const (
	// OutcomeStored means a new row was written.
	OutcomeStored Outcome = "stored"
	// OutcomeDuplicate means the event ID was already present. This is the
	// expected result of an at-least-once redelivery, not a failure.
	OutcomeDuplicate Outcome = "duplicate"
	// OutcomeInvalid means the message can never be processed: it is not valid
	// JSON, or it violates the event contract. Retrying cannot help.
	OutcomeInvalid Outcome = "invalid"
	// OutcomeFailed means processing failed in a way that may succeed later.
	OutcomeFailed Outcome = "failed"
)

// RetryPolicy bounds how long the processor will keep trying a transient error.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// Delay returns the backoff before the given attempt (1-based).
//
// Backoff is exponential and capped. There is no jitter because a single
// consumer per partition cannot form a thundering herd against itself, and its
// absence keeps the tests deterministic.
func (p RetryPolicy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := p.BaseDelay
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	maxDelay := p.MaxDelay
	if maxDelay <= 0 {
		maxDelay = 5 * time.Second
	}

	// Cap the exponent before shifting so a large attempt count cannot overflow.
	exp := math.Min(float64(attempt-1), 20)
	delay := time.Duration(float64(base) * math.Pow(2, exp))
	if delay > maxDelay || delay <= 0 {
		delay = maxDelay
	}
	return delay
}

// ErrPermanent marks a message that must not be retried.
var ErrPermanent = errors.New("permanent processing failure")

// Processor turns one Kafka message into at most one database row.
type Processor struct {
	store  EventStore
	retry  RetryPolicy
	bounds event.TimeBounds
	log    *slog.Logger
	// sleep is injectable so retry tests do not spend real wall-clock time.
	sleep func(ctx context.Context, d time.Duration) error
	// now is injectable so the time-bound check is testable without waiting.
	now func() time.Time
}

// ProcessorOptions configures a Processor.
type ProcessorOptions struct {
	Store  EventStore
	Retry  RetryPolicy
	Logger *slog.Logger

	// Bounds must carry a future bound only. Setting MaxAge here would classify
	// any backlog older than it as permanently invalid and discard it; see
	// event.TimeBounds. NewProcessor drops MaxAge rather than trusting callers
	// to remember.
	Bounds event.TimeBounds

	Sleep func(ctx context.Context, d time.Duration) error
	Now   func() time.Time
}

// NewProcessor builds a Processor.
func NewProcessor(opts ProcessorOptions) *Processor {
	if opts.Retry.MaxAttempts < 1 {
		opts.Retry.MaxAttempts = 5
	}
	if opts.Sleep == nil {
		opts.Sleep = sleepContext
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Processor{
		store: opts.Store,
		retry: opts.Retry,
		// Only the future bound survives. A MaxAge here would mean that draining
		// a backlog older than it deleted the backlog, so the unsafe half of the
		// configuration is discarded at construction rather than documented and
		// hoped for.
		bounds: event.TimeBounds{MaxFuture: opts.Bounds.MaxFuture},
		log:    opts.Logger,
		sleep:  opts.Sleep,
		now:    opts.Now,
	}
}

// Process decodes, revalidates and stores one message.
//
// The returned error is non-nil only when the caller must not commit the
// offset. An invalid message returns OutcomeInvalid with a nil error: it is
// logged loudly and its offset is committed, because a permanently undecodable
// record would otherwise stall its partition forever.
func (p *Processor) Process(ctx context.Context, msg Message) (Outcome, error) {
	var ev event.Event
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		p.logInvalid(ctx, msg, "message is not a valid telemetry event", err)
		return OutcomeInvalid, nil
	}

	// Revalidate on the way out of Kafka. The ingestion API is the usual
	// producer, but the topic is not private to it, and a schema regression
	// must not be able to write malformed rows.
	//
	// The future bound is re-applied here for the same reason, and is safe to
	// re-apply because now only moves forward: anything that passed it at
	// ingestion still passes it. How old the record is, by contrast, is never
	// grounds for discarding it — that is just how backlogs look.
	ev.Normalize()
	if err := ev.ValidateWithin(p.bounds, p.now()); err != nil {
		p.logInvalid(ctx, msg, "message violates the telemetry event contract", err)
		return OutcomeInvalid, nil
	}

	// The producer's record timestamp is when the ingestion API accepted the
	// event, which is exactly the "received_at" the schema wants.
	receivedAt := msg.Timestamp
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}

	inserted, err := p.insertWithRetry(ctx, ev, receivedAt)
	if err != nil {
		return OutcomeFailed, err
	}

	if !inserted {
		p.log.InfoContext(ctx, "duplicate event ignored",
			slog.String("event_id", ev.EventID),
			slog.String("tenant_id", ev.TenantID),
			slog.String("service_name", ev.ServiceName),
			slog.Int("partition", int(msg.Partition)),
			slog.Int64("offset", msg.Offset),
		)
		return OutcomeDuplicate, nil
	}

	p.log.InfoContext(ctx, "event stored",
		slog.String("event_id", ev.EventID),
		slog.String("tenant_id", ev.TenantID),
		slog.String("service_name", ev.ServiceName),
		slog.String("event_type", ev.EventType),
		slog.String("severity", string(ev.Severity)),
		slog.String("event_trace_id", ev.TraceID),
		slog.Int("partition", int(msg.Partition)),
		slog.Int64("offset", msg.Offset),
	)

	return OutcomeStored, nil
}

// insertWithRetry retries only errors the store classifies as transient.
//
// Retrying an insert is safe precisely because the statement is idempotent: if
// a previous attempt actually committed before the connection dropped, the
// retry lands on ON CONFLICT DO NOTHING instead of creating a second row.
func (p *Processor) insertWithRetry(ctx context.Context, ev event.Event, receivedAt time.Time) (bool, error) {
	var lastErr error

	for attempt := 1; attempt <= p.retry.MaxAttempts; attempt++ {
		inserted, err := p.store.Insert(ctx, ev, receivedAt)
		if err == nil {
			return inserted, nil
		}
		lastErr = err

		if !store.IsRetryable(err) {
			p.log.ErrorContext(ctx, "permanent database failure",
				slog.String("event_id", ev.EventID),
				slog.Int("attempt", attempt),
				slog.String("error", err.Error()),
			)
			return false, fmt.Errorf("%w: store event %s: %w", ErrPermanent, ev.EventID, err)
		}

		if attempt == p.retry.MaxAttempts {
			break
		}

		delay := p.retry.Delay(attempt)
		p.log.WarnContext(ctx, "transient database failure, retrying",
			slog.String("event_id", ev.EventID),
			slog.Int("attempt", attempt),
			slog.Int("max_attempts", p.retry.MaxAttempts),
			slog.Duration("backoff", delay),
			slog.String("error", err.Error()),
		)

		if err := p.sleep(ctx, delay); err != nil {
			return false, fmt.Errorf("store event %s: %w", ev.EventID, err)
		}
	}

	p.log.ErrorContext(ctx, "database retries exhausted",
		slog.String("event_id", ev.EventID),
		slog.Int("attempts", p.retry.MaxAttempts),
		slog.String("error", lastErr.Error()),
	)

	return false, fmt.Errorf("store event %s after %d attempts: %w", ev.EventID, p.retry.MaxAttempts, lastErr)
}

// logInvalid records a poison message with enough context to find it again on
// the topic. The offset is committed afterwards, so this log line is the only
// durable trace of the discarded record until a dead letter topic is added.
func (p *Processor) logInvalid(ctx context.Context, msg Message, reason string, err error) {
	p.log.ErrorContext(ctx, "discarding invalid message",
		slog.String("reason", reason),
		slog.String("error", err.Error()),
		slog.String("topic", msg.Topic),
		slog.Int("partition", int(msg.Partition)),
		slog.Int64("offset", msg.Offset),
		slog.String("key", string(msg.Key)),
		slog.Int("value_bytes", len(msg.Value)),
	)
}

// sleepContext waits for d unless ctx is cancelled first.
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
