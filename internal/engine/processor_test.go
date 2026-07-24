package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/acbspace/sentinel-flow-project/internal/engine"
	"github.com/acbspace/sentinel-flow-project/internal/event"
)

const validUUID = "6f1c4d5e-8b3a-4c2d-9e7f-1a2b3c4d5e6f"

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// fakeStore stands in for PostgreSQL. It records every insert and can be
// scripted to fail a fixed number of times, which is what makes the retry
// behaviour testable without a database or real sleeping.
type fakeStore struct {
	mu sync.Mutex
	// seen tracks event IDs already stored, mirroring the unique constraint.
	seen map[string]bool
	// failures is the queue of errors to return before succeeding.
	failures []error
	calls    int
	inserted []event.Event
}

func newFakeStore(failures ...error) *fakeStore {
	return &fakeStore{seen: make(map[string]bool), failures: failures}
}

func (s *fakeStore) Insert(_ context.Context, ev event.Event, _ time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++

	if len(s.failures) > 0 {
		err := s.failures[0]
		s.failures = s.failures[1:]
		return false, err
	}

	if s.seen[ev.EventID] {
		// Exactly what ON CONFLICT DO NOTHING reports: no error, no new row.
		return false, nil
	}
	s.seen[ev.EventID] = true
	s.inserted = append(s.inserted, ev)
	return true, nil
}

func (s *fakeStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *fakeStore) insertedEvents() []event.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]event.Event(nil), s.inserted...)
}

func validEvent() event.Event {
	return event.Event{
		EventID:       validUUID,
		SchemaVersion: event.SchemaVersion10,
		TenantID:      "demo-tenant",
		ServiceName:   "payment-service",
		Environment:   "local",
		EventType:     "request.completed",
		Severity:      event.SeverityInfo,
		Timestamp:     event.NewTimestamp(time.Date(2026, 7, 22, 10, 30, 0, 0, time.UTC)),
		TraceID:       "0af7651916cd43dd8448eb211c80319c",
		Attributes:    event.Attributes{"latency_ms": 125},
	}
}

func messageFor(t *testing.T, ev event.Event) engine.Message {
	t.Helper()

	payload, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return engine.Message{
		Topic:     "telemetry.events.v1",
		Partition: 0,
		Offset:    1,
		Key:       []byte(ev.PartitionKey()),
		Value:     payload,
		Timestamp: time.Date(2026, 7, 22, 10, 30, 1, 0, time.UTC),
	}
}

// newProcessor builds a processor whose sleep is instant, so retry tests run at
// full speed while still exercising the real backoff bookkeeping.
func newProcessor(t *testing.T, store engine.EventStore, attempts int) (*engine.Processor, *[]time.Duration) {
	t.Helper()

	var slept []time.Duration
	p := engine.NewProcessor(engine.ProcessorOptions{
		Store: store,
		Retry: engine.RetryPolicy{
			MaxAttempts: attempts,
			BaseDelay:   10 * time.Millisecond,
			MaxDelay:    100 * time.Millisecond,
		},
		Logger: discardLogger(),
		Sleep: func(_ context.Context, d time.Duration) error {
			slept = append(slept, d)
			return nil
		},
	})
	return p, &slept
}

func TestProcessStoresValidEvent(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	processor, _ := newProcessor(t, store, 3)

	ev := validEvent()
	outcome, err := processor.Process(context.Background(), messageFor(t, ev))

	if err != nil {
		t.Fatalf("Process() = %v, want nil", err)
	}
	if outcome != engine.OutcomeStored {
		t.Errorf("outcome = %q, want %q", outcome, engine.OutcomeStored)
	}

	inserted := store.insertedEvents()
	if len(inserted) != 1 {
		t.Fatalf("stored %d events, want 1", len(inserted))
	}
	if inserted[0].EventID != ev.EventID {
		t.Errorf("stored EventID = %q, want %q", inserted[0].EventID, ev.EventID)
	}
	if inserted[0].Attributes["latency_ms"] != float64(125) {
		t.Errorf("attributes were not preserved: %v", inserted[0].Attributes)
	}
}

func TestProcessIgnoresDuplicateEvents(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	processor, _ := newProcessor(t, store, 3)

	ev := validEvent()
	msg := messageFor(t, ev)

	// First delivery stores the event.
	outcome, err := processor.Process(context.Background(), msg)
	if err != nil {
		t.Fatalf("first Process() = %v, want nil", err)
	}
	if outcome != engine.OutcomeStored {
		t.Fatalf("first outcome = %q, want %q", outcome, engine.OutcomeStored)
	}

	// A redelivery of the same record, which at-least-once delivery makes
	// inevitable, must be absorbed rather than duplicated or rejected.
	redelivered := msg
	redelivered.Offset = msg.Offset + 1

	outcome, err = processor.Process(context.Background(), redelivered)
	if err != nil {
		t.Fatalf("redelivery Process() = %v, want nil", err)
	}
	if outcome != engine.OutcomeDuplicate {
		t.Errorf("redelivery outcome = %q, want %q", outcome, engine.OutcomeDuplicate)
	}

	if n := len(store.insertedEvents()); n != 1 {
		t.Errorf("stored %d events, want 1", n)
	}
}

func TestProcessIgnoresDuplicateFromADifferentPartition(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	processor, _ := newProcessor(t, store, 3)

	ev := validEvent()

	first := messageFor(t, ev)
	if _, err := processor.Process(context.Background(), first); err != nil {
		t.Fatalf("first Process() = %v", err)
	}

	// The same event ID arriving on another partition (for example after a key
	// change or a producer replay) is still a duplicate: the database, not the
	// partition assignment, is the authority on identity.
	second := first
	second.Partition = 2
	second.Offset = 0

	outcome, err := processor.Process(context.Background(), second)
	if err != nil {
		t.Fatalf("second Process() = %v", err)
	}
	if outcome != engine.OutcomeDuplicate {
		t.Errorf("outcome = %q, want %q", outcome, engine.OutcomeDuplicate)
	}
	if n := len(store.insertedEvents()); n != 1 {
		t.Errorf("stored %d events, want 1", n)
	}
}

func TestProcessDiscardsUnprocessableMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value []byte
	}{
		{
			name:  "not JSON at all",
			value: []byte("this is not json"),
		},
		{
			name:  "JSON but not an object",
			value: []byte(`["nope"]`),
		},
		{
			name:  "empty payload",
			value: []byte(""),
		},
		{
			name:  "object missing every required field",
			value: []byte(`{}`),
		},
		{
			name:  "event id is not a uuid",
			value: []byte(`{"event_id":"nope","schema_version":"1.0","tenant_id":"t","service_name":"s","event_type":"e","severity":"info","timestamp":"2026-07-22T10:30:00Z"}`),
		},
		{
			name:  "unsupported schema version",
			value: []byte(`{"event_id":"` + validUUID + `","schema_version":"99.0","tenant_id":"t","service_name":"s","event_type":"e","severity":"info","timestamp":"2026-07-22T10:30:00Z"}`),
		},
		{
			name:  "unsupported severity",
			value: []byte(`{"event_id":"` + validUUID + `","schema_version":"1.0","tenant_id":"t","service_name":"s","event_type":"e","severity":"nuclear","timestamp":"2026-07-22T10:30:00Z"}`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := newFakeStore()
			processor, _ := newProcessor(t, store, 3)

			outcome, err := processor.Process(context.Background(), engine.Message{
				Topic: "telemetry.events.v1",
				Value: tc.value,
			})

			// A poison message returns no error: the offset must advance, or
			// this record stalls its partition forever.
			if err != nil {
				t.Fatalf("Process() = %v, want nil so the offset can advance", err)
			}
			if outcome != engine.OutcomeInvalid {
				t.Errorf("outcome = %q, want %q", outcome, engine.OutcomeInvalid)
			}
			if store.callCount() != 0 {
				t.Errorf("store was called %d times, want 0", store.callCount())
			}
		})
	}
}

func TestProcessRetriesTransientFailures(t *testing.T) {
	t.Parallel()

	// Two connection failures then success.
	transient := &net.OpError{Op: "dial", Err: errors.New("connection refused")}
	store := newFakeStore(transient, transient)
	processor, slept := newProcessor(t, store, 5)

	outcome, err := processor.Process(context.Background(), messageFor(t, validEvent()))

	if err != nil {
		t.Fatalf("Process() = %v, want nil after successful retry", err)
	}
	if outcome != engine.OutcomeStored {
		t.Errorf("outcome = %q, want %q", outcome, engine.OutcomeStored)
	}
	if store.callCount() != 3 {
		t.Errorf("store called %d times, want 3 (two failures then success)", store.callCount())
	}
	if len(*slept) != 2 {
		t.Fatalf("backed off %d times, want 2", len(*slept))
	}
	// Backoff must actually grow rather than hammering at a fixed interval.
	if (*slept)[1] <= (*slept)[0] {
		t.Errorf("backoff did not increase: %v then %v", (*slept)[0], (*slept)[1])
	}
}

func TestProcessStopsAfterRetryBudgetIsExhausted(t *testing.T) {
	t.Parallel()

	transient := &net.OpError{Op: "dial", Err: errors.New("connection refused")}
	store := newFakeStore(transient, transient, transient, transient, transient)
	processor, slept := newProcessor(t, store, 3)

	outcome, err := processor.Process(context.Background(), messageFor(t, validEvent()))

	// Returning an error is what stops the offset from being committed, so the
	// records are redelivered instead of silently lost.
	if err == nil {
		t.Fatal("Process() = nil, want an error so the offset is not committed")
	}
	if outcome != engine.OutcomeFailed {
		t.Errorf("outcome = %q, want %q", outcome, engine.OutcomeFailed)
	}
	if store.callCount() != 3 {
		t.Errorf("store called %d times, want exactly the 3 attempt budget", store.callCount())
	}
	if len(*slept) != 2 {
		t.Errorf("backed off %d times, want 2 (no sleep after the final attempt)", len(*slept))
	}
}

func TestProcessDoesNotRetryPermanentFailures(t *testing.T) {
	t.Parallel()

	// 23514 is check_violation: the same row will be rejected every time.
	permanent := &pgconn.PgError{Code: "23514", Message: "check constraint violated"}
	store := newFakeStore(permanent, permanent, permanent)
	processor, slept := newProcessor(t, store, 5)

	outcome, err := processor.Process(context.Background(), messageFor(t, validEvent()))

	if err == nil {
		t.Fatal("Process() = nil, want an error")
	}
	if !errors.Is(err, engine.ErrPermanent) {
		t.Errorf("error %v is not marked permanent", err)
	}
	if outcome != engine.OutcomeFailed {
		t.Errorf("outcome = %q, want %q", outcome, engine.OutcomeFailed)
	}
	if store.callCount() != 1 {
		t.Errorf("store called %d times, want 1 (no retry on a permanent error)", store.callCount())
	}
	if len(*slept) != 0 {
		t.Errorf("backed off %d times, want 0", len(*slept))
	}
}

func TestProcessNormalizesBeforeStoring(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	processor, _ := newProcessor(t, store, 3)

	// A producer that bypassed the ingestion API could write sloppy values
	// straight to the topic; the engine must normalize them anyway.
	raw := []byte(`{
		"event_id":"` + validUUID + `",
		"schema_version":"1.0",
		"tenant_id":"  demo-tenant  ",
		"service_name":"payment-service",
		"event_type":"request.completed",
		"severity":"INFO",
		"timestamp":"2026-07-22T12:30:00+02:00"
	}`)

	outcome, err := processor.Process(context.Background(), engine.Message{
		Topic: "telemetry.events.v1",
		Value: raw,
	})
	if err != nil {
		t.Fatalf("Process() = %v", err)
	}
	if outcome != engine.OutcomeStored {
		t.Fatalf("outcome = %q, want %q", outcome, engine.OutcomeStored)
	}

	inserted := store.insertedEvents()
	if len(inserted) != 1 {
		t.Fatalf("stored %d events, want 1", len(inserted))
	}

	got := inserted[0]
	if got.TenantID != "demo-tenant" {
		t.Errorf("TenantID = %q, want %q", got.TenantID, "demo-tenant")
	}
	if got.Severity != event.SeverityInfo {
		t.Errorf("Severity = %q, want %q", got.Severity, event.SeverityInfo)
	}
	if got.Environment != event.DefaultEnvironment {
		t.Errorf("Environment = %q, want %q", got.Environment, event.DefaultEnvironment)
	}
	if want := time.Date(2026, 7, 22, 10, 30, 0, 0, time.UTC); !got.Timestamp.Equal(want) {
		t.Errorf("Timestamp = %v, want %v", got.Timestamp.Time, want)
	}
}

func TestProcessStopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	transient := &net.OpError{Op: "dial", Err: errors.New("connection refused")}
	store := newFakeStore(transient, transient, transient)

	processor := engine.NewProcessor(engine.ProcessorOptions{
		Store:  store,
		Retry:  engine.RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
		Logger: discardLogger(),
		// A shutdown arriving mid-backoff must abandon the retry loop rather
		// than finishing its budget.
		Sleep: func(context.Context, time.Duration) error { return context.Canceled },
	})

	outcome, err := processor.Process(context.Background(), messageFor(t, validEvent()))

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Process() = %v, want context.Canceled", err)
	}
	if outcome != engine.OutcomeFailed {
		t.Errorf("outcome = %q, want %q", outcome, engine.OutcomeFailed)
	}
	if store.callCount() != 1 {
		t.Errorf("store called %d times, want 1 before the cancellation was noticed", store.callCount())
	}
}

func TestRetryPolicyDelay(t *testing.T) {
	t.Parallel()

	policy := engine.RetryPolicy{MaxAttempts: 10, BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 100 * time.Millisecond},
		{attempt: 2, want: 200 * time.Millisecond},
		{attempt: 3, want: 400 * time.Millisecond},
		{attempt: 4, want: 800 * time.Millisecond},
		// Capped at MaxDelay from here on.
		{attempt: 5, want: time.Second},
		{attempt: 20, want: time.Second},
		// A very large attempt count must not overflow into a negative delay.
		{attempt: 1000, want: time.Second},
	}

	for _, tc := range tests {
		if got := policy.Delay(tc.attempt); got != tc.want {
			t.Errorf("Delay(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}
