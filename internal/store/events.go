package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
)

// insertEventSQL is the single write path for telemetry.
//
// ON CONFLICT DO NOTHING on the primary key is the idempotency mechanism for the
// whole pipeline: because Kafka delivery is at-least-once, the engine will
// occasionally see a record it has already stored, and the database is the one
// place that can decide that question atomically under concurrency.
//
// The conflict target is (event_timestamp, event_id), not event_id alone,
// because telemetry_events is range-partitioned on event_timestamp and
// PostgreSQL requires the partition key in every unique constraint. This still
// collapses redeliveries: a replayed Kafka record is byte-identical, so it
// carries the same timestamp, routes to the same partition, and conflicts. See
// migration 0006 for the guarantee this narrows and why that is acceptable.
const insertEventSQL = `
INSERT INTO telemetry_events (
    event_id, schema_version, tenant_id, service_name, environment,
    event_type, severity, event_timestamp, trace_id, attributes, received_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (event_timestamp, event_id) DO NOTHING`

const countEventsSQL = `SELECT count(*) FROM telemetry_events`

// EventStore persists telemetry events.
type EventStore struct {
	pool    *pgxpool.Pool
	metrics *obs.DBMetrics
	timeout time.Duration
}

// NewEventStore builds an event store over an existing pool.
func NewEventStore(pool *pgxpool.Pool, metrics *obs.DBMetrics, timeout time.Duration) *EventStore {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &EventStore{pool: pool, metrics: metrics, timeout: timeout}
}

// Insert stores ev and reports whether it was new.
//
// A false return means an event with the same ID was already stored, which is
// the expected outcome of a Kafka redelivery and is not an error.
func (s *EventStore) Insert(ctx context.Context, ev event.Event, receivedAt time.Time) (bool, error) {
	attributes, err := json.Marshal(ev.Attributes)
	if err != nil {
		// Non-retryable: the same payload will fail to encode every time.
		s.metrics.Record(ctx, "insert_event", "error", 0)
		return false, fmt.Errorf("encode attributes for event %s: %w", ev.EventID, err)
	}
	if ev.Attributes == nil {
		attributes = []byte("{}")
	}

	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	tag, err := s.pool.Exec(ctx, insertEventSQL,
		ev.EventID,
		ev.SchemaVersion,
		ev.TenantID,
		ev.ServiceName,
		ev.Environment,
		ev.EventType,
		string(ev.Severity),
		ev.Timestamp.Time,
		ev.TraceID,
		attributes,
		receivedAt,
	)
	elapsed := time.Since(start)

	if err != nil {
		s.metrics.Record(ctx, "insert_event", "error", elapsed)
		return false, fmt.Errorf("insert event %s: %w", ev.EventID, err)
	}

	inserted := tag.RowsAffected() == 1
	outcome := "duplicate"
	if inserted {
		outcome = "inserted"
	}
	s.metrics.Record(ctx, "insert_event", outcome, elapsed)

	return inserted, nil
}

// Count returns the number of stored events. It exists for the readiness of
// operators rather than of the process: the demo and the integration test use
// it to assert that the pipeline actually wrote something.
func (s *EventStore) Count(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	var count int64
	err := s.pool.QueryRow(ctx, countEventsSQL).Scan(&count)
	elapsed := time.Since(start)

	if err != nil {
		s.metrics.Record(ctx, "count_events", "error", elapsed)
		return 0, fmt.Errorf("count telemetry events: %w", err)
	}

	s.metrics.Record(ctx, "count_events", "ok", elapsed)
	return count, nil
}

// Ping verifies the pool can still reach PostgreSQL; it backs readiness.
func (s *EventStore) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	return nil
}

// ServiceWindow is the per-service event tally over a time window: the total
// number of events, how many of them were error or critical, and how many of
// those errors are new. It is the input the correlation engine's error-rate rule
// reasons over.
type ServiceWindow struct {
	TenantID    string
	ServiceName string
	Total       int64
	Errors      int64

	// NewErrors is the subset of Errors at or after the countSince bound.
	//
	// Correlation windows overlap: a 60s window evaluated every 15s observes the
	// same event four times. Errors is therefore the right number to judge a rate
	// on and the right evidence to open an incident with, but it must not be what
	// a repeat detection adds to an already-open incident's running total —
	// NewErrors is.
	NewErrors int64
}

// windowStatsSQL aggregates recent events per tenant and service. The FILTER
// clauses count the bad events, and the newly arrived subset of them, in the
// same pass as the total, so one scan yields every number the error-rate rule
// and the incident counter need.
const windowStatsSQL = `
SELECT tenant_id,
       service_name,
       count(*)                                                        AS total,
       count(*) FILTER (WHERE severity IN ('error', 'critical'))        AS errors,
       count(*) FILTER (WHERE severity IN ('error', 'critical')
                          AND event_timestamp >= $2)                    AS new_errors
FROM telemetry_events
WHERE event_timestamp >= $1
GROUP BY tenant_id, service_name`

// WindowStats returns the per-service event tallies for every event at or after
// since, counting separately those at or after countSince. Services with no
// events in the window are simply absent from the result rather than reported as
// zero.
//
// countSince is expected to be within [since, now]; the caller clamps it to the
// window start so that a first cycle, or one that follows a long gap, still
// counts every event the window covers exactly once.
func (s *EventStore) WindowStats(ctx context.Context, since, countSince time.Time) ([]ServiceWindow, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	rows, err := s.pool.Query(ctx, windowStatsSQL, since, countSince)
	if err != nil {
		s.metrics.Record(ctx, "window_stats", "error", time.Since(start))
		return nil, fmt.Errorf("aggregate window stats: %w", err)
	}
	defer rows.Close()

	var windows []ServiceWindow
	for rows.Next() {
		var w ServiceWindow
		if err := rows.Scan(&w.TenantID, &w.ServiceName, &w.Total, &w.Errors, &w.NewErrors); err != nil {
			s.metrics.Record(ctx, "window_stats", "error", time.Since(start))
			return nil, fmt.Errorf("scan window stats: %w", err)
		}
		windows = append(windows, w)
	}
	if err := rows.Err(); err != nil {
		s.metrics.Record(ctx, "window_stats", "error", time.Since(start))
		return nil, fmt.Errorf("aggregate window stats: %w", err)
	}

	s.metrics.Record(ctx, "window_stats", "ok", time.Since(start))
	return windows, nil
}

// StoredEvent is a telemetry event as it exists in the database: the producer's
// event plus the two pipeline timestamps the read API exposes. received_at is
// when the ingestion API accepted it; processed_at is when the engine wrote the
// row, so their difference is the end-to-end pipeline latency.
type StoredEvent struct {
	event.Event
	ReceivedAt  time.Time `json:"received_at"`
	ProcessedAt time.Time `json:"processed_at"`
}

// EventFilter selects and paginates stored events. A zero-valued field is not
// filtered on; Since and Until bound event_timestamp inclusively when set.
//
// After and Offset are alternative ways to page. After is the one to use; Offset
// remains for callers written against the older API.
type EventFilter struct {
	TenantID    string
	ServiceName string
	Severity    string
	EventType   string
	TraceID     string
	Since       time.Time
	Until       time.Time
	Limit       int
	Offset      int
	After       Cursor
}

// storedEventColumns is the read column order for a stored event, matched by
// scanStoredEvent.
const storedEventColumns = `event_id, schema_version, tenant_id, service_name,
	environment, event_type, severity, event_timestamp, trace_id, attributes,
	received_at, processed_at`

// ListEvents returns the stored events matching filter, newest first, and the
// cursor for the next page. The cursor is empty when this page is the last.
func (s *EventStore) ListEvents(ctx context.Context, filter EventFilter) ([]StoredEvent, Cursor, error) {
	query, args, limit := buildEventListQuery(filter)

	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		s.metrics.Record(ctx, "list_events", "error", time.Since(start))
		return nil, Cursor{}, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	var events []StoredEvent
	for rows.Next() {
		ev, err := scanStoredEvent(rows)
		if err != nil {
			s.metrics.Record(ctx, "list_events", "error", time.Since(start))
			return nil, Cursor{}, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		s.metrics.Record(ctx, "list_events", "error", time.Since(start))
		return nil, Cursor{}, fmt.Errorf("list events: %w", err)
	}

	var next Cursor
	if len(events) > limit {
		// The extra row proves there is more; it belongs to the next page.
		events = events[:limit]
		last := events[len(events)-1]
		next = Cursor{Time: last.Timestamp.Time, ID: last.EventID}
	}

	s.metrics.Record(ctx, "list_events", "ok", time.Since(start))
	return events, next, nil
}

// buildEventListQuery renders the SELECT for ListEvents, returning the query,
// its arguments and the page size the caller asked for. Pure, for the same
// reason as buildIncidentListQuery: the placeholder arithmetic is exactly what a
// unit test should pin.
func buildEventListQuery(f EventFilter) (string, []any, int) {
	var b filterBuilder
	if f.TenantID != "" {
		b.add("tenant_id = $%d", f.TenantID)
	}
	if f.ServiceName != "" {
		b.add("service_name = $%d", f.ServiceName)
	}
	if f.Severity != "" {
		b.add("severity = $%d", f.Severity)
	}
	if f.EventType != "" {
		b.add("event_type = $%d", f.EventType)
	}
	if f.TraceID != "" {
		b.add("trace_id = $%d", f.TraceID)
	}
	if !f.Since.IsZero() {
		b.add("event_timestamp >= $%d", f.Since)
	}
	if !f.Until.IsZero() {
		b.add("event_timestamp <= $%d", f.Until)
	}
	b.after("event_timestamp", "event_id", f.After)

	// event_id breaks ties so the order is total. Without it two events sharing a
	// timestamp could straddle a page boundary and one would be skipped.
	page, limit := b.paginate(f.Limit, f.Offset)

	query := "SELECT " + storedEventColumns + " FROM telemetry_events" + b.where() +
		" ORDER BY event_timestamp DESC, event_id DESC" + page
	return query, b.args, limit
}

// scanStoredEvent reads one row in storedEventColumns order.
func scanStoredEvent(row rowScanner) (StoredEvent, error) {
	var (
		se       StoredEvent
		severity string
		ts       time.Time
		attrs    []byte
	)

	if err := row.Scan(
		&se.EventID,
		&se.SchemaVersion,
		&se.TenantID,
		&se.ServiceName,
		&se.Environment,
		&se.EventType,
		&severity,
		&ts,
		&se.TraceID,
		&attrs,
		&se.ReceivedAt,
		&se.ProcessedAt,
	); err != nil {
		return StoredEvent{}, err
	}

	se.Severity = event.Severity(severity)
	se.Timestamp = event.NewTimestamp(ts)
	if len(attrs) > 0 {
		if err := json.Unmarshal(attrs, &se.Attributes); err != nil {
			return StoredEvent{}, fmt.Errorf("decode attributes for event %s: %w", se.EventID, err)
		}
	}
	return se, nil
}
