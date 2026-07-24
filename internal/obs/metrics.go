package obs

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// meterName namespaces every instrument this project defines.
const meterName = "github.com/acbspace/sentinel-flow-project"

// HTTPMetrics records server side request volume and latency.
//
// otelhttp already emits its own request duration histogram; these instruments
// exist so that the dashboards this project cares about (per route, per status
// class) do not depend on the exact semantic conventions version in use.
type HTTPMetrics struct {
	requests metric.Int64Counter
	duration metric.Float64Histogram
}

// NewHTTPMetrics creates the HTTP server instruments.
func NewHTTPMetrics(mp metric.MeterProvider) (*HTTPMetrics, error) {
	meter := mp.Meter(meterName)

	requests, err := meter.Int64Counter(
		"sentinelflow.http.server.requests",
		metric.WithDescription("Number of HTTP requests handled."),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return nil, fmt.Errorf("create http request counter: %w", err)
	}

	duration, err := meter.Float64Histogram(
		"sentinelflow.http.server.duration",
		metric.WithDescription("Duration of HTTP requests handled."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("create http duration histogram: %w", err)
	}

	return &HTTPMetrics{requests: requests, duration: duration}, nil
}

// Record captures one completed request.
func (m *HTTPMetrics) Record(ctx context.Context, method, route string, status int, elapsed time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("http.request.method", method),
		attribute.String("http.route", route),
		attribute.Int("http.response.status_code", status),
	)
	m.requests.Add(ctx, 1, attrs)
	m.duration.Record(ctx, float64(elapsed.Nanoseconds())/float64(time.Millisecond), attrs)
}

// KafkaMetrics records producer and consumer activity.
type KafkaMetrics struct {
	published       metric.Int64Counter
	publishDuration metric.Float64Histogram
	consumed        metric.Int64Counter
	processDuration metric.Float64Histogram
}

// NewKafkaMetrics creates the Kafka instruments.
func NewKafkaMetrics(mp metric.MeterProvider) (*KafkaMetrics, error) {
	meter := mp.Meter(meterName)

	published, err := meter.Int64Counter(
		"sentinelflow.kafka.published",
		metric.WithDescription("Number of records handed to the Kafka producer, by outcome."),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka publish counter: %w", err)
	}

	publishDuration, err := meter.Float64Histogram(
		"sentinelflow.kafka.publish.duration",
		metric.WithDescription("Time spent waiting for a Kafka produce acknowledgement."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka publish duration histogram: %w", err)
	}

	consumed, err := meter.Int64Counter(
		"sentinelflow.kafka.consumed",
		metric.WithDescription("Number of records consumed from Kafka, by outcome."),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consume counter: %w", err)
	}

	processDuration, err := meter.Float64Histogram(
		"sentinelflow.kafka.process.duration",
		metric.WithDescription("Time spent processing one consumed record end to end."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka process duration histogram: %w", err)
	}

	return &KafkaMetrics{
		published:       published,
		publishDuration: publishDuration,
		consumed:        consumed,
		processDuration: processDuration,
	}, nil
}

// RecordPublish captures one produce attempt. outcome is "success" or "error".
func (m *KafkaMetrics) RecordPublish(ctx context.Context, topic, outcome string, elapsed time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("messaging.destination.name", topic),
		attribute.String("outcome", outcome),
	)
	m.published.Add(ctx, 1, attrs)
	m.publishDuration.Record(ctx, float64(elapsed.Nanoseconds())/float64(time.Millisecond), attrs)
}

// RecordConsume captures one processed record. outcome describes what the
// engine did with it: stored, duplicate, invalid or failed.
func (m *KafkaMetrics) RecordConsume(ctx context.Context, topic, outcome string, elapsed time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("messaging.source.name", topic),
		attribute.String("outcome", outcome),
	)
	m.consumed.Add(ctx, 1, attrs)
	m.processDuration.Record(ctx, float64(elapsed.Nanoseconds())/float64(time.Millisecond), attrs)
}

// DBMetrics records database operation volume and latency.
type DBMetrics struct {
	operations metric.Int64Counter
	duration   metric.Float64Histogram
}

// NewDBMetrics creates the database instruments.
func NewDBMetrics(mp metric.MeterProvider) (*DBMetrics, error) {
	meter := mp.Meter(meterName)

	operations, err := meter.Int64Counter(
		"sentinelflow.db.operations",
		metric.WithDescription("Number of database operations, by operation and outcome."),
		metric.WithUnit("{operation}"),
	)
	if err != nil {
		return nil, fmt.Errorf("create db operation counter: %w", err)
	}

	duration, err := meter.Float64Histogram(
		"sentinelflow.db.operation.duration",
		metric.WithDescription("Duration of database operations."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("create db duration histogram: %w", err)
	}

	return &DBMetrics{operations: operations, duration: duration}, nil
}

// Record captures one database call. outcome is "inserted", "duplicate" or
// "error" so that duplicate suppression is observable rather than invisible.
func (m *DBMetrics) Record(ctx context.Context, operation, outcome string, elapsed time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("db.operation.name", operation),
		attribute.String("outcome", outcome),
	)
	m.operations.Add(ctx, 1, attrs)
	m.duration.Record(ctx, float64(elapsed.Nanoseconds())/float64(time.Millisecond), attrs)
}

// CorrelationMetrics records the correlation engine's evaluation cycles and the
// incidents they produce. Every method tolerates a nil receiver so correlation
// can run without a meter wired (which is what the evaluator's unit tests do).
type CorrelationMetrics struct {
	evaluations metric.Int64Counter
	duration    metric.Float64Histogram
	incidents   metric.Int64Counter
}

// NewCorrelationMetrics creates the correlation instruments.
func NewCorrelationMetrics(mp metric.MeterProvider) (*CorrelationMetrics, error) {
	meter := mp.Meter(meterName)

	evaluations, err := meter.Int64Counter(
		"sentinelflow.correlation.evaluations",
		metric.WithDescription("Number of correlation cycles run, by outcome."),
		metric.WithUnit("{evaluation}"),
	)
	if err != nil {
		return nil, fmt.Errorf("create correlation evaluation counter: %w", err)
	}

	duration, err := meter.Float64Histogram(
		"sentinelflow.correlation.evaluation.duration",
		metric.WithDescription("Duration of one correlation cycle."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("create correlation duration histogram: %w", err)
	}

	incidents, err := meter.Int64Counter(
		"sentinelflow.correlation.incidents",
		metric.WithDescription("Number of incidents affected by correlation, by outcome (opened, grouped, resolved)."),
		metric.WithUnit("{incident}"),
	)
	if err != nil {
		return nil, fmt.Errorf("create correlation incident counter: %w", err)
	}

	return &CorrelationMetrics{
		evaluations: evaluations,
		duration:    duration,
		incidents:   incidents,
	}, nil
}

// RecordEvaluation captures one completed cycle. outcome is "ok" or "error".
func (m *CorrelationMetrics) RecordEvaluation(ctx context.Context, outcome string, elapsed time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("outcome", outcome))
	m.evaluations.Add(ctx, 1, attrs)
	m.duration.Record(ctx, float64(elapsed.Nanoseconds())/float64(time.Millisecond), attrs)
}

// RecordIncidents records how many incidents a cycle opened, grouped and
// resolved. Zero counts are skipped so the time series shows only real activity.
func (m *CorrelationMetrics) RecordIncidents(ctx context.Context, opened, grouped, resolved int) {
	if m == nil {
		return
	}
	m.addIncidents(ctx, "opened", opened)
	m.addIncidents(ctx, "grouped", grouped)
	m.addIncidents(ctx, "resolved", resolved)
}

func (m *CorrelationMetrics) addIncidents(ctx context.Context, outcome string, n int) {
	if n <= 0 {
		return
	}
	m.incidents.Add(ctx, int64(n), metric.WithAttributes(attribute.String("outcome", outcome)))
}

// AlertingMetrics records the alert worker's activity. The notification timeline
// itself lives in the database; this counts the workflows the starter launches.
// Its method tolerates a nil receiver, like CorrelationMetrics.
type AlertingMetrics struct {
	workflowsStarted metric.Int64Counter
}

// NewAlertingMetrics creates the alerting instruments.
func NewAlertingMetrics(mp metric.MeterProvider) (*AlertingMetrics, error) {
	meter := mp.Meter(meterName)

	started, err := meter.Int64Counter(
		"sentinelflow.alerting.workflows_started",
		metric.WithDescription("Number of incident alert workflows started by the poller."),
		metric.WithUnit("{workflow}"),
	)
	if err != nil {
		return nil, fmt.Errorf("create alerting workflow counter: %w", err)
	}

	return &AlertingMetrics{workflowsStarted: started}, nil
}

// RecordStarted counts alert workflows launched in one poll cycle.
func (m *AlertingMetrics) RecordStarted(ctx context.Context, n int) {
	if m == nil || n <= 0 {
		return
	}
	m.workflowsStarted.Add(ctx, int64(n))
}

// RemediationMetrics records the remediation worker's activity. As with
// alerting, the per-action detail lives in the database; this counts the runs.
type RemediationMetrics struct {
	workflowsStarted metric.Int64Counter
}

// NewRemediationMetrics creates the remediation instruments.
func NewRemediationMetrics(mp metric.MeterProvider) (*RemediationMetrics, error) {
	meter := mp.Meter(meterName)

	started, err := meter.Int64Counter(
		"sentinelflow.remediation.workflows_started",
		metric.WithDescription("Number of incident remediation runs started by the poller."),
		metric.WithUnit("{workflow}"),
	)
	if err != nil {
		return nil, fmt.Errorf("create remediation workflow counter: %w", err)
	}

	return &RemediationMetrics{workflowsStarted: started}, nil
}

// RecordStarted counts remediation runs launched in one poll cycle.
func (m *RemediationMetrics) RecordStarted(ctx context.Context, n int) {
	if m == nil || n <= 0 {
		return
	}
	m.workflowsStarted.Add(ctx, int64(n))
}
