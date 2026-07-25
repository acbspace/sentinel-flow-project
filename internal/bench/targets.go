package bench

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/acbspace/sentinel-flow-project/internal/event"
)

// EventOptions shapes the synthetic events the ingest target publishes.
type EventOptions struct {
	// TenantID and ServiceName default to a dedicated bench identity so that a
	// benchmark run's rows are trivially separable from demo traffic — and so a
	// run never lands inside the demo services' correlation windows.
	TenantID    string
	ServiceName string

	// ServiceCount spreads the run across that many synthetic services, named
	// <ServiceName>-1..N and rotated per request.
	//
	// This matters more than it looks. The Kafka partition key is
	// tenant_id:service_name, so a run with a single service name puts every
	// event on one partition and therefore measures one consumer — the engine's
	// aggregate throughput and any change to it stay invisible no matter how
	// many partitions or replicas exist.
	//
	// Set this to several times the partition count, not to the partition count.
	// Keys are hashed, not round-robined, so N keys over N partitions leave some
	// partitions empty most of the time — measured here, three services over
	// three partitions put all of the load on one. A ratio of about 4:1 makes an
	// empty partition unlikely enough to ignore.
	//
	// The default of 1 keeps ingest-latency numbers comparable with single-key
	// runs; raise it when the number being measured is the consume side.
	ServiceCount int

	// Severity defaults to info on purpose. Benchmarking is a sustained burst of
	// events, and at error severity that burst is exactly what the correlation
	// engine is built to notice: a default of error would open an incident, page
	// the on-call rotation and start a runbook every time someone measured
	// throughput.
	Severity event.Severity

	// Now is injectable so a test can pin the event timestamp.
	Now func() time.Time

	// NewID is injectable for the same reason. It must be safe to call from
	// several goroutines at once, because every worker calls it per request.
	NewID func() string
}

func (o EventOptions) withDefaults() EventOptions {
	if o.TenantID == "" {
		o.TenantID = "bench-tenant"
	}
	if o.ServiceName == "" {
		o.ServiceName = "bench-service"
	}
	if o.ServiceCount <= 0 {
		o.ServiceCount = 1
	}
	if o.Severity == "" {
		o.Severity = event.SeverityInfo
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.NewID == nil {
		o.NewID = uuid.NewString
	}
	return o
}

// HealthTarget measures the cheapest possible path: HTTP parsing and routing,
// with no I/O behind it. It is the floor every other number is read against.
func HealthTarget(baseURL string) (Target, error) {
	endpoint, err := join(baseURL, "/health")
	if err != nil {
		return Target{}, err
	}
	return Target{
		Name:   "GET /health",
		Work:   "HTTP + routing only",
		Method: "GET",
		URL:    endpoint,
	}, nil
}

// IncidentsTarget measures the read path: the same HTTP work as /health plus one
// PostgreSQL query, so the difference between the two rows is the database.
func IncidentsTarget(baseURL string) (Target, error) {
	endpoint, err := join(baseURL, "/v1/incidents")
	if err != nil {
		return Target{}, err
	}
	return Target{
		Name:   "GET /v1/incidents",
		Work:   "+ PostgreSQL query",
		Method: "GET",
		URL:    endpoint,
	}, nil
}

// EventsTarget measures the write path: validation plus a synchronous Kafka
// produce at acks=all, which is a real broker round-trip on the hot path rather
// than a buffered write that returns before anything is durable.
//
// Every request carries a fresh event_id, so the run measures the insert path
// rather than repeatedly colliding on the engine's deduplication.
func EventsTarget(baseURL string, opts EventOptions) (Target, error) {
	endpoint, err := join(baseURL, "/v1/events")
	if err != nil {
		return Target{}, err
	}
	opts = opts.withDefaults()

	if _, ok := severitySupported(opts.Severity); !ok {
		return Target{}, fmt.Errorf("severity %q is not part of the event contract (supported: %s)",
			opts.Severity, strings.Join(event.SupportedSeverities(), ", "))
	}

	// next rotates the service name across workers. It is atomic because every
	// worker calls Body concurrently, and the rotation must not be a data race.
	var next atomic.Uint64

	body := func() ([]byte, error) {
		serviceName := opts.ServiceName
		if opts.ServiceCount > 1 {
			slot := next.Add(1) % uint64(opts.ServiceCount)
			serviceName = fmt.Sprintf("%s-%d", opts.ServiceName, slot+1)
		}

		ev := event.Event{
			EventID:       opts.NewID(),
			SchemaVersion: event.SchemaVersion10,
			TenantID:      opts.TenantID,
			ServiceName:   serviceName,
			Environment:   "bench",
			EventType:     "request.completed",
			Severity:      opts.Severity,
			Timestamp:     event.NewTimestamp(opts.Now().UTC()),
			Attributes: event.Attributes{
				"http_status_code": 200,
				"duration_ms":      12,
			},
		}

		payload, err := json.Marshal(ev)
		if err != nil {
			return nil, fmt.Errorf("encode bench event: %w", err)
		}
		return payload, nil
	}

	return Target{
		Name:        "POST /v1/events",
		Work:        "+ validation + **synchronous Kafka `acks=all`**",
		Method:      "POST",
		URL:         endpoint,
		ContentType: "application/json",
		Body:        body,
	}, nil
}

// EventsBatchTarget measures the batch path: the same validation and the same
// synchronous acks=all produce, but amortised over size events per request
// instead of one.
//
// Throughput here is reported in requests per second like every other row, so
// multiply by size to compare it against the single-event path in events.
func EventsBatchTarget(baseURL string, opts EventOptions, size int) (Target, error) {
	if size < 1 {
		return Target{}, fmt.Errorf("batch size %d must be at least 1", size)
	}

	single, err := EventsTarget(baseURL, opts)
	if err != nil {
		return Target{}, err
	}
	endpoint, err := join(baseURL, "/v1/events:batch")
	if err != nil {
		return Target{}, err
	}

	body := func() ([]byte, error) {
		items := make([]json.RawMessage, 0, size)
		for range size {
			one, err := single.Body()
			if err != nil {
				return nil, err
			}
			items = append(items, one)
		}

		payload, err := json.Marshal(struct {
			Events []json.RawMessage `json:"events"`
		}{Events: items})
		if err != nil {
			return nil, fmt.Errorf("encode bench batch: %w", err)
		}
		return payload, nil
	}

	return Target{
		Name:        "POST /v1/events:batch",
		Work:        fmt.Sprintf("+ %d events per request, one Kafka `acks=all`", size),
		Method:      "POST",
		URL:         endpoint,
		ContentType: "application/json",
		Body:        body,
	}, nil
}

// severitySupported checks a severity against the event contract's vocabulary,
// so a typo in a flag fails before the run rather than producing a run in which
// every request is a 400.
func severitySupported(s event.Severity) (event.Severity, bool) {
	for _, supported := range event.SupportedSeverities() {
		if string(s) == supported {
			return s, true
		}
	}
	return s, false
}

// join appends a path to a base URL, rejecting a base that is not a usable
// absolute HTTP URL. Catching it here means a mistyped flag reports the flag
// rather than surfacing as thousands of identical request failures.
func join(baseURL, path string) (string, error) {
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return "", fmt.Errorf("parse base URL %q: %w", baseURL, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("base URL %q must be http or https", baseURL)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("base URL %q has no host", baseURL)
	}
	return parsed.String() + path, nil
}
