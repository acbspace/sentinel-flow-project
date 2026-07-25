package bench

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/event"
)

func TestPercentileNearestRank(t *testing.T) {
	// Ten samples, 1ms..10ms. Nearest rank means every answer is a sample that
	// actually occurred, so these are exact rather than interpolated.
	r := Result{}
	for i := 1; i <= 10; i++ {
		r.latencies = append(r.latencies, time.Duration(i)*time.Millisecond)
	}

	tests := []struct {
		q    float64
		want time.Duration
	}{
		{0.50, 5 * time.Millisecond},
		{0.90, 9 * time.Millisecond},
		{0.95, 10 * time.Millisecond},
		{0.99, 10 * time.Millisecond},
		{1.0, 10 * time.Millisecond},
		{0.0, 1 * time.Millisecond},
		{0.10, 1 * time.Millisecond},
	}

	for _, tt := range tests {
		if got := r.Percentile(tt.q); got != tt.want {
			t.Errorf("Percentile(%v) = %v, want %v", tt.q, got, tt.want)
		}
	}
}

func TestPercentileEmpty(t *testing.T) {
	var r Result
	if got := r.Percentile(0.99); got != 0 {
		t.Errorf("Percentile on an empty result = %v, want 0", got)
	}
	if got := r.Average(); got != 0 {
		t.Errorf("Average on an empty result = %v, want 0", got)
	}
	if got := r.RequestsPerSecond(); got != 0 {
		t.Errorf("RequestsPerSecond on an empty result = %v, want 0", got)
	}
}

func TestRunSendsExactRequestCount(t *testing.T) {
	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	result, err := Run(context.Background(), Target{
		Name:   "GET /test",
		Method: "GET",
		URL:    srv.URL,
	}, Options{Workers: 4, Requests: 200})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := result.Requests(); got != 200 {
		t.Errorf("Requests() = %d, want 200", got)
	}
	if got := served.Load(); got != 200 {
		t.Errorf("server saw %d requests, want 200", got)
	}
	if !result.Clean() {
		t.Errorf("Clean() = false, want true (statuses %v, failures %d)", result.Statuses, result.Failures)
	}
	if got := result.Statuses[http.StatusOK]; got != 200 {
		t.Errorf("Statuses[200] = %d, want 200", got)
	}
}

func TestRunDiscardsWarmupRequests(t *testing.T) {
	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	result, err := Run(context.Background(), Target{
		Name:   "GET /test",
		Method: "GET",
		URL:    srv.URL,
	}, Options{Workers: 2, Requests: 50, Warmup: 30})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The server sees warmup + measured; the result reports only the measured.
	if got := served.Load(); got != 80 {
		t.Errorf("server saw %d requests, want 80 (30 warmup + 50 measured)", got)
	}
	if got := result.Requests(); got != 50 {
		t.Errorf("Requests() = %d, want 50 — warmup must not be measured", got)
	}
}

func TestRunRecordsNonSuccessStatuses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	result, err := Run(context.Background(), Target{
		Name:   "GET /test",
		Method: "GET",
		URL:    srv.URL,
	}, Options{Workers: 2, Requests: 20})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// A run that is uniformly rejected still produces excellent latencies. The
	// point of Clean is that those latencies never get quoted as a result.
	if result.Clean() {
		t.Error("Clean() = true for a run that was entirely 503s, want false")
	}
	if got := result.Statuses[http.StatusServiceUnavailable]; got != 20 {
		t.Errorf("Statuses[503] = %d, want 20", got)
	}
	if got := result.Successes(); got != 0 {
		t.Errorf("Successes() = %d, want 0", got)
	}
}

func TestRunCountsTransportFailures(t *testing.T) {
	// A closed server refuses connections, which is a failure with no response.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	url := srv.URL
	srv.Close()

	result, err := Run(context.Background(), Target{
		Name:   "GET /gone",
		Method: "GET",
		URL:    url,
	}, Options{Workers: 2, Requests: 10})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if result.Failures != 10 {
		t.Errorf("Failures = %d, want 10", result.Failures)
	}
	if result.Requests() != 0 {
		t.Errorf("Requests() = %d, want 0 — a refused connection has no latency to record", result.Requests())
	}
	if result.Clean() {
		t.Error("Clean() = true for a run that never reached the server, want false")
	}
}

func TestRunStopsOnContextCancellation(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(release) })
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		<-release
		cancel()
	}()

	// Without cancellation this would block forever on the hanging handler.
	if _, err := Run(ctx, Target{
		Name:   "GET /hang",
		Method: "GET",
		URL:    srv.URL,
	}, Options{Workers: 2, Requests: 1000}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	<-done
}

func TestRunRejectsUnusableConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		target Target
		opts   Options
	}{
		{"no name", Target{Method: "GET", URL: "http://x"}, Options{Requests: 1}},
		{"no method", Target{Name: "n", URL: "http://x"}, Options{Requests: 1}},
		{"no url", Target{Name: "n", Method: "GET"}, Options{Requests: 1}},
		{"no budget", Target{Name: "n", Method: "GET", URL: "http://x"}, Options{}},
		{"negative warmup", Target{Name: "n", Method: "GET", URL: "http://x"}, Options{Requests: 1, Warmup: -1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Run(context.Background(), tt.target, tt.opts); err == nil {
				t.Error("Run() error = nil, want an error")
			}
		})
	}
}

func TestEventsTargetProducesContractValidEvents(t *testing.T) {
	target, err := EventsTarget("http://localhost:8080", EventOptions{})
	if err != nil {
		t.Fatalf("EventsTarget: %v", err)
	}

	if target.URL != "http://localhost:8080/v1/events" {
		t.Errorf("URL = %q, want http://localhost:8080/v1/events", target.URL)
	}

	// The bench is only measuring the accept path if the events it sends are
	// actually acceptable; validate against the same contract the API applies.
	seen := make(map[string]bool)
	for range 100 {
		payload, err := target.Body()
		if err != nil {
			t.Fatalf("Body: %v", err)
		}

		var ev event.Event
		if err := json.Unmarshal(payload, &ev); err != nil {
			t.Fatalf("decode generated event: %v", err)
		}
		ev.Normalize()
		if err := ev.Validate(); err != nil {
			t.Fatalf("generated event violates the contract: %v", err)
		}

		if seen[ev.EventID] {
			t.Fatalf("event_id %s generated twice; the run would measure deduplication", ev.EventID)
		}
		seen[ev.EventID] = true
	}
}

func TestEventsTargetDefaultsToInfoSeverity(t *testing.T) {
	target, err := EventsTarget("http://localhost:8080", EventOptions{})
	if err != nil {
		t.Fatalf("EventsTarget: %v", err)
	}

	payload, err := target.Body()
	if err != nil {
		t.Fatalf("Body: %v", err)
	}

	var ev event.Event
	if err := json.Unmarshal(payload, &ev); err != nil {
		t.Fatalf("decode generated event: %v", err)
	}

	// Benchmarking at error severity would open an incident, page the rotation
	// and start a runbook on every run.
	if ev.Severity != event.SeverityInfo {
		t.Errorf("Severity = %q, want %q", ev.Severity, event.SeverityInfo)
	}
}

func TestEventsTargetSpreadsAcrossServices(t *testing.T) {
	target, err := EventsTarget("http://localhost:8080", EventOptions{ServiceCount: 3})
	if err != nil {
		t.Fatalf("EventsTarget: %v", err)
	}

	// The Kafka partition key is tenant:service, so the spread of service names
	// is the spread across partitions. Without it, a run measures one consumer.
	counts := make(map[string]int)
	for range 300 {
		payload, err := target.Body()
		if err != nil {
			t.Fatalf("Body: %v", err)
		}
		var ev event.Event
		if err := json.Unmarshal(payload, &ev); err != nil {
			t.Fatalf("decode generated event: %v", err)
		}
		counts[ev.ServiceName]++
	}

	if len(counts) != 3 {
		t.Fatalf("generated %d distinct service names, want 3: %v", len(counts), counts)
	}
	for _, name := range []string{"bench-service-1", "bench-service-2", "bench-service-3"} {
		if counts[name] != 100 {
			t.Errorf("service %s got %d events, want an even 100: %v", name, counts[name], counts)
		}
	}
}

func TestEventsTargetSingleServiceKeepsBareName(t *testing.T) {
	target, err := EventsTarget("http://localhost:8080", EventOptions{})
	if err != nil {
		t.Fatalf("EventsTarget: %v", err)
	}

	payload, err := target.Body()
	if err != nil {
		t.Fatalf("Body: %v", err)
	}
	var ev event.Event
	if err := json.Unmarshal(payload, &ev); err != nil {
		t.Fatalf("decode generated event: %v", err)
	}

	// The default must not become "bench-service-1": earlier runs used the bare
	// name, and a silent rename would break comparison against them.
	if ev.ServiceName != "bench-service" {
		t.Errorf("ServiceName = %q, want %q", ev.ServiceName, "bench-service")
	}
}

func TestEventsTargetBodyIsConcurrencySafe(t *testing.T) {
	target, err := EventsTarget("http://localhost:8080", EventOptions{ServiceCount: 4})
	if err != nil {
		t.Fatalf("EventsTarget: %v", err)
	}

	// Body is called from every worker at once; under -race this pins that the
	// service rotation is not a data race.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				if _, err := target.Body(); err != nil {
					t.Errorf("Body: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestEventsTargetRejectsUnknownSeverity(t *testing.T) {
	if _, err := EventsTarget("http://localhost:8080", EventOptions{Severity: "catastrophe"}); err == nil {
		t.Error("EventsTarget() error = nil for an unknown severity, want an error")
	}
}

func TestTargetsRejectBadBaseURL(t *testing.T) {
	for _, base := range []string{"", "localhost:8080", "ftp://localhost", "http://"} {
		if _, err := HealthTarget(base); err == nil {
			t.Errorf("HealthTarget(%q) error = nil, want an error", base)
		}
	}
}

func TestReportRendersMarkdownTable(t *testing.T) {
	r := Result{
		Name:     "GET /health",
		Work:     "HTTP + routing only",
		Elapsed:  time.Second,
		Statuses: map[int]int{200: 1000},
	}
	for range 1000 {
		r.latencies = append(r.latencies, 2*time.Millisecond)
	}

	got := Report([]Result{r})

	for _, want := range []string{"| Path |", "`GET /health`", "HTTP + routing only", "1,000", "2.0 ms"} {
		if !strings.Contains(got, want) {
			t.Errorf("Report() missing %q:\n%s", want, got)
		}
	}
}

func TestSummaryFlagsAnUncleanRun(t *testing.T) {
	clean := Result{Name: "a", Elapsed: time.Second, Statuses: map[int]int{200: 2}}
	clean.latencies = []time.Duration{time.Millisecond, time.Millisecond}

	unclean := Result{Name: "b", Elapsed: time.Second, Statuses: map[int]int{503: 2}, Failures: 3}
	unclean.latencies = []time.Duration{time.Millisecond, time.Millisecond}

	got := Summary([]Result{clean, unclean})

	if !strings.Contains(got, "all 2xx") {
		t.Errorf("Summary() should report a clean run as such:\n%s", got)
	}
	if !strings.Contains(got, "NOT CLEAN") {
		t.Errorf("Summary() should flag an unclean run:\n%s", got)
	}
	if !strings.Contains(got, "3 transport failures") {
		t.Errorf("Summary() should report transport failures:\n%s", got)
	}
	if !strings.Contains(got, "2×503") {
		t.Errorf("Summary() should report the status distribution:\n%s", got)
	}
}

func TestFormatCountGroupsThousands(t *testing.T) {
	tests := []struct {
		in   float64
		want string
	}{
		{0, "0"},
		{42, "42"},
		{999, "999"},
		{1000, "1,000"},
		{8186, "8,186"},
		{20155, "20,155"},
		{1234567, "1,234,567"},
	}

	for _, tt := range tests {
		if got := formatCount(tt.in); got != tt.want {
			t.Errorf("formatCount(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestFormatDurationFallsBackToMicroseconds(t *testing.T) {
	if got := formatDuration(2400 * time.Microsecond); got != "2.4 ms" {
		t.Errorf("formatDuration(2.4ms) = %q, want \"2.4 ms\"", got)
	}
	if got := formatDuration(40 * time.Microsecond); got != "40 µs" {
		t.Errorf("formatDuration(40µs) = %q, want \"40 µs\"", got)
	}
	if got := formatDuration(0); got != "0.0 ms" {
		t.Errorf("formatDuration(0) = %q, want \"0.0 ms\"", got)
	}
}
