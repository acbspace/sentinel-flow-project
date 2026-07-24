package demo_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/demo"
	"github.com/acbspace/sentinel-flow-project/internal/event"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// recordingSink captures emitted telemetry instead of sending it over HTTP.
type recordingSink struct {
	mu     sync.Mutex
	events []event.Event
	err    error
}

func (s *recordingSink) Emit(_ context.Context, ev event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = append(s.events, ev)
	return s.err
}

func (s *recordingSink) emitted() []event.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]event.Event(nil), s.events...)
}

// newHandler builds a handler with a deterministic simulator: a failure rate of
// exactly 0 or 1 removes randomness entirely, and zero latency keeps tests fast.
func newHandler(t *testing.T, failureRate float64, sink *recordingSink, downstream demo.Downstream) *demo.Handler {
	t.Helper()

	return demo.NewHandler(demo.HandlerConfig{
		ServiceName:             "payment-service",
		Route:                   "/demo/payments",
		TenantID:                "demo-tenant",
		Environment:             "test",
		IDPrefix:                "pay",
		SuccessStatus:           http.StatusOK,
		FailureStatus:           http.StatusPaymentRequired,
		DownstreamFailureStatus: http.StatusBadGateway,
		Simulator: demo.NewSimulator(demo.SimulatorConfig{
			FailureRate: failureRate,
			Source:      rand.NewSource(1),
		}),
		Sink:       sink,
		Downstream: downstream,
		Logger:     discardLogger(),
		NewID:      func() string { return "fixed-id" },
	})
}

func post(t *testing.T, handler *demo.Handler) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/demo/payments", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestHandlerEmitsTelemetryOnSuccess(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	rec := post(t, newHandler(t, 0, sink, nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp struct {
		ID        string `json:"id"`
		Status    string `json:"status"`
		LatencyMS int64  `json:"latency_ms"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ID != "pay_fixed-id" {
		t.Errorf("id = %q, want %q", resp.ID, "pay_fixed-id")
	}
	if resp.Status != "completed" {
		t.Errorf("status = %q, want %q", resp.Status, "completed")
	}

	events := sink.emitted()
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want 1", len(events))
	}

	ev := events[0]
	// The event the demo service produces must satisfy the same contract the
	// ingestion API enforces, otherwise the demo cannot work end to end.
	if err := ev.Validate(); err != nil {
		t.Fatalf("emitted event failed validation: %v", err)
	}
	if ev.ServiceName != "payment-service" {
		t.Errorf("ServiceName = %q, want %q", ev.ServiceName, "payment-service")
	}
	if ev.TenantID != "demo-tenant" {
		t.Errorf("TenantID = %q, want %q", ev.TenantID, "demo-tenant")
	}
	if ev.EventType != "request.completed" {
		t.Errorf("EventType = %q, want %q", ev.EventType, "request.completed")
	}
	if ev.Severity != event.SeverityInfo {
		t.Errorf("Severity = %q, want %q", ev.Severity, event.SeverityInfo)
	}
	if ev.SchemaVersion != event.SchemaVersion10 {
		t.Errorf("SchemaVersion = %q, want %q", ev.SchemaVersion, event.SchemaVersion10)
	}

	// Every attribute the milestone requires must be present.
	for _, key := range []string{"http_method", "http_route", "http_status_code", "latency_ms"} {
		if _, ok := ev.Attributes[key]; !ok {
			t.Errorf("attribute %q is missing; got %v", key, ev.Attributes)
		}
	}
	if ev.Attributes["http_method"] != http.MethodPost {
		t.Errorf("http_method = %v, want POST", ev.Attributes["http_method"])
	}
	if ev.Attributes["http_route"] != "/demo/payments" {
		t.Errorf("http_route = %v, want /demo/payments", ev.Attributes["http_route"])
	}
	if ev.Attributes["http_status_code"] != http.StatusOK {
		t.Errorf("http_status_code = %v, want 200", ev.Attributes["http_status_code"])
	}
}

func TestHandlerEmitsTelemetryOnFailure(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	// A failure rate of 1 always fails, so this test never flakes.
	rec := post(t, newHandler(t, 1, sink, nil))

	if rec.Code != http.StatusPaymentRequired {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusPaymentRequired)
	}

	events := sink.emitted()
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want 1", len(events))
	}

	ev := events[0]
	if err := ev.Validate(); err != nil {
		t.Fatalf("emitted event failed validation: %v", err)
	}
	// 402 is a client-visible refusal, which maps to warn rather than error.
	if ev.Severity != event.SeverityWarn {
		t.Errorf("Severity = %q, want %q", ev.Severity, event.SeverityWarn)
	}
	if ev.Attributes["outcome"] != "failed" {
		t.Errorf("outcome = %v, want failed", ev.Attributes["outcome"])
	}
	if ev.Attributes["http_status_code"] != http.StatusPaymentRequired {
		t.Errorf("http_status_code = %v, want 402", ev.Attributes["http_status_code"])
	}
}

func TestHandlerSeverityMapsFromStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		failureStatus int
		wantSeverity  event.Severity
	}{
		{"server failures are errors", http.StatusInternalServerError, event.SeverityError},
		{"client refusals are warnings", http.StatusPaymentRequired, event.SeverityWarn},
		{"bad gateway is an error", http.StatusBadGateway, event.SeverityError},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sink := &recordingSink{}
			handler := demo.NewHandler(demo.HandlerConfig{
				ServiceName:   "order-service",
				Route:         "/demo/orders",
				TenantID:      "demo-tenant",
				Environment:   "test",
				IDPrefix:      "ord",
				SuccessStatus: http.StatusCreated,
				FailureStatus: tc.failureStatus,
				Simulator:     demo.NewSimulator(demo.SimulatorConfig{FailureRate: 1, Source: rand.NewSource(1)}),
				Sink:          sink,
				Logger:        discardLogger(),
			})

			req := httptest.NewRequest(http.MethodPost, "/demo/orders", strings.NewReader("{}"))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tc.failureStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.failureStatus)
			}

			events := sink.emitted()
			if len(events) != 1 {
				t.Fatalf("emitted %d events, want 1", len(events))
			}
			if events[0].Severity != tc.wantSeverity {
				t.Errorf("Severity = %q, want %q", events[0].Severity, tc.wantSeverity)
			}
		})
	}
}

func TestHandlerCallsDownstream(t *testing.T) {
	t.Parallel()

	t.Run("a successful downstream leaves the request successful", func(t *testing.T) {
		t.Parallel()

		called := false
		sink := &recordingSink{}
		handler := newHandler(t, 0, sink, func(context.Context) error {
			called = true
			return nil
		})

		rec := post(t, handler)

		if !called {
			t.Error("downstream was not called")
		}
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("a rejecting downstream fails the request with its own status", func(t *testing.T) {
		t.Parallel()

		sink := &recordingSink{}
		handler := newHandler(t, 0, sink, func(context.Context) error {
			return demo.ErrDownstreamRejected
		})

		rec := post(t, handler)

		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadGateway)
		}

		events := sink.emitted()
		if len(events) != 1 {
			t.Fatalf("emitted %d events, want 1", len(events))
		}
		if events[0].Attributes["outcome"] != "failed" {
			t.Errorf("outcome = %v, want failed", events[0].Attributes["outcome"])
		}
	})

	t.Run("an unreachable downstream reports a gateway error", func(t *testing.T) {
		t.Parallel()

		sink := &recordingSink{}
		handler := newHandler(t, 0, sink, func(context.Context) error {
			return errors.New("connection refused")
		})

		rec := post(t, handler)

		if rec.Code != http.StatusBadGateway {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusBadGateway)
		}
	})
}

func TestHandlerSurvivesTelemetryFailure(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{err: errors.New("ingestion API unreachable")}
	rec := post(t, newHandler(t, 0, sink, nil))

	// Losing an observability event must not turn a successful business
	// operation into a failed HTTP response.
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d despite the telemetry failure", rec.Code, http.StatusOK)
	}
}

func TestHandlerGeneratesUniqueEventIDs(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	handler := newHandler(t, 0, sink, nil)

	for range 5 {
		post(t, handler)
	}

	events := sink.emitted()
	if len(events) != 5 {
		t.Fatalf("emitted %d events, want 5", len(events))
	}

	// Event IDs are the pipeline's idempotency key; two distinct requests
	// sharing one would make the second silently vanish at the database.
	seen := make(map[string]bool, len(events))
	for _, ev := range events {
		if seen[ev.EventID] {
			t.Fatalf("duplicate event ID %q across distinct requests", ev.EventID)
		}
		seen[ev.EventID] = true
	}
}

func TestSimulator(t *testing.T) {
	t.Parallel()

	t.Run("a zero failure rate never fails", func(t *testing.T) {
		t.Parallel()

		sim := demo.NewSimulator(demo.SimulatorConfig{FailureRate: 0, Source: rand.NewSource(42)})
		for i := range 100 {
			if sim.ShouldFail() {
				t.Fatalf("ShouldFail() = true on call %d with a zero failure rate", i)
			}
		}
	})

	t.Run("a failure rate of one always fails", func(t *testing.T) {
		t.Parallel()

		sim := demo.NewSimulator(demo.SimulatorConfig{FailureRate: 1, Source: rand.NewSource(42)})
		for i := range 100 {
			if !sim.ShouldFail() {
				t.Fatalf("ShouldFail() = false on call %d with a failure rate of one", i)
			}
		}
	})

	t.Run("a fixed seed reproduces the same sequence", func(t *testing.T) {
		t.Parallel()

		first := demo.NewSimulator(demo.SimulatorConfig{FailureRate: 0.5, Source: rand.NewSource(7)})
		second := demo.NewSimulator(demo.SimulatorConfig{FailureRate: 0.5, Source: rand.NewSource(7)})

		for i := range 50 {
			if a, b := first.ShouldFail(), second.ShouldFail(); a != b {
				t.Fatalf("call %d diverged: %v vs %v", i, a, b)
			}
		}
	})

	t.Run("latency stays within the configured range", func(t *testing.T) {
		t.Parallel()

		min, max := 20*time.Millisecond, 50*time.Millisecond
		sim := demo.NewSimulator(demo.SimulatorConfig{
			MinLatency: min,
			MaxLatency: max,
			Source:     rand.NewSource(3),
		})

		for range 200 {
			got := sim.Latency()
			if got < min || got >= max {
				t.Fatalf("Latency() = %v, want within [%v, %v)", got, min, max)
			}
		}
	})

	t.Run("an inverted range degrades to a fixed latency", func(t *testing.T) {
		t.Parallel()

		sim := demo.NewSimulator(demo.SimulatorConfig{
			MinLatency: 50 * time.Millisecond,
			MaxLatency: 10 * time.Millisecond,
			Source:     rand.NewSource(3),
		})

		if got := sim.Latency(); got != 50*time.Millisecond {
			t.Errorf("Latency() = %v, want 50ms", got)
		}
	})
}
