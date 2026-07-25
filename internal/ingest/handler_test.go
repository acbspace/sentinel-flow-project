package ingest_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/ingest"
)

// fakePublisher records what the handler tried to publish and can be told to
// fail, which is what lets these tests cover the Kafka failure path without a
// broker.
type fakePublisher struct {
	mu        sync.Mutex
	published []event.Event
	batches   int
	err       error
}

func (f *fakePublisher) Publish(_ context.Context, ev event.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, ev)
	return nil
}

func (f *fakePublisher) PublishBatch(_ context.Context, evs []event.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, evs...)
	f.batches++
	return nil
}

// batchCount reports how many produce calls the batch took, which is the whole
// point of the endpoint.
func (f *fakePublisher) batchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.batches
}

func (f *fakePublisher) events() []event.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]event.Event(nil), f.published...)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

const validUUID = "6f1c4d5e-8b3a-4c2d-9e7f-1a2b3c4d5e6f"

// validBody is a well-formed request body; tests derive variants from it.
func validBody() map[string]any {
	return map[string]any{
		"event_id":       validUUID,
		"schema_version": "1.0",
		"tenant_id":      "demo-tenant",
		"service_name":   "payment-service",
		"environment":    "local",
		"event_type":     "request.completed",
		"severity":       "info",
		"timestamp":      "2026-07-22T10:30:00Z",
		"trace_id":       "0af7651916cd43dd8448eb211c80319c",
		"attributes": map[string]any{
			"http_method":      "POST",
			"http_route":       "/demo/payments",
			"http_status_code": 200,
			"latency_ms":       125,
		},
	}
}

// jsonString encodes v, panicking on failure. It deliberately takes no
// *testing.T so that it can also be called while building table-driven cases.
func jsonString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic("marshal test body: " + err.Error())
	}
	return string(b)
}

// newRequest builds a POST /v1/events request with a JSON content type.
func newRequest(body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestPostEventAccepts(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	handler := ingest.NewHandler(ingest.Options{Publisher: publisher, Logger: discardLogger()})

	rec := httptest.NewRecorder()
	handler.PostEvent(rec, newRequest(jsonString(validBody())))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	var resp struct {
		Status  string `json:"status"`
		EventID string `json:"event_id"`
		TraceID string `json:"trace_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "accepted" {
		t.Errorf("status = %q, want %q", resp.Status, "accepted")
	}
	if resp.EventID != validUUID {
		t.Errorf("event_id = %q, want %q", resp.EventID, validUUID)
	}

	published := publisher.events()
	if len(published) != 1 {
		t.Fatalf("published %d events, want 1", len(published))
	}

	got := published[0]
	if got.EventID != validUUID {
		t.Errorf("published EventID = %q, want %q", got.EventID, validUUID)
	}
	if got.TenantID != "demo-tenant" {
		t.Errorf("published TenantID = %q, want %q", got.TenantID, "demo-tenant")
	}
	// The Kafka key must be derivable from what was published.
	if want := "demo-tenant:payment-service"; got.PartitionKey() != want {
		t.Errorf("PartitionKey() = %q, want %q", got.PartitionKey(), want)
	}
	if got.Attributes["http_route"] != "/demo/payments" {
		t.Errorf("attributes were not preserved: %v", got.Attributes)
	}
}

func TestPostEventRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// body is the raw request body.
		body string
		// contentType overrides the default application/json when set.
		contentType string
		wantStatus  int
		wantError   string
		// wantFields lists validation fields expected in the details array.
		wantFields []string
	}{
		{
			name:       "empty body",
			body:       "",
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_json",
		},
		{
			name:       "malformed JSON",
			body:       `{"event_id": `,
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_json",
		},
		{
			name:       "not a JSON object",
			body:       `["an", "array"]`,
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_json",
		},
		{
			name:       "wrong field type",
			body:       `{"event_id": 42}`,
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_json",
		},
		{
			name:       "unknown field",
			body:       `{"event_id":"` + validUUID + `","surprise":"value"}`,
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_json",
		},
		{
			name:       "trailing content after the object",
			body:       jsonString(validBody()) + `{"another":"object"}`,
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_json",
		},
		{
			name:       "timestamp is not RFC3339",
			body:       `{"event_id":"` + validUUID + `","timestamp":"yesterday"}`,
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_json",
		},
		{
			name:        "wrong content type",
			body:        jsonString(validBody()),
			contentType: "text/plain",
			wantStatus:  http.StatusUnsupportedMediaType,
			wantError:   "unsupported_media_type",
		},
		{
			name:       "missing required fields",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
			wantError:  "validation_failed",
			wantFields: []string{"event_id", "schema_version", "tenant_id", "service_name", "event_type", "severity", "timestamp"},
		},
		{
			name: "unsupported schema version",
			body: func() string {
				b := validBody()
				b["schema_version"] = "9.9"
				return jsonString(b)
			}(),
			wantStatus: http.StatusBadRequest,
			wantError:  "validation_failed",
			wantFields: []string{"schema_version"},
		},
		{
			name: "unsupported severity",
			body: func() string {
				b := validBody()
				b["severity"] = "catastrophic"
				return jsonString(b)
			}(),
			wantStatus: http.StatusBadRequest,
			wantError:  "validation_failed",
			wantFields: []string{"severity"},
		},
		{
			name: "event id is not a uuid",
			body: func() string {
				b := validBody()
				b["event_id"] = "12345"
				return jsonString(b)
			}(),
			wantStatus: http.StatusBadRequest,
			wantError:  "validation_failed",
			wantFields: []string{"event_id"},
		},
		{
			name: "blank tenant id",
			body: func() string {
				b := validBody()
				b["tenant_id"] = "   "
				return jsonString(b)
			}(),
			wantStatus: http.StatusBadRequest,
			wantError:  "validation_failed",
			wantFields: []string{"tenant_id"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			publisher := &fakePublisher{}
			handler := ingest.NewHandler(ingest.Options{Publisher: publisher, Logger: discardLogger()})

			req := newRequest(tc.body)
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}

			rec := httptest.NewRecorder()
			handler.PostEvent(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}

			var resp struct {
				Error   string             `json:"error"`
				Message string             `json:"message"`
				Details []event.FieldError `json:"details"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode error response: %v; body: %s", err, rec.Body.String())
			}

			if resp.Error != tc.wantError {
				t.Errorf("error = %q, want %q", resp.Error, tc.wantError)
			}
			if resp.Message == "" {
				t.Error("error response has an empty message")
			}

			if len(tc.wantFields) > 0 {
				got := make(map[string]bool, len(resp.Details))
				for _, d := range resp.Details {
					got[d.Field] = true
				}
				for _, field := range tc.wantFields {
					if !got[field] {
						t.Errorf("missing detail for field %q; got %+v", field, resp.Details)
					}
				}
			}

			// A rejected event must never reach Kafka.
			if n := len(publisher.events()); n != 0 {
				t.Errorf("published %d events, want 0", n)
			}
		})
	}
}

func TestPostEventRejectsOversizedBody(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	handler := ingest.NewHandler(ingest.Options{
		Publisher:    publisher,
		Logger:       discardLogger(),
		MaxBodyBytes: 256,
	})

	body := validBody()
	body["attributes"] = map[string]any{"padding": strings.Repeat("x", 4096)}

	rec := httptest.NewRecorder()
	handler.PostEvent(rec, newRequest(jsonString(body)))

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
	}

	var resp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if resp.Error != "payload_too_large" {
		t.Errorf("error = %q, want %q", resp.Error, "payload_too_large")
	}
	if n := len(publisher.events()); n != 0 {
		t.Errorf("published %d events, want 0", n)
	}
}

func TestPostEventReportsPublishFailure(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{err: errors.New("broker unreachable")}
	handler := ingest.NewHandler(ingest.Options{Publisher: publisher, Logger: discardLogger()})

	rec := httptest.NewRecorder()
	handler.PostEvent(rec, newRequest(jsonString(validBody())))

	// The event was never durably written, so the caller must be told to retry
	// rather than being handed a 202 for an event that no longer exists.
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	if got := rec.Header().Get("Retry-After"); got == "" {
		t.Error("Retry-After header is missing on a retryable failure")
	}

	var resp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if resp.Error != "publish_failed" {
		t.Errorf("error = %q, want %q", resp.Error, "publish_failed")
	}
}

func TestPostEventNormalizesBeforePublishing(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	handler := ingest.NewHandler(ingest.Options{Publisher: publisher, Logger: discardLogger()})

	body := validBody()
	body["tenant_id"] = "  demo-tenant  "
	body["severity"] = "INFO"
	body["environment"] = ""

	rec := httptest.NewRecorder()
	handler.PostEvent(rec, newRequest(jsonString(body)))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	published := publisher.events()
	if len(published) != 1 {
		t.Fatalf("published %d events, want 1", len(published))
	}

	got := published[0]
	if got.TenantID != "demo-tenant" {
		t.Errorf("TenantID = %q, want %q", got.TenantID, "demo-tenant")
	}
	if got.Severity != event.SeverityInfo {
		t.Errorf("Severity = %q, want %q", got.Severity, event.SeverityInfo)
	}
	if got.Environment != event.DefaultEnvironment {
		t.Errorf("Environment = %q, want %q", got.Environment, event.DefaultEnvironment)
	}
}

func TestPostEventAcceptsJSONContentTypeVariants(t *testing.T) {
	t.Parallel()

	tests := []string{
		"application/json",
		"application/json; charset=utf-8",
		"APPLICATION/JSON",
		"application/vnd.sentinelflow+json",
		"", // absent Content-Type is tolerated
	}

	for _, contentType := range tests {
		t.Run(fmt.Sprintf("content type %q", contentType), func(t *testing.T) {
			t.Parallel()

			publisher := &fakePublisher{}
			handler := ingest.NewHandler(ingest.Options{Publisher: publisher, Logger: discardLogger()})

			req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(jsonString(validBody())))
			if contentType != "" {
				req.Header.Set("Content-Type", contentType)
			}

			rec := httptest.NewRecorder()
			handler.PostEvent(rec, req)

			if rec.Code != http.StatusAccepted {
				t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusAccepted, rec.Body.String())
			}
		})
	}
}

// boundsNow is the fixed "now" the time-bound tests measure against, so every
// bound in them is exact rather than relative to the wall clock.
var boundsNow = time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)

func boundedHandler(publisher ingest.Publisher, bounds event.TimeBounds) *ingest.Handler {
	return ingest.NewHandler(ingest.Options{
		Publisher: publisher,
		Logger:    discardLogger(),
		Bounds:    bounds,
		Now:       func() time.Time { return boundsNow },
	})
}

// TestPostEventRejectsFutureDatedEvent pins the bug this bound exists for.
//
// The correlation window is "event_timestamp >= now() - window", so a row dated
// in the future matches every window forever: it holds an incident open that can
// never go quiet, and auto-resolution never fires. One such event is enough.
func TestPostEventRejectsFutureDatedEvent(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	handler := boundedHandler(publisher, event.TimeBounds{MaxFuture: 5 * time.Minute})

	body := validBody()
	body["timestamp"] = boundsNow.AddDate(4, 0, 0).Format(time.RFC3339)

	rec := httptest.NewRecorder()
	handler.PostEvent(rec, newRequest(jsonString(body)))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	var resp struct {
		Error   string `json:"error"`
		Details []struct {
			Field string `json:"field"`
		} `json:"details"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if resp.Error != "validation_failed" {
		t.Errorf("error = %q, want %q", resp.Error, "validation_failed")
	}
	if len(resp.Details) == 0 || resp.Details[0].Field != "timestamp" {
		t.Errorf("details = %+v, want a timestamp field error", resp.Details)
	}

	// The point of rejecting at the front door is that it never reaches Kafka,
	// and therefore never reaches a correlation window.
	if n := len(publisher.events()); n != 0 {
		t.Errorf("published %d events, want 0", n)
	}
}

func TestPostEventRejectsExcessivelyBackdatedEvent(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	handler := boundedHandler(publisher, event.TimeBounds{MaxAge: 7 * 24 * time.Hour})

	body := validBody()
	body["timestamp"] = boundsNow.AddDate(-2, 0, 0).Format(time.RFC3339)

	rec := httptest.NewRecorder()
	handler.PostEvent(rec, newRequest(jsonString(body)))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if n := len(publisher.events()); n != 0 {
		t.Errorf("published %d events, want 0", n)
	}
}

func TestPostEventAcceptsAnEventInsideBothBounds(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	handler := boundedHandler(publisher, event.TimeBounds{
		MaxFuture: 5 * time.Minute,
		MaxAge:    7 * 24 * time.Hour,
	})

	body := validBody()
	body["timestamp"] = boundsNow.Add(-time.Minute).Format(time.RFC3339)

	rec := httptest.NewRecorder()
	handler.PostEvent(rec, newRequest(jsonString(body)))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if n := len(publisher.events()); n != 1 {
		t.Errorf("published %d events, want 1", n)
	}
}
