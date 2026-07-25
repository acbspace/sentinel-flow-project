package ingest_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/ingest"
)

func batchRequest(body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/events:batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

// batchBody wraps n valid events, each with a distinct id, in the batch envelope.
func batchBody(n int) string {
	events := make([]map[string]any, 0, n)
	for i := range n {
		ev := validBody()
		ev["event_id"] = fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		events = append(events, ev)
	}
	return jsonString(map[string]any{"events": events})
}

type batchResult struct {
	Status   string `json:"status"`
	Accepted int    `json:"accepted"`
	Rejected int    `json:"rejected"`
	Errors   []struct {
		Index   int    `json:"index"`
		Error   string `json:"error"`
		Details []struct {
			Field string `json:"field"`
		} `json:"details"`
	} `json:"errors"`
}

func decodeBatch(t *testing.T, rec *httptest.ResponseRecorder) batchResult {
	t.Helper()

	var resp batchResult
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode batch response: %v (body: %s)", err, rec.Body.String())
	}
	return resp
}

func TestPostEventBatchAcceptsAllValidEvents(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	handler := ingest.NewHandler(ingest.Options{Publisher: publisher, Logger: discardLogger()})

	rec := httptest.NewRecorder()
	handler.PostEventBatch(rec, batchRequest(batchBody(50)))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body: %s", rec.Code, rec.Body.String())
	}

	resp := decodeBatch(t, rec)
	if resp.Accepted != 50 || resp.Rejected != 0 {
		t.Errorf("accepted/rejected = %d/%d, want 50/0", resp.Accepted, resp.Rejected)
	}
	if n := len(publisher.events()); n != 50 {
		t.Errorf("published %d events, want 50", n)
	}

	// One produce call for the whole batch is the reason this endpoint exists.
	if got := publisher.batchCount(); got != 1 {
		t.Errorf("produce calls = %d, want 1", got)
	}
}

func TestPostEventBatchRejectsOnlyTheBadItems(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	handler := ingest.NewHandler(ingest.Options{Publisher: publisher, Logger: discardLogger()})

	good := validBody()
	good["event_id"] = "00000000-0000-4000-8000-000000000001"

	badSeverity := validBody()
	badSeverity["event_id"] = "00000000-0000-4000-8000-000000000002"
	badSeverity["severity"] = "catastrophe"

	alsoGood := validBody()
	alsoGood["event_id"] = "00000000-0000-4000-8000-000000000003"

	body := jsonString(map[string]any{"events": []map[string]any{good, badSeverity, alsoGood}})

	rec := httptest.NewRecorder()
	handler.PostEventBatch(rec, batchRequest(body))

	// One bad event must not cost the caller the good ones.
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body: %s", rec.Code, rec.Body.String())
	}

	resp := decodeBatch(t, rec)
	if resp.Accepted != 2 || resp.Rejected != 1 {
		t.Fatalf("accepted/rejected = %d/%d, want 2/1", resp.Accepted, resp.Rejected)
	}
	if len(resp.Errors) != 1 {
		t.Fatalf("errors = %d, want 1", len(resp.Errors))
	}
	// The index is what tells the caller which of its events to fix.
	if resp.Errors[0].Index != 1 {
		t.Errorf("rejected index = %d, want 1", resp.Errors[0].Index)
	}
	if len(resp.Errors[0].Details) == 0 || resp.Errors[0].Details[0].Field != "severity" {
		t.Errorf("details = %+v, want a severity field error", resp.Errors[0].Details)
	}
	if n := len(publisher.events()); n != 2 {
		t.Errorf("published %d events, want 2", n)
	}
}

func TestPostEventBatchReportsAMalformedItemByIndex(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	handler := ingest.NewHandler(ingest.Options{Publisher: publisher, Logger: discardLogger()})

	good := validBody()
	unknownField := validBody()
	unknownField["event_id"] = "00000000-0000-4000-8000-000000000009"
	unknownField["shoe_size"] = 42

	body := jsonString(map[string]any{"events": []map[string]any{good, unknownField}})

	rec := httptest.NewRecorder()
	handler.PostEventBatch(rec, batchRequest(body))

	// Items are decoded one at a time, so strict decoding can reject one of them
	// without taking down the whole request.
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body: %s", rec.Code, rec.Body.String())
	}
	resp := decodeBatch(t, rec)
	if resp.Accepted != 1 || resp.Rejected != 1 {
		t.Errorf("accepted/rejected = %d/%d, want 1/1", resp.Accepted, resp.Rejected)
	}
	if len(resp.Errors) != 1 || resp.Errors[0].Index != 1 {
		t.Errorf("errors = %+v, want one at index 1", resp.Errors)
	}
}

func TestPostEventBatchWithNothingValidIsABadRequest(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	handler := ingest.NewHandler(ingest.Options{Publisher: publisher, Logger: discardLogger()})

	bad := validBody()
	bad["severity"] = "catastrophe"
	body := jsonString(map[string]any{"events": []map[string]any{bad}})

	rec := httptest.NewRecorder()
	handler.PostEventBatch(rec, batchRequest(body))

	// Nothing publishable means the request itself was wrong, not partially so.
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rec.Code, rec.Body.String())
	}
	if n := len(publisher.events()); n != 0 {
		t.Errorf("published %d events, want 0", n)
	}
}

func TestPostEventBatchRejectsAnEmptyBatch(t *testing.T) {
	t.Parallel()

	handler := ingest.NewHandler(ingest.Options{Publisher: &fakePublisher{}, Logger: discardLogger()})

	for _, body := range []string{`{"events":[]}`, `{}`} {
		rec := httptest.NewRecorder()
		handler.PostEventBatch(rec, batchRequest(body))

		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400", body, rec.Code)
		}
	}
}

func TestPostEventBatchEnforcesTheEventCap(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	handler := ingest.NewHandler(ingest.Options{
		Publisher:      publisher,
		Logger:         discardLogger(),
		MaxBatchEvents: 10,
	})

	rec := httptest.NewRecorder()
	handler.PostEventBatch(rec, batchRequest(batchBody(11)))

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body: %s", rec.Code, rec.Body.String())
	}
	if n := len(publisher.events()); n != 0 {
		t.Errorf("published %d events, want 0", n)
	}
}

func TestPostEventBatchEnforcesTheByteCap(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	handler := ingest.NewHandler(ingest.Options{
		Publisher:     publisher,
		Logger:        discardLogger(),
		MaxBatchBytes: 512,
	})

	rec := httptest.NewRecorder()
	handler.PostEventBatch(rec, batchRequest(batchBody(50)))

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body: %s", rec.Code, rec.Body.String())
	}
}

func TestPostEventBatchPublishFailureIsRetryable(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{err: errors.New("broker unreachable")}
	handler := ingest.NewHandler(ingest.Options{Publisher: publisher, Logger: discardLogger()})

	rec := httptest.NewRecorder()
	handler.PostEventBatch(rec, batchRequest(batchBody(5)))

	// A 202 must mean every accepted event is durable, so a failed publish is a
	// retryable failure for the whole request rather than a success with a
	// footnote.
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("a 503 should carry Retry-After")
	}
}

func TestPostEventBatchAppliesTimeBounds(t *testing.T) {
	t.Parallel()

	publisher := &fakePublisher{}
	handler := ingest.NewHandler(ingest.Options{
		Publisher: publisher,
		Logger:    discardLogger(),
		Bounds:    event.TimeBounds{MaxFuture: 5 * time.Minute},
		Now:       func() time.Time { return boundsNow },
	})

	good := validBody()
	good["event_id"] = "00000000-0000-4000-8000-000000000001"
	good["timestamp"] = boundsNow.Format(time.RFC3339)

	future := validBody()
	future["event_id"] = "00000000-0000-4000-8000-000000000002"
	future["timestamp"] = boundsNow.AddDate(4, 0, 0).Format(time.RFC3339)

	body := jsonString(map[string]any{"events": []map[string]any{good, future}})

	rec := httptest.NewRecorder()
	handler.PostEventBatch(rec, batchRequest(body))

	resp := decodeBatch(t, rec)
	if resp.Accepted != 1 || resp.Rejected != 1 {
		t.Fatalf("accepted/rejected = %d/%d, want 1/1", resp.Accepted, resp.Rejected)
	}
	if len(resp.Errors) != 1 || len(resp.Errors[0].Details) == 0 ||
		resp.Errors[0].Details[0].Field != "timestamp" {
		t.Errorf("errors = %+v, want a timestamp rejection", resp.Errors)
	}
}

func TestPostEventBatchRejectsNonJSONContentType(t *testing.T) {
	t.Parallel()

	handler := ingest.NewHandler(ingest.Options{Publisher: &fakePublisher{}, Logger: discardLogger()})

	req := httptest.NewRequest(http.MethodPost, "/v1/events:batch", strings.NewReader(batchBody(1)))
	req.Header.Set("Content-Type", "text/plain")

	rec := httptest.NewRecorder()
	handler.PostEventBatch(rec, req)

	if rec.Code != http.StatusUnsupportedMediaType {
		t.Errorf("status = %d, want 415", rec.Code)
	}
}
