package alerting_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/alerting"
)

func sampleArgs() alerting.SendNotificationArgs {
	return alerting.SendNotificationArgs{
		NotificationID: "22222222-2222-2222-2222-222222222222",
		IncidentID:     "11111111-1111-1111-1111-111111111111",
		Level:          1,
		Target:         "primary",
		Contact:        "alice",
		Title:          "elevated error rate",
		Severity:       "error",
		Reason:         "escalation",
	}
}

func TestNotifierLogsWhenNoWebhookConfigured(t *testing.T) {
	t.Parallel()

	rec := &fakeRecorder{}
	n := alerting.NewNotifier(rec, "", 0, discardLogger())

	if err := n.Dispatch(context.Background(), sampleArgs()); err != nil {
		t.Fatalf("Dispatch() = %v", err)
	}

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("recorded %d notifications, want 1", len(got))
	}
	if got[0].Channel != "log" || got[0].Status != "sent" {
		t.Errorf("channel/status = %q/%q, want log/sent", got[0].Channel, got[0].Status)
	}
}

func TestNotifierDeliversWebhook(t *testing.T) {
	t.Parallel()

	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rec := &fakeRecorder{}
	n := alerting.NewNotifier(rec, srv.URL, time.Second, discardLogger())

	if err := n.Dispatch(context.Background(), sampleArgs()); err != nil {
		t.Fatalf("Dispatch() = %v", err)
	}

	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Errorf("webhook received %d requests, want 1", got)
	}
	if got := rec.snapshot(); got[0].Channel != "webhook" || got[0].Status != "sent" {
		t.Errorf("channel/status = %q/%q, want webhook/sent", got[0].Channel, got[0].Status)
	}
}

func TestNotifierRecordsFailedWebhookWithoutErroring(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	rec := &fakeRecorder{}
	n := alerting.NewNotifier(rec, srv.URL, time.Second, discardLogger())

	// A webhook failure must not fail the dispatch: the audit row is still the
	// point, and escalation should not stall on a flaky endpoint.
	if err := n.Dispatch(context.Background(), sampleArgs()); err != nil {
		t.Fatalf("Dispatch() returned %v, want nil on webhook failure", err)
	}

	got := rec.snapshot()
	if got[0].Status != "failed" || got[0].Channel != "webhook" {
		t.Errorf("status/channel = %q/%q, want failed/webhook", got[0].Status, got[0].Channel)
	}
	if _, ok := got[0].Detail["error"]; !ok {
		t.Error("a failed notification should record an error in its detail")
	}
}
