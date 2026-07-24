package remediate_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/remediate"
	"github.com/acbspace/sentinel-flow-project/internal/runbook"
)

func TestExecutorNoop(t *testing.T) {
	t.Parallel()

	e := remediate.NewExecutor(time.Second, discardLogger())
	detail, err := e.Execute(context.Background(), remediate.ExecuteArgs{
		IncidentID: "i1", StepName: "capture diagnostics", Kind: string(runbook.ActionNoop),
		Params: map[string]string{"collect": "logs"},
	})
	if err != nil {
		t.Fatalf("Execute() = %v", err)
	}
	if detail["executed"] != "noop" {
		t.Errorf("detail[executed] = %v, want noop", detail["executed"])
	}
}

func TestExecutorWebhookSuccess(t *testing.T) {
	t.Parallel()

	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	e := remediate.NewExecutor(time.Second, discardLogger())
	detail, err := e.Execute(context.Background(), remediate.ExecuteArgs{
		IncidentID: "i1", StepName: "restart", Kind: string(runbook.ActionWebhook), Target: srv.URL,
	})
	if err != nil {
		t.Fatalf("Execute() = %v", err)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Errorf("target received %d requests, want 1", got)
	}
	if detail["status_code"] != http.StatusAccepted {
		t.Errorf("detail[status_code] = %v, want %d", detail["status_code"], http.StatusAccepted)
	}
}

func TestExecutorFailures(t *testing.T) {
	t.Parallel()

	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failing.Close()

	tests := []struct {
		name string
		args remediate.ExecuteArgs
	}{
		{
			name: "non-2xx response fails the step",
			args: remediate.ExecuteArgs{StepName: "restart", Kind: string(runbook.ActionWebhook), Target: failing.URL},
		},
		{
			name: "webhook without a target fails the step",
			args: remediate.ExecuteArgs{StepName: "restart", Kind: string(runbook.ActionWebhook)},
		},
		{
			// An unrecognised action must fail loudly rather than quietly
			// "succeed" having done nothing.
			name: "unknown action kind fails the step",
			args: remediate.ExecuteArgs{StepName: "mystery", Kind: "teleport"},
		},
	}

	e := remediate.NewExecutor(time.Second, discardLogger())
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if _, err := e.Execute(context.Background(), tc.args); err == nil {
				t.Error("Execute() = nil, want an error")
			}
		})
	}
}
