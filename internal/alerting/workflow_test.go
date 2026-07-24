package alerting_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/acbspace/sentinel-flow-project/internal/alerting"
	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/oncall"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// fakeStatusStore returns a scripted incident status for the workflow's DB
// re-check.
type fakeStatusStore struct {
	status incident.Status
	err    error
}

func (f *fakeStatusStore) Status(context.Context, string) (incident.Status, error) {
	return f.status, f.err
}

// fakeRecorder collects the notifications the real notifier records, so a test
// can assert exactly what was dispatched.
type fakeRecorder struct {
	mu      sync.Mutex
	records []store.Notification
}

func (f *fakeRecorder) Record(_ context.Context, n store.Notification) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, n)
	return nil
}

func (f *fakeRecorder) snapshot() []store.Notification {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]store.Notification(nil), f.records...)
}

func testPolicy() oncall.EscalationPolicy {
	return oncall.EscalationPolicy{Levels: []oncall.Level{
		{Target: "primary", AckTimeout: 2 * time.Minute, Rotation: oncall.Rotation{Contacts: []oncall.Contact{{Name: "alice"}}}},
		{Target: "secondary", AckTimeout: 5 * time.Minute, Rotation: oncall.Rotation{Contacts: []oncall.Contact{{Name: "bob"}}}},
	}}
}

func testInput() alerting.IncidentAlertInput {
	return alerting.IncidentAlertInput{
		Incident: alerting.IncidentRef{
			ID: "11111111-1111-1111-1111-111111111111", TenantID: "tenant-a",
			ServiceName: "payment-service", Severity: "error", Title: "elevated error rate",
		},
		Policy: testPolicy(),
	}
}

// runWorkflow executes the alert workflow in a Temporal test environment with the
// real activities wired to the supplied status, and returns the recorded
// notifications. beforeExecute can schedule signals via the environment.
func runWorkflow(t *testing.T, status incident.Status, beforeExecute func(env *testsuite.TestWorkflowEnvironment)) []store.Notification {
	t.Helper()

	recorder := &fakeRecorder{}
	activities := alerting.NewActivities(
		&fakeStatusStore{status: status},
		alerting.NewNotifier(recorder, "", 0, discardLogger()),
		discardLogger(),
	)

	ts := &testsuite.WorkflowTestSuite{}
	ts.SetLogger(discardLogger())
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities)

	if beforeExecute != nil {
		beforeExecute(env)
	}

	env.ExecuteWorkflow(alerting.IncidentAlertWorkflow, testInput())

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	return recorder.snapshot()
}

func TestWorkflowEscalatesThroughEveryLevelWhenUnacknowledged(t *testing.T) {
	t.Parallel()

	records := runWorkflow(t, incident.StatusOpen, nil)

	if len(records) != 3 {
		t.Fatalf("recorded %d notifications, want 3 (two levels + exhausted): %+v", len(records), records)
	}
	if records[0].Level != 1 || records[0].Target != "primary" {
		t.Errorf("first = level %d %q, want level 1 primary", records[0].Level, records[0].Target)
	}
	if records[1].Level != 2 || records[1].Target != "secondary" {
		t.Errorf("second = level %d %q, want level 2 secondary", records[1].Level, records[1].Target)
	}
	if records[2].Target != "escalation exhausted" {
		t.Errorf("third target = %q, want escalation exhausted", records[2].Target)
	}
}

func TestWorkflowStopsOnAcknowledgeSignal(t *testing.T) {
	t.Parallel()

	// Acknowledge 30s in — before the 2m level-1 timeout — so escalation never
	// reaches level 2 and never exhausts.
	records := runWorkflow(t, incident.StatusOpen, func(env *testsuite.TestWorkflowEnvironment) {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(alerting.SignalAcknowledge, nil)
		}, 30*time.Second)
	})

	if len(records) != 1 {
		t.Fatalf("recorded %d notifications, want 1 (level 1 only): %+v", len(records), records)
	}
	if records[0].Level != 1 {
		t.Errorf("notified level %d, want 1", records[0].Level)
	}
}

func TestWorkflowStopsOnResolveSignal(t *testing.T) {
	t.Parallel()

	records := runWorkflow(t, incident.StatusOpen, func(env *testsuite.TestWorkflowEnvironment) {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(alerting.SignalResolve, nil)
		}, 30*time.Second)
	})

	if len(records) != 1 {
		t.Fatalf("recorded %d notifications, want 1: %+v", len(records), records)
	}
}

func TestWorkflowSendsNothingWhenIncidentAlreadyClosed(t *testing.T) {
	t.Parallel()

	for _, status := range []incident.Status{incident.StatusAcknowledged, incident.StatusResolved} {
		status := status
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			// The DB re-check at level 1 sees the incident is already handled, so
			// the workflow pages no one.
			if records := runWorkflow(t, status, nil); len(records) != 0 {
				t.Errorf("recorded %d notifications for an already-%s incident, want 0", len(records), status)
			}
		})
	}
}
