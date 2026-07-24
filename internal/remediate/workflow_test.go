package remediate_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/remediate"
	"github.com/acbspace/sentinel-flow-project/internal/runbook"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

type fakeStatusStore struct {
	status incident.Status
}

func (f *fakeStatusStore) Status(context.Context, string) (incident.Status, error) {
	return f.status, nil
}

// fakeRecorder captures the audit rows the workflow writes. Because a step's row
// is upserted as its status advances, the recorder keeps the latest status per
// step index as well as the full ordered history.
type fakeRecorder struct {
	mu      sync.Mutex
	history []store.RemediationAction
}

func (f *fakeRecorder) Upsert(_ context.Context, a store.RemediationAction) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.history = append(f.history, a)
	return nil
}

// finalStatuses returns the last recorded status for each step, in step order.
func (f *fakeRecorder) finalStatuses() map[int]string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make(map[int]string)
	for _, a := range f.history {
		out[a.StepIndex] = a.Status
	}
	return out
}

func (f *fakeRecorder) snapshot() []store.RemediationAction {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]store.RemediationAction(nil), f.history...)
}

// testRunbook is one auto step followed by one approval-gated step, which is the
// shape the shipped catalog uses.
func testRunbook() runbook.Runbook {
	return runbook.Runbook{
		ID:              "test-runbook",
		Name:            "test",
		Match:           runbook.Matcher{RuleID: "error_rate"},
		ApprovalTimeout: 10 * time.Minute,
		Steps: []runbook.Step{
			{Name: "capture diagnostics", Kind: runbook.ActionNoop, Mode: runbook.ModeAuto},
			{Name: "restart instances", Kind: runbook.ActionNoop, Mode: runbook.ModeApproval},
		},
	}
}

func testInput() remediate.IncidentRemediationInput {
	return remediate.IncidentRemediationInput{
		Incident: remediate.IncidentRef{
			ID: "11111111-1111-1111-1111-111111111111", TenantID: "tenant-a",
			ServiceName: "payment-service", Severity: "error", Title: "elevated error rate",
		},
		Runbook: testRunbook(),
	}
}

// runWorkflow executes the remediation workflow against the real activities over
// in-memory fakes, and returns the recorder.
func runWorkflow(t *testing.T, status incident.Status, beforeExecute func(env *testsuite.TestWorkflowEnvironment)) *fakeRecorder {
	t.Helper()

	recorder := &fakeRecorder{}
	activities := remediate.NewActivities(
		&fakeStatusStore{status: status},
		recorder,
		remediate.NewExecutor(time.Second, discardLogger()),
		discardLogger(),
	)

	ts := &testsuite.WorkflowTestSuite{}
	ts.SetLogger(discardLogger())
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities)

	if beforeExecute != nil {
		beforeExecute(env)
	}

	env.ExecuteWorkflow(remediate.IncidentRemediationWorkflow, testInput())

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	return recorder
}

func TestRemediationRunsAutoStepThenWaitsForApproval(t *testing.T) {
	t.Parallel()

	// Approve 30s in, well inside the 10m approval window.
	rec := runWorkflow(t, incident.StatusOpen, func(env *testsuite.TestWorkflowEnvironment) {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(remediate.SignalApprove, remediate.Decision{Actor: "alice"})
		}, 30*time.Second)
	})

	final := rec.finalStatuses()
	if final[1] != store.RemediationSucceeded {
		t.Errorf("step 1 final status = %q, want succeeded", final[1])
	}
	if final[2] != store.RemediationSucceeded {
		t.Errorf("step 2 final status = %q, want succeeded after approval", final[2])
	}

	// The gated step must have been recorded as pending before it ran, and the
	// approver must be captured in the trail.
	var sawPending, sawApprover bool
	for _, a := range rec.snapshot() {
		if a.StepIndex == 2 && a.Status == store.RemediationPending {
			sawPending = true
		}
		if a.StepIndex == 2 && a.Actor == "alice" {
			sawApprover = true
		}
	}
	if !sawPending {
		t.Error("the gated step was never recorded as pending")
	}
	if !sawApprover {
		t.Error("the approving actor was not recorded")
	}
}

func TestRemediationHaltsWhenStepIsRejected(t *testing.T) {
	t.Parallel()

	rec := runWorkflow(t, incident.StatusOpen, func(env *testsuite.TestWorkflowEnvironment) {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(remediate.SignalReject, remediate.Decision{Actor: "bob"})
		}, 30*time.Second)
	})

	final := rec.finalStatuses()
	if final[1] != store.RemediationSucceeded {
		t.Errorf("step 1 final status = %q, want succeeded", final[1])
	}
	// The rejected step must be recorded as rejected and never executed.
	if final[2] != store.RemediationRejected {
		t.Errorf("step 2 final status = %q, want rejected", final[2])
	}
}

func TestRemediationHaltsWhenApprovalTimesOut(t *testing.T) {
	t.Parallel()

	// No signal at all: the approval window expires.
	rec := runWorkflow(t, incident.StatusOpen, nil)

	final := rec.finalStatuses()
	if final[2] != store.RemediationTimedOut {
		t.Errorf("step 2 final status = %q, want timed_out when nobody approves", final[2])
	}
}

func TestRemediationSkipsWhenIncidentAlreadyResolved(t *testing.T) {
	t.Parallel()

	// Somebody resolving the incident is the clearest signal automation should
	// stand down — even the first, unattended step must not run.
	rec := runWorkflow(t, incident.StatusResolved, nil)

	final := rec.finalStatuses()
	if final[1] != store.RemediationSkipped {
		t.Errorf("step 1 final status = %q, want skipped for a resolved incident", final[1])
	}
	if _, ran := final[2]; ran {
		t.Error("a later step was recorded even though the runbook should have halted")
	}
}
