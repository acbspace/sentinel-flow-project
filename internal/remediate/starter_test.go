package remediate_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/remediate"
	"github.com/acbspace/sentinel-flow-project/internal/runbook"
)

type fakeIncidentSource struct {
	open    []incident.Incident
	listErr error
	marked  []string
}

func (f *fakeIncidentSource) ListOpenUnremediated(context.Context, int) ([]incident.Incident, error) {
	return f.open, f.listErr
}

func (f *fakeIncidentSource) MarkRemediated(_ context.Context, id string) error {
	f.marked = append(f.marked, id)
	return nil
}

type fakeWorkflowStarter struct {
	started []remediate.IncidentRemediationInput
	err     error
}

func (f *fakeWorkflowStarter) StartRemediationWorkflow(_ context.Context, in remediate.IncidentRemediationInput) error {
	if f.err != nil {
		return f.err
	}
	f.started = append(f.started, in)
	return nil
}

func testCatalog() runbook.Catalog {
	return runbook.Catalog{Runbooks: []runbook.Runbook{{
		ID:              "error-rate-response",
		Match:           runbook.Matcher{RuleID: "error_rate"},
		ApprovalTimeout: time.Minute,
		Steps:           []runbook.Step{{Name: "s", Kind: runbook.ActionNoop, Mode: runbook.ModeAuto}},
	}}}
}

func newTestStarter(src remediate.IncidentSource, wf remediate.WorkflowStarter) *remediate.Starter {
	return remediate.NewStarter(remediate.StarterOptions{
		Incidents: src,
		Workflows: wf,
		Catalog:   testCatalog(),
		Logger:    discardLogger(),
	})
}

func openIncident(id, ruleID string) incident.Incident {
	return incident.Incident{
		ID: id, TenantID: "tenant-a", ServiceName: "payment-service", RuleID: ruleID,
		Title: "elevated error rate", Severity: event.SeverityError, Status: incident.StatusOpen,
	}
}

func TestStarterStartsRunForMatchingIncident(t *testing.T) {
	t.Parallel()

	src := &fakeIncidentSource{open: []incident.Incident{
		openIncident("11111111-1111-1111-1111-111111111111", "error_rate"),
	}}
	wf := &fakeWorkflowStarter{}

	if err := newTestStarter(src, wf).StartPending(context.Background()); err != nil {
		t.Fatalf("StartPending() = %v", err)
	}

	if len(wf.started) != 1 {
		t.Fatalf("started %d runs, want 1", len(wf.started))
	}
	if got := wf.started[0].Runbook.ID; got != "error-rate-response" {
		t.Errorf("started runbook = %q, want error-rate-response", got)
	}
	if len(src.marked) != 1 {
		t.Errorf("marked %d incidents, want 1", len(src.marked))
	}
}

func TestStarterMarksButDoesNotRunUncoveredIncident(t *testing.T) {
	t.Parallel()

	// No runbook matches this rule: there is nothing to automate, but the
	// incident must still be marked so it is not re-examined forever.
	src := &fakeIncidentSource{open: []incident.Incident{
		openIncident("22222222-2222-2222-2222-222222222222", "high_latency"),
	}}
	wf := &fakeWorkflowStarter{}

	if err := newTestStarter(src, wf).StartPending(context.Background()); err != nil {
		t.Fatalf("StartPending() = %v", err)
	}

	if len(wf.started) != 0 {
		t.Errorf("started %d runs for an uncovered incident, want 0", len(wf.started))
	}
	if len(src.marked) != 1 {
		t.Errorf("marked %d incidents, want 1 (so it is not rechecked forever)", len(src.marked))
	}
}

func TestStarterDoesNotMarkWhenStartFails(t *testing.T) {
	t.Parallel()

	src := &fakeIncidentSource{open: []incident.Incident{
		openIncident("33333333-3333-3333-3333-333333333333", "error_rate"),
	}}
	wf := &fakeWorkflowStarter{err: errors.New("temporal unreachable")}

	if err := newTestStarter(src, wf).StartPending(context.Background()); err != nil {
		t.Fatalf("StartPending() = %v, want nil (failures are logged, not returned)", err)
	}
	if len(src.marked) != 0 {
		t.Errorf("marked %d incidents after a failed start, want 0 so it retries", len(src.marked))
	}
}

func TestStarterReturnsErrorWhenListFails(t *testing.T) {
	t.Parallel()

	src := &fakeIncidentSource{listErr: errors.New("database down")}
	if err := newTestStarter(src, &fakeWorkflowStarter{}).StartPending(context.Background()); err == nil {
		t.Fatal("StartPending() = nil, want the list error to propagate")
	}
}
