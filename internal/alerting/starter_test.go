package alerting_test

import (
	"context"
	"errors"
	"testing"

	"github.com/acbspace/sentinel-flow-project/internal/alerting"
	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/incident"
)

type fakeIncidentSource struct {
	open    []incident.Incident
	listErr error
	markErr error
	marked  []string
}

func (f *fakeIncidentSource) ListOpenUnalerted(context.Context, int) ([]incident.Incident, error) {
	return f.open, f.listErr
}

func (f *fakeIncidentSource) MarkAlerted(_ context.Context, id string) error {
	if f.markErr != nil {
		return f.markErr
	}
	f.marked = append(f.marked, id)
	return nil
}

type fakeStarter struct {
	started []alerting.IncidentAlertInput
	err     error
}

func (f *fakeStarter) StartAlertWorkflow(_ context.Context, in alerting.IncidentAlertInput) error {
	if f.err != nil {
		return f.err
	}
	f.started = append(f.started, in)
	return nil
}

func newTestStarter(src alerting.IncidentSource, wf alerting.WorkflowStarter) *alerting.Starter {
	return alerting.NewStarter(alerting.StarterOptions{
		Incidents: src,
		Workflows: wf,
		Policy:    testPolicy(),
		Logger:    discardLogger(),
	})
}

func openIncident(id, service string) incident.Incident {
	return incident.Incident{
		ID: id, TenantID: "tenant-a", ServiceName: service, RuleID: "error_rate",
		Title: "elevated error rate", Severity: event.SeverityError, Status: incident.StatusOpen,
	}
}

func TestStarterStartsAndMarksEachIncident(t *testing.T) {
	t.Parallel()

	src := &fakeIncidentSource{open: []incident.Incident{
		openIncident("11111111-1111-1111-1111-111111111111", "payment-service"),
		openIncident("22222222-2222-2222-2222-222222222222", "order-service"),
	}}
	wf := &fakeStarter{}

	if err := newTestStarter(src, wf).StartPending(context.Background()); err != nil {
		t.Fatalf("StartPending() = %v", err)
	}

	if len(wf.started) != 2 {
		t.Fatalf("started %d workflows, want 2", len(wf.started))
	}
	if len(src.marked) != 2 {
		t.Fatalf("marked %d incidents, want 2", len(src.marked))
	}
	// The input carries the incident identity and the escalation policy.
	if wf.started[0].Incident.ID != "11111111-1111-1111-1111-111111111111" {
		t.Errorf("first workflow incident id = %q", wf.started[0].Incident.ID)
	}
	if len(wf.started[0].Policy.Levels) != 2 {
		t.Errorf("workflow input policy has %d levels, want 2", len(wf.started[0].Policy.Levels))
	}
}

func TestStarterDoesNotMarkWhenStartFails(t *testing.T) {
	t.Parallel()

	src := &fakeIncidentSource{open: []incident.Incident{
		openIncident("11111111-1111-1111-1111-111111111111", "payment-service"),
	}}
	wf := &fakeStarter{err: errors.New("temporal unreachable")}

	// A failed start is logged and skipped, not fatal: the incident stays
	// unmarked so a later tick retries it.
	if err := newTestStarter(src, wf).StartPending(context.Background()); err != nil {
		t.Fatalf("StartPending() = %v, want nil (failures are logged, not returned)", err)
	}
	if len(src.marked) != 0 {
		t.Errorf("marked %d incidents after a failed start, want 0", len(src.marked))
	}
}

func TestStarterReturnsErrorWhenListFails(t *testing.T) {
	t.Parallel()

	src := &fakeIncidentSource{listErr: errors.New("database down")}
	wf := &fakeStarter{}

	if err := newTestStarter(src, wf).StartPending(context.Background()); err == nil {
		t.Fatal("StartPending() = nil, want the list error to propagate")
	}
	if len(wf.started) != 0 {
		t.Errorf("started %d workflows despite a list failure, want 0", len(wf.started))
	}
}
