package remediate

import (
	"context"
	"errors"
	"log/slog"

	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

// IncidentStatusStore reads an incident's current status.
// *store.IncidentStore satisfies it.
type IncidentStatusStore interface {
	Status(ctx context.Context, id string) (incident.Status, error)
}

// ActionRecorder persists one step of a runbook run.
// *store.RemediationStore satisfies it.
type ActionRecorder interface {
	Upsert(ctx context.Context, a store.RemediationAction) error
}

// ActionRecord is the workflow's view of an audit row. It is mapped onto the
// store's type in the activity, keeping the workflow free of storage concerns.
type ActionRecord struct {
	ID         string
	IncidentID string
	RunbookID  string
	StepIndex  int
	StepName   string
	ActionKind string
	Mode       string
	Status     string
	Actor      string
	Detail     map[string]any
}

// ExecuteArgs is what the workflow hands the ExecuteAction activity.
type ExecuteArgs struct {
	IncidentID  string
	ServiceName string
	Title       string
	StepName    string
	Kind        string
	Target      string
	Params      map[string]string
}

// ExecutionResult is what a successful action reports back for the audit trail.
type ExecutionResult struct {
	Detail map[string]any
}

// Activities holds the non-deterministic dependencies the remediation workflow
// reaches through: the database and the action executor.
type Activities struct {
	incidents IncidentStatusStore
	recorder  ActionRecorder
	executor  *Executor
	log       *slog.Logger
}

// NewActivities builds the activity set.
func NewActivities(incidents IncidentStatusStore, recorder ActionRecorder, executor *Executor, log *slog.Logger) *Activities {
	return &Activities{incidents: incidents, recorder: recorder, executor: executor, log: log}
}

// CheckIncidentStatus returns the incident's current status. A missing incident
// reads as resolved, which halts the runbook — the safe direction to fail.
func (a *Activities) CheckIncidentStatus(ctx context.Context, incidentID string) (string, error) {
	status, err := a.incidents.Status(ctx, incidentID)
	if err != nil {
		if errors.Is(err, store.ErrIncidentNotFound) {
			return string(incident.StatusResolved), nil
		}
		return "", err
	}
	return string(status), nil
}

// RecordAction writes or advances one step's audit row.
func (a *Activities) RecordAction(ctx context.Context, rec ActionRecord) error {
	return a.recorder.Upsert(ctx, store.RemediationAction{
		ID:         rec.ID,
		IncidentID: rec.IncidentID,
		RunbookID:  rec.RunbookID,
		StepIndex:  rec.StepIndex,
		StepName:   rec.StepName,
		ActionKind: rec.ActionKind,
		Mode:       rec.Mode,
		Status:     rec.Status,
		Actor:      rec.Actor,
		Detail:     rec.Detail,
	})
}

// ExecuteAction performs one runbook step.
func (a *Activities) ExecuteAction(ctx context.Context, args ExecuteArgs) (ExecutionResult, error) {
	detail, err := a.executor.Execute(ctx, args)
	if err != nil {
		return ExecutionResult{}, err
	}
	return ExecutionResult{Detail: detail}, nil
}
