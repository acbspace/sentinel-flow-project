// Package remediate runs a runbook against an incident: a Temporal workflow that
// walks the runbook's steps, executes the ones marked safe to run unattended, and
// pauses at the ones that need a human to approve before anything touches
// production.
//
// The safety posture is deliberate and one-directional: a rejection, an approval
// timeout, or a failed step halts the runbook. Automation never continues after a
// human has said no, after nobody answered, or after something already went
// wrong — and every one of those outcomes is recorded, not just the happy path.
package remediate

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/runbook"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

// TaskQueue is the Temporal task queue the remediation worker listens on.
const TaskQueue = "incident-remediation"

// Signals a human sends to release or stop a gated step.
const (
	SignalApprove = "approve"
	SignalReject  = "reject"
)

// WorkflowID is the deterministic workflow id for an incident's remediation.
// As with alerting, deriving it from the incident id makes "one remediation run
// per incident" a Temporal guarantee and lets the API address the workflow.
func WorkflowID(incidentID string) string {
	return "incident-remediation-" + incidentID
}

// IncidentRef is the incident snapshot the workflow needs.
type IncidentRef struct {
	ID          string
	TenantID    string
	ServiceName string
	Severity    string
	Title       string
}

// IncidentRemediationInput starts a remediation run. The runbook travels in the
// input so the workflow replays deterministically even if the catalog changes
// while a run is in flight — the same reasoning as the escalation policy.
type IncidentRemediationInput struct {
	Incident IncidentRef
	Runbook  runbook.Runbook
}

// Decision is the payload of an approve/reject signal, so the audit trail can
// record who released the step.
type Decision struct {
	Actor string
}

// actionNamespace seeds the deterministic action ids.
var actionNamespace = uuid.MustParse("6f1c2d3e-4a5b-11ee-be56-0242ac120002")

// actionID derives a stable UUID for one step of one incident's runbook. Pure, so
// it is safe inside a workflow, and it makes the audit write idempotent: a retry
// or replay updates the same row instead of creating a second.
func actionID(incidentID string, stepIndex int) string {
	return uuid.NewSHA1(actionNamespace, fmt.Appendf(nil, "%s:%d", incidentID, stepIndex)).String()
}

// IncidentRemediationWorkflow executes a runbook against one incident.
func IncidentRemediationWorkflow(ctx workflow.Context, in IncidentRemediationInput) error {
	log := workflow.GetLogger(ctx)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})

	var (
		approved bool
		rejected bool
		actor    string
	)
	// Receive in a loop: a runbook may gate more than one step, so the channel
	// must keep listening rather than consuming a single decision.
	workflow.Go(ctx, func(gctx workflow.Context) {
		ch := workflow.GetSignalChannel(gctx, SignalApprove)
		for {
			var d Decision
			ch.Receive(gctx, &d)
			approved, actor = true, d.Actor
		}
	})
	workflow.Go(ctx, func(gctx workflow.Context) {
		ch := workflow.GetSignalChannel(gctx, SignalReject)
		for {
			var d Decision
			ch.Receive(gctx, &d)
			rejected, actor = true, d.Actor
		}
	})

	// Methods referenced by name; the worker holds the real instance.
	var a *Activities

	record := func(rec ActionRecord) error {
		if err := workflow.ExecuteActivity(ctx, a.RecordAction, rec).Get(ctx, nil); err != nil {
			return fmt.Errorf("record remediation step %d: %w", rec.StepIndex, err)
		}
		return nil
	}

	for i, step := range in.Runbook.Steps {
		stepNum := i + 1
		rec := ActionRecord{
			ID:         actionID(in.Incident.ID, stepNum),
			IncidentID: in.Incident.ID,
			RunbookID:  in.Runbook.ID,
			StepIndex:  stepNum,
			StepName:   step.Name,
			ActionKind: string(step.Kind),
			Mode:       string(step.Mode),
		}

		// Never act on an incident that is no longer live. Somebody resolving it
		// is the clearest possible signal that automation should stand down.
		var status string
		if err := workflow.ExecuteActivity(ctx, a.CheckIncidentStatus, in.Incident.ID).Get(ctx, &status); err != nil {
			return fmt.Errorf("check incident status: %w", err)
		}
		if s := incident.Status(status); s != incident.StatusOpen && s != incident.StatusAcknowledged {
			rec.Status = store.RemediationSkipped
			rec.Detail = map[string]any{"reason": "incident is no longer active", "incident_status": status}
			log.Info("remediation halted: incident no longer active",
				"incident_id", in.Incident.ID, "step", step.Name, "incident_status", status)
			return record(rec)
		}

		if step.Mode == runbook.ModeApproval {
			rec.Status = store.RemediationPending
			rec.Detail = map[string]any{"awaiting": "approval", "timeout": in.Runbook.ApprovalTimeout.String()}
			if err := record(rec); err != nil {
				return err
			}
			log.Info("remediation step awaiting approval",
				"incident_id", in.Incident.ID, "step", step.Name)

			met, err := workflow.AwaitWithTimeout(ctx, in.Runbook.ApprovalTimeout, func() bool { return approved || rejected })
			if err != nil {
				return err
			}

			switch {
			case !met:
				rec.Status = store.RemediationTimedOut
				rec.Detail = map[string]any{"reason": "nobody approved within the timeout"}
				log.Warn("remediation halted: approval timed out",
					"incident_id", in.Incident.ID, "step", step.Name)
				return record(rec)
			case rejected:
				rec.Status = store.RemediationRejected
				rec.Actor = actor
				rec.Detail = map[string]any{"reason": "rejected by an operator"}
				log.Info("remediation halted: step rejected",
					"incident_id", in.Incident.ID, "step", step.Name, "actor", actor)
				return record(rec)
			}

			rec.Status = store.RemediationApproved
			rec.Actor = actor
			rec.Detail = map[string]any{"approved_by": actor}
			if err := record(rec); err != nil {
				return err
			}

			// Clear the decision so a later gated step waits for its own.
			approved, rejected, actor = false, false, ""
		}

		var result ExecutionResult
		execErr := workflow.ExecuteActivity(ctx, a.ExecuteAction, ExecuteArgs{
			IncidentID:  in.Incident.ID,
			ServiceName: in.Incident.ServiceName,
			Title:       in.Incident.Title,
			StepName:    step.Name,
			Kind:        string(step.Kind),
			Target:      step.Target,
			Params:      step.Params,
		}).Get(ctx, &result)

		if execErr != nil {
			rec.Status = store.RemediationFailed
			rec.Detail = map[string]any{"error": execErr.Error()}
			log.Error("remediation halted: step failed",
				"incident_id", in.Incident.ID, "step", step.Name, "error", execErr.Error())
			// Record the failure and stop. Continuing to automate after something
			// went wrong is how a small incident becomes a large one.
			return record(rec)
		}

		rec.Status = store.RemediationSucceeded
		rec.Detail = result.Detail
		if err := record(rec); err != nil {
			return err
		}
		log.Info("remediation step completed",
			"incident_id", in.Incident.ID, "step", step.Name, "kind", string(step.Kind))
	}

	log.Info("remediation runbook complete",
		"incident_id", in.Incident.ID, "runbook_id", in.Runbook.ID)
	return nil
}
