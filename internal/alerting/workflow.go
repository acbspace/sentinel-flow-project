// Package alerting turns an open incident into a page. Its heart is a Temporal
// workflow that notifies the on-call responder and, if the incident is not
// acknowledged in time, escalates through the policy until someone owns it or the
// levels run out.
//
// Temporal is the right tool here because escalation is durable, timer-driven and
// human-in-the-loop: the workflow may sleep for minutes between levels, must
// survive process restarts, and reacts to an out-of-band acknowledgement. The
// database remains the source of truth — signals are the fast path, and the
// workflow re-reads incident status before each escalation so a lost signal still
// converges to the right outcome.
package alerting

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/oncall"
)

// TaskQueue is the Temporal task queue the alert worker listens on and that the
// starter targets when it schedules workflows.
const TaskQueue = "incident-alerts"

// Signal names the incidents-api sends to stop an escalation early.
const (
	SignalAcknowledge = "acknowledge"
	SignalResolve     = "resolve"
)

const (
	reasonEscalation = "escalation"
	reasonExhausted  = "exhausted"
)

// WorkflowID is the deterministic Temporal workflow id for an incident's alert.
// Deriving it from the incident id makes "one alert workflow per incident" a
// Temporal guarantee, and lets the incidents-api address the workflow to signal
// it without any shared lookup table. A recurrence is a new incident (new id, per
// milestone 2), so it gets its own workflow.
func WorkflowID(incidentID string) string {
	return "incident-alert-" + incidentID
}

// IncidentRef is the incident snapshot the workflow needs, captured at start so
// the workflow does not depend on the row mutating beneath it.
type IncidentRef struct {
	ID          string
	TenantID    string
	ServiceName string
	Severity    string
	Title       string
}

// IncidentAlertInput starts an alert workflow. The escalation policy travels in
// the input so the workflow replays deterministically even if the deployment's
// policy changes while a workflow is in flight.
type IncidentAlertInput struct {
	Incident IncidentRef
	Policy   oncall.EscalationPolicy
}

// notificationNamespace seeds the deterministic notification ids. Any fixed UUID
// works; this is an arbitrary constant.
var notificationNamespace = uuid.MustParse("1b4e28ba-2fa1-11d2-883f-0016d3cca427")

// notificationID derives a stable UUID from the incident, level and reason. It is
// pure, so it is safe to call inside a workflow, and it makes the record activity
// idempotent: a retry or replay computes the same id and lands on ON CONFLICT DO
// NOTHING.
func notificationID(incidentID string, level int, reason string) string {
	name := fmt.Sprintf("%s:%d:%s", incidentID, level, reason)
	return uuid.NewSHA1(notificationNamespace, []byte(name)).String()
}

// IncidentAlertWorkflow pages the on-call responder for an incident and escalates
// through the policy until it is acknowledged or resolved.
func IncidentAlertWorkflow(ctx workflow.Context, in IncidentAlertInput) error {
	log := workflow.GetLogger(ctx)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	})

	var acked, resolved bool
	// Signals are the fast path to stop escalating; the per-level DB re-check
	// below is the authority if a signal is ever lost.
	workflow.Go(ctx, func(gctx workflow.Context) {
		workflow.GetSignalChannel(gctx, SignalAcknowledge).Receive(gctx, nil)
		acked = true
	})
	workflow.Go(ctx, func(gctx workflow.Context) {
		workflow.GetSignalChannel(gctx, SignalResolve).Receive(gctx, nil)
		resolved = true
	})

	// The methods are referenced by name; the worker registers the real instance.
	var a *Activities

	for i, level := range in.Policy.Levels {
		levelNum := i + 1

		var status string
		if err := workflow.ExecuteActivity(ctx, a.CheckIncidentStatus, in.Incident.ID).Get(ctx, &status); err != nil {
			return fmt.Errorf("check incident status: %w", err)
		}
		switch incident.Status(status) {
		case incident.StatusResolved:
			resolved = true
		case incident.StatusAcknowledged:
			acked = true
		}
		if acked || resolved {
			break
		}

		contact := level.Rotation.OnCallAt(workflow.Now(ctx))
		args := SendNotificationArgs{
			NotificationID: notificationID(in.Incident.ID, levelNum, reasonEscalation),
			IncidentID:     in.Incident.ID,
			Level:          levelNum,
			Target:         level.Target,
			Contact:        contact.Name,
			ContactAddress: contact.Address,
			Title:          in.Incident.Title,
			Severity:       in.Incident.Severity,
			Reason:         reasonEscalation,
		}
		if err := workflow.ExecuteActivity(ctx, a.SendNotification, args).Get(ctx, nil); err != nil {
			return fmt.Errorf("send notification for level %d: %w", levelNum, err)
		}
		log.Info("paged on-call",
			"incident_id", in.Incident.ID, "level", levelNum,
			"target", level.Target, "contact", contact.Name)

		met, err := workflow.AwaitWithTimeout(ctx, level.AckTimeout, func() bool { return acked || resolved })
		if err != nil {
			return err
		}
		if met {
			break // acknowledged or resolved within the window
		}
		// timed out with no acknowledgement → escalate to the next level
	}

	if !acked && !resolved {
		last := len(in.Policy.Levels)
		args := SendNotificationArgs{
			NotificationID: notificationID(in.Incident.ID, last, reasonExhausted),
			IncidentID:     in.Incident.ID,
			Level:          last,
			Target:         "escalation exhausted",
			Contact:        "none",
			Title:          in.Incident.Title,
			Severity:       in.Incident.Severity,
			Reason:         reasonExhausted,
		}
		if err := workflow.ExecuteActivity(ctx, a.SendNotification, args).Get(ctx, nil); err != nil {
			return fmt.Errorf("send exhausted notification: %w", err)
		}
		log.Warn("escalation exhausted with no acknowledgement", "incident_id", in.Incident.ID)
	}

	return nil
}
