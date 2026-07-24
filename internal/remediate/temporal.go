package remediate

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

// TemporalStarter starts remediation workflows through a Temporal client.
type TemporalStarter struct {
	client    client.Client
	taskQueue string
}

// NewTemporalStarter builds a TemporalStarter over a connected client.
func NewTemporalStarter(c client.Client, taskQueue string) *TemporalStarter {
	return &TemporalStarter{client: c, taskQueue: taskQueue}
}

// StartRemediationWorkflow starts an incident's remediation run, idempotently.
//
// The workflow id is the incident id and duplicates are rejected, so an already
// existing run — in progress or finished — is treated as success. An incident is
// never remediated twice.
func (s *TemporalStarter) StartRemediationWorkflow(ctx context.Context, in IncidentRemediationInput) error {
	opts := client.StartWorkflowOptions{
		ID:                    WorkflowID(in.Incident.ID),
		TaskQueue:             s.taskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}

	_, err := s.client.ExecuteWorkflow(ctx, opts, IncidentRemediationWorkflow, in)
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &alreadyStarted) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("execute remediation workflow for incident %s: %w", in.Incident.ID, err)
	}
	return nil
}

// ErrNoRemediationRun reports that an approve/reject arrived for an incident with
// no remediation workflow still running — it already finished, or never started.
var ErrNoRemediationRun = errors.New("no remediation run is in progress for this incident")

// TemporalSignaler releases or stops a gated remediation step.
type TemporalSignaler struct {
	client client.Client
}

// NewTemporalSignaler builds a signaler over a connected client.
func NewTemporalSignaler(c client.Client) *TemporalSignaler {
	return &TemporalSignaler{client: c}
}

// Approve releases the step currently awaiting a decision.
func (s *TemporalSignaler) Approve(ctx context.Context, incidentID, actor string) error {
	return s.signal(ctx, incidentID, SignalApprove, actor)
}

// Reject stops the runbook at the step currently awaiting a decision.
func (s *TemporalSignaler) Reject(ctx context.Context, incidentID, actor string) error {
	return s.signal(ctx, incidentID, SignalReject, actor)
}

func (s *TemporalSignaler) signal(ctx context.Context, incidentID, name, actor string) error {
	err := s.client.SignalWorkflow(ctx, WorkflowID(incidentID), "", name, Decision{Actor: actor})

	// Unlike an alert signal, a missing workflow here is meaningful: the caller
	// believed there was something to decide on, and there is not.
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return ErrNoRemediationRun
	}
	if err != nil {
		return fmt.Errorf("signal %q to remediation workflow for incident %s: %w", name, incidentID, err)
	}
	return nil
}
