package alerting

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

// TemporalStarter starts alert workflows through a Temporal client. It is the
// production WorkflowStarter; the poller's tests use a fake instead.
type TemporalStarter struct {
	client    client.Client
	taskQueue string
}

// NewTemporalStarter builds a TemporalStarter over a connected client.
func NewTemporalStarter(c client.Client, taskQueue string) *TemporalStarter {
	return &TemporalStarter{client: c, taskQueue: taskQueue}
}

// StartAlertWorkflow starts the alert workflow for an incident, idempotently.
//
// The workflow id is the incident id and the reuse policy rejects duplicates, so
// a workflow that already exists — running or completed — surfaces as an
// "already started" error, which is treated as success. Exactly one alert
// workflow ever runs per incident.
func (s *TemporalStarter) StartAlertWorkflow(ctx context.Context, in IncidentAlertInput) error {
	opts := client.StartWorkflowOptions{
		ID:                    WorkflowID(in.Incident.ID),
		TaskQueue:             s.taskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}

	_, err := s.client.ExecuteWorkflow(ctx, opts, IncidentAlertWorkflow, in)
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &alreadyStarted) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("execute alert workflow for incident %s: %w", in.Incident.ID, err)
	}
	return nil
}
