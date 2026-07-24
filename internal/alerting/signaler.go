package alerting

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

// TemporalSignaler tells an incident's alert workflow that it has been
// acknowledged or resolved, so escalation stops promptly rather than waiting for
// the workflow's next database re-check. It is the fast path; the database
// remains the authority.
type TemporalSignaler struct {
	client client.Client
}

// NewTemporalSignaler builds a signaler over a connected client.
func NewTemporalSignaler(c client.Client) *TemporalSignaler {
	return &TemporalSignaler{client: c}
}

// SignalAcknowledged signals the incident's alert workflow that it was acknowledged.
func (s *TemporalSignaler) SignalAcknowledged(ctx context.Context, incidentID string) error {
	return s.signal(ctx, incidentID, SignalAcknowledge)
}

// SignalResolved signals the incident's alert workflow that it was resolved.
func (s *TemporalSignaler) SignalResolved(ctx context.Context, incidentID string) error {
	return s.signal(ctx, incidentID, SignalResolve)
}

func (s *TemporalSignaler) signal(ctx context.Context, incidentID, name string) error {
	err := s.client.SignalWorkflow(ctx, WorkflowID(incidentID), "", name, nil)

	// A missing workflow means the alert already ended (resolved or escalation
	// exhausted). That is not an error: there is simply nothing left to stop.
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("signal %q to alert workflow for incident %s: %w", name, incidentID, err)
	}
	return nil
}
