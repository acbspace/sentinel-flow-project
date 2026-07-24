package remediate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
	"github.com/acbspace/sentinel-flow-project/internal/runbook"
)

// IncidentSource supplies incidents needing a remediation run and marks them once
// one has been started. *store.IncidentStore satisfies it.
type IncidentSource interface {
	ListOpenUnremediated(ctx context.Context, limit int) ([]incident.Incident, error)
	MarkRemediated(ctx context.Context, id string) error
}

// WorkflowStarter launches a remediation workflow. TemporalStarter is the
// production implementation.
type WorkflowStarter interface {
	StartRemediationWorkflow(ctx context.Context, in IncidentRemediationInput) error
}

// Starter polls for newly-opened incidents and starts one remediation run each,
// for those a runbook actually covers.
type Starter struct {
	incidents IncidentSource
	workflows WorkflowStarter
	catalog   runbook.Catalog
	metrics   *obs.RemediationMetrics
	log       *slog.Logger
	interval  time.Duration
	batchSize int
}

// StarterOptions configures a Starter.
type StarterOptions struct {
	Incidents IncidentSource
	Workflows WorkflowStarter
	Catalog   runbook.Catalog
	Metrics   *obs.RemediationMetrics
	Logger    *slog.Logger
	Interval  time.Duration
	BatchSize int
}

// NewStarter builds the remediation starter.
func NewStarter(opts StarterOptions) *Starter {
	if opts.Interval <= 0 {
		opts.Interval = 10 * time.Second
	}
	if opts.BatchSize < 1 {
		opts.BatchSize = 50
	}
	return &Starter{
		incidents: opts.Incidents,
		workflows: opts.Workflows,
		catalog:   opts.Catalog,
		metrics:   opts.Metrics,
		log:       opts.Logger,
		interval:  opts.Interval,
		batchSize: opts.BatchSize,
	}
}

// Run polls immediately, then once per interval, until ctx is cancelled. A failed
// cycle is logged and retried rather than crashing the worker.
func (s *Starter) Run(ctx context.Context) error {
	s.log.Info("remediation starter loop starting",
		slog.Duration("interval", s.interval),
		slog.Int("runbooks", len(s.catalog.Runbooks)),
	)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			s.log.Info("remediation starter loop stopping", slog.String("reason", "context cancelled"))
			return nil
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Starter) tick(ctx context.Context) {
	if err := s.StartPending(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		s.log.Error("remediation starter cycle failed; will retry next tick", slog.String("error", err.Error()))
	}
}

// StartPending starts a remediation run for each open incident a runbook covers.
//
// An incident with no matching runbook is marked anyway: there is nothing to
// automate, and leaving it unmarked would mean re-examining it forever.
func (s *Starter) StartPending(ctx context.Context) error {
	incidents, err := s.incidents.ListOpenUnremediated(ctx, s.batchSize)
	if err != nil {
		return fmt.Errorf("list open unremediated incidents: %w", err)
	}

	started := 0
	for _, inc := range incidents {
		rb, ok := s.catalog.Find(inc.RuleID, inc.ServiceName, string(inc.Severity))
		if !ok {
			s.log.DebugContext(ctx, "no runbook matches incident; nothing to automate",
				slog.String("incident_id", inc.ID),
				slog.String("rule_id", inc.RuleID),
				slog.String("service_name", inc.ServiceName),
			)
			s.markRemediated(ctx, inc.ID)
			continue
		}

		in := IncidentRemediationInput{
			Incident: IncidentRef{
				ID:          inc.ID,
				TenantID:    inc.TenantID,
				ServiceName: inc.ServiceName,
				Severity:    string(inc.Severity),
				Title:       inc.Title,
			},
			Runbook: rb,
		}

		if err := s.workflows.StartRemediationWorkflow(ctx, in); err != nil {
			s.log.ErrorContext(ctx, "start remediation workflow failed",
				slog.String("incident_id", inc.ID),
				slog.String("error", err.Error()),
			)
			continue
		}

		s.markRemediated(ctx, inc.ID)
		started++
		s.log.InfoContext(ctx, "remediation workflow started",
			slog.String("incident_id", inc.ID),
			slog.String("runbook_id", rb.ID),
			slog.String("service_name", inc.ServiceName),
		)
	}

	s.metrics.RecordStarted(ctx, started)
	return nil
}

// markRemediated is best-effort: starting is idempotent, so a missed mark costs
// one harmlessly-rejected start next tick, not a second remediation run.
func (s *Starter) markRemediated(ctx context.Context, id string) {
	if err := s.incidents.MarkRemediated(ctx, id); err != nil {
		s.log.WarnContext(ctx, "mark incident remediated failed",
			slog.String("incident_id", id),
			slog.String("error", err.Error()),
		)
	}
}
