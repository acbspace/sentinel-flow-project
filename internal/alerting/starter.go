package alerting

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
	"github.com/acbspace/sentinel-flow-project/internal/oncall"
)

// IncidentSource supplies the incidents needing an alert workflow and marks them
// once one is started. *store.IncidentStore satisfies it.
type IncidentSource interface {
	ListOpenUnalerted(ctx context.Context, limit int) ([]incident.Incident, error)
	MarkAlerted(ctx context.Context, id string) error
}

// WorkflowStarter launches an alert workflow. The Temporal-backed implementation
// is TemporalStarter; the interface keeps the poller testable without a server.
type WorkflowStarter interface {
	StartAlertWorkflow(ctx context.Context, in IncidentAlertInput) error
}

// Starter polls for newly-opened incidents and starts one alert workflow each.
//
// It is the same shape as the correlation loop: a ticker over a database query,
// running in the service's errgroup. Because the workflow id is the incident id
// and starting is idempotent, and because it marks each incident afterwards, the
// starter is safe to run repeatedly and safe to restart.
type Starter struct {
	incidents IncidentSource
	workflows WorkflowStarter
	policy    oncall.EscalationPolicy
	metrics   *obs.AlertingMetrics
	log       *slog.Logger
	interval  time.Duration
	batchSize int
}

// StarterOptions configures a Starter.
type StarterOptions struct {
	Incidents IncidentSource
	Workflows WorkflowStarter
	Policy    oncall.EscalationPolicy
	Metrics   *obs.AlertingMetrics
	Logger    *slog.Logger
	Interval  time.Duration
	BatchSize int
}

// NewStarter builds the alert starter.
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
		policy:    opts.Policy,
		metrics:   opts.Metrics,
		log:       opts.Logger,
		interval:  opts.Interval,
		batchSize: opts.BatchSize,
	}
}

// Run polls immediately, then once per interval, until ctx is cancelled. Like
// the correlation loop it logs-and-continues on failure: alerting is best-effort
// over already-durable incidents and must not crash the worker.
func (s *Starter) Run(ctx context.Context) error {
	s.log.Info("alert starter loop starting", slog.Duration("interval", s.interval))

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			s.log.Info("alert starter loop stopping", slog.String("reason", "context cancelled"))
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
		s.log.Error("alert starter cycle failed; will retry next tick", slog.String("error", err.Error()))
	}
}

// StartPending starts an alert workflow for each open, not-yet-alerted incident.
//
// A single incident that fails to start is logged and skipped, not fatal: it
// stays unalerted (alerted_at is left NULL) and is retried next tick. Marking is
// best-effort for the same reason — starting is idempotent, so a missed mark just
// means one more (rejected) start attempt later.
func (s *Starter) StartPending(ctx context.Context) error {
	incidents, err := s.incidents.ListOpenUnalerted(ctx, s.batchSize)
	if err != nil {
		return fmt.Errorf("list open unalerted incidents: %w", err)
	}

	started := 0
	for _, inc := range incidents {
		in := IncidentAlertInput{
			Incident: IncidentRef{
				ID:          inc.ID,
				TenantID:    inc.TenantID,
				ServiceName: inc.ServiceName,
				Severity:    string(inc.Severity),
				Title:       inc.Title,
			},
			Policy: s.policy,
		}

		if err := s.workflows.StartAlertWorkflow(ctx, in); err != nil {
			s.log.ErrorContext(ctx, "start alert workflow failed",
				slog.String("incident_id", inc.ID),
				slog.String("error", err.Error()),
			)
			continue
		}

		if err := s.incidents.MarkAlerted(ctx, inc.ID); err != nil {
			s.log.WarnContext(ctx, "mark incident alerted failed",
				slog.String("incident_id", inc.ID),
				slog.String("error", err.Error()),
			)
		}

		started++
		s.log.InfoContext(ctx, "alert workflow started",
			slog.String("incident_id", inc.ID),
			slog.String("service_name", inc.ServiceName),
			slog.String("severity", string(inc.Severity)),
		)
	}

	s.metrics.RecordStarted(ctx, started)
	return nil
}
