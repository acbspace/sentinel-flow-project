package correlate

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

// WindowSource supplies the per-service event tallies a rule reasons over. It is
// an interface so the evaluator can be tested against a fake without a database;
// *store.EventStore satisfies it.
type WindowSource interface {
	WindowStats(ctx context.Context, since, countSince time.Time) ([]store.ServiceWindow, error)
}

// IncidentSink persists the incidents the evaluator opens and closes the ones it
// auto-resolves. *store.IncidentStore satisfies it.
type IncidentSink interface {
	UpsertOpen(ctx context.Context, inc incident.Incident, groupedIncrement int64) (bool, error)
	AutoResolveStale(ctx context.Context, olderThan time.Time) (int64, error)
}

// Evaluator runs one correlation cycle: pull windows, apply rules, open or group
// incidents, then auto-resolve the quiet ones.
type Evaluator struct {
	source       WindowSource
	sink         IncidentSink
	rules        []Rule
	metrics      *obs.CorrelationMetrics
	log          *slog.Logger
	resolveAfter time.Duration

	// lastCycleAt is when the previous successful cycle ran. It bounds the slice
	// of each window whose events have not been counted into an incident yet, so
	// overlapping windows do not count the same event twice. It is zero until the
	// first cycle completes, which makes that cycle count its whole window — the
	// right amount for an incident it is about to open.
	//
	// Only Runner calls EvaluateOnce, from a single goroutine, so this needs no
	// synchronisation. It is per-process state, which is sound because the
	// advisory lock keeps one replica evaluating for as long as it holds the lock.
	lastCycleAt time.Time

	// now and newID are injected so tests are deterministic: a fixed clock makes
	// the window bounds and resolve cutoff exact, and a scripted id generator
	// makes opened incidents comparable.
	now   func() time.Time
	newID func() string
}

// EvaluatorOptions configures an Evaluator.
type EvaluatorOptions struct {
	Source       WindowSource
	Sink         IncidentSink
	Rules        []Rule
	Metrics      *obs.CorrelationMetrics
	Logger       *slog.Logger
	ResolveAfter time.Duration
	Now          func() time.Time
	NewID        func() string
}

// NewEvaluator builds an Evaluator, filling in the injectable seams with real
// implementations when the caller leaves them nil.
func NewEvaluator(opts EvaluatorOptions) *Evaluator {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.NewID == nil {
		opts.NewID = uuid.NewString
	}
	if opts.ResolveAfter <= 0 {
		opts.ResolveAfter = 5 * time.Minute
	}
	return &Evaluator{
		source:       opts.Source,
		sink:         opts.Sink,
		rules:        opts.Rules,
		metrics:      opts.Metrics,
		log:          opts.Logger,
		resolveAfter: opts.ResolveAfter,
		now:          opts.Now,
		newID:        opts.NewID,
	}
}

// EvaluateOnce runs a single correlation cycle.
//
// Rules are grouped by window so each distinct lookback is queried once. Every
// firing detection is upserted, which either opens a new incident or groups into
// the active one sharing its fingerprint. Finally, incidents whose most recent
// detection predates the resolve cutoff are auto-resolved.
func (e *Evaluator) EvaluateOnce(ctx context.Context) error {
	start := e.now()

	byWindow := make(map[time.Duration][]Rule)
	for _, r := range e.rules {
		byWindow[r.Window] = append(byWindow[r.Window], r)
	}

	var opened, grouped int
	for window, rules := range byWindow {
		since := start.Add(-window)

		// Count only what the previous cycle did not, but never reach back past
		// the window itself: a first cycle, or one following a gap longer than
		// the window, has no earlier events left to count.
		countSince := since
		if e.lastCycleAt.After(countSince) {
			countSince = e.lastCycleAt
		}

		windows, err := e.source.WindowStats(ctx, since, countSince)
		if err != nil {
			e.metrics.RecordEvaluation(ctx, "error", e.now().Sub(start))
			return fmt.Errorf("read window stats for %s lookback: %w", window, err)
		}

		for _, w := range windows {
			for _, r := range rules {
				det := r.Evaluate(w)
				if !det.Fires {
					continue
				}

				inc := e.buildIncident(r, w, det)
				isNew, err := e.sink.UpsertOpen(ctx, inc, det.NewEventCount)
				if err != nil {
					e.metrics.RecordEvaluation(ctx, "error", e.now().Sub(start))
					return fmt.Errorf("open incident for rule %s on %s/%s: %w",
						r.ID, w.TenantID, w.ServiceName, err)
				}

				if isNew {
					opened++
					e.log.InfoContext(ctx, "incident opened",
						slog.String("rule_id", r.ID),
						slog.String("tenant_id", w.TenantID),
						slog.String("service_name", w.ServiceName),
						slog.String("severity", string(r.IncidentSeverity)),
						slog.String("title", det.Title),
					)
				} else {
					grouped++
					e.log.DebugContext(ctx, "detection grouped into open incident",
						slog.String("rule_id", r.ID),
						slog.String("tenant_id", w.TenantID),
						slog.String("service_name", w.ServiceName),
					)
				}
			}
		}
	}

	resolved, err := e.sink.AutoResolveStale(ctx, e.now().Add(-e.resolveAfter))
	if err != nil {
		e.metrics.RecordEvaluation(ctx, "error", e.now().Sub(start))
		return fmt.Errorf("auto-resolve stale incidents: %w", err)
	}
	if resolved > 0 {
		e.log.InfoContext(ctx, "incidents auto-resolved after going quiet",
			slog.Int64("resolved", resolved),
			slog.Duration("quiet_period", e.resolveAfter),
		)
	}

	// Advanced only here, so a cycle that failed part-way leaves the bound where
	// it was and the next cycle re-counts that slice rather than skipping it.
	e.lastCycleAt = start

	e.metrics.RecordEvaluation(ctx, "ok", e.now().Sub(start))
	e.metrics.RecordIncidents(ctx, opened, grouped, int(resolved))
	return nil
}

// buildIncident turns a firing detection into the incident to upsert. FirstSeenAt
// and LastSeenAt are both set to now; on a grouping upsert the store keeps the
// original FirstSeenAt and only advances LastSeenAt.
func (e *Evaluator) buildIncident(r Rule, w store.ServiceWindow, det Detection) incident.Incident {
	now := e.now()
	return incident.Incident{
		ID:          e.newID(),
		Fingerprint: incident.Fingerprint(r.ID, w.TenantID, w.ServiceName),
		TenantID:    w.TenantID,
		ServiceName: w.ServiceName,
		RuleID:      r.ID,
		Title:       det.Title,
		Severity:    r.IncidentSeverity,
		Status:      incident.StatusOpen,
		EventCount:  det.EventCount,
		FirstSeenAt: now,
		LastSeenAt:  now,
		Details:     det.Details,
	}
}
