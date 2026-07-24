package correlate

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Runner drives the evaluator on a fixed cadence until its context is cancelled.
type Runner struct {
	evaluator *Evaluator
	interval  time.Duration
	log       *slog.Logger
}

// NewRunner builds the correlation loop.
func NewRunner(evaluator *Evaluator, interval time.Duration, log *slog.Logger) *Runner {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	return &Runner{evaluator: evaluator, interval: interval, log: log}
}

// Run evaluates immediately, then once per interval, until ctx is cancelled.
//
// Unlike the consume loop, a failed cycle does not stop the runner or the
// process. Correlation is a best-effort read over data that is already durably
// stored: a transient database error during a cycle must not take down event
// ingestion, so it is logged loudly and retried on the next tick. The only way
// Run stops is context cancellation, which it treats as a clean shutdown.
func (r *Runner) Run(ctx context.Context) error {
	r.log.Info("correlation loop starting", slog.Duration("interval", r.interval))

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	// Evaluate once up front so a freshly started engine does not wait a whole
	// interval before it can open its first incident.
	r.evaluate(ctx)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("correlation loop stopping", slog.String("reason", "context cancelled"))
			return nil
		case <-ticker.C:
			r.evaluate(ctx)
		}
	}
}

// evaluate runs one cycle and absorbs its error, unless the error is context
// cancellation, in which case the loop is shutting down and the next select will
// observe ctx.Done().
func (r *Runner) evaluate(ctx context.Context) {
	if err := r.evaluator.EvaluateOnce(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		r.log.Error("correlation cycle failed; will retry next tick",
			slog.String("error", err.Error()),
		)
	}
}
