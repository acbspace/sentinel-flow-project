package janitor

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Runner drives the janitor on a fixed cadence until its context is cancelled.
type Runner struct {
	janitor  *Janitor
	interval time.Duration
	log      *slog.Logger
}

// NewRunner builds the maintenance loop.
func NewRunner(j *Janitor, interval time.Duration, log *slog.Logger) *Runner {
	if interval <= 0 {
		interval = time.Hour
	}
	return &Runner{janitor: j, interval: interval, log: log}
}

// Run maintains immediately, then once per interval, until ctx is cancelled.
//
// Like the correlation loop, a failed cycle is logged and retried rather than
// fatal. Partition maintenance is ahead-of-time work: the lookahead exists
// precisely so that several consecutive failures are survivable, and taking the
// service down on the first one would remove the margin instead of using it.
func (r *Runner) Run(ctx context.Context) error {
	r.log.Info("janitor loop starting",
		slog.Duration("interval", r.interval),
		slog.Duration("lookahead", r.janitor.lookahead),
		slog.Duration("retention", r.janitor.retention),
	)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.maintain(ctx)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("janitor loop stopping", slog.String("reason", "context cancelled"))
			return nil
		case <-ticker.C:
			r.maintain(ctx)
		}
	}
}

func (r *Runner) maintain(ctx context.Context) {
	result, err := r.janitor.RunOnce(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		r.log.Error("janitor cycle failed; will retry next tick", slog.String("error", err.Error()))
		return
	}

	r.log.Debug("janitor cycle complete",
		slog.Int("created", len(result.Created)),
		slog.Int("dropped", len(result.Dropped)),
		slog.Int64("default_partition_rows", result.DefaultRows),
	)
}
