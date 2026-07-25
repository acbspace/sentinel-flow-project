// Package janitor keeps the telemetry_events partitions healthy: it creates the
// days that are about to be written to, drops the days that have aged out, and
// reports anything that landed in the default partition.
//
// It exists as its own service rather than as another loop inside the engine for
// the same reason remediation is separate from alerting: it is the only
// component in the system that destroys data, and that deserves its own blast
// radius, its own deployment cadence, and the ability to be scaled to zero
// without stopping ingestion.
package janitor

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Maintainer is the partition catalogue the janitor operates on.
// *store.PartitionStore satisfies it; the interface keeps the schedule logic
// testable without a database.
type Maintainer interface {
	ListPartitions(ctx context.Context) ([]string, error)
	CreatePartition(ctx context.Context, day time.Time) error
	DropPartition(ctx context.Context, name string) error
	DefaultPartitionRows(ctx context.Context) (int64, error)
}

// DayParser turns a partition name into the day it covers, reporting false for
// names that are not dated daily partitions. store.PartitionDay satisfies it.
type DayParser func(name string) (time.Time, bool)

// NameBuilder returns the partition name for a day. store.PartitionName
// satisfies it.
type NameBuilder func(day time.Time) string

// Janitor runs one maintenance cycle: ensure, expire, report.
type Janitor struct {
	partitions Maintainer
	dayOf      DayParser
	nameOf     NameBuilder
	log        *slog.Logger

	lookahead time.Duration
	retention time.Duration

	now func() time.Time
}

// Options configures a Janitor.
type Options struct {
	Partitions Maintainer
	DayOf      DayParser
	NameOf     NameBuilder
	Logger     *slog.Logger

	// Lookahead is how far ahead partitions are created. It must comfortably
	// exceed both the cycle interval and any plausible outage of this service,
	// because a day with no partition sends its events to the default one.
	Lookahead time.Duration

	// Retention is how long a day's events are kept. A partition is dropped only
	// once every timestamp it could hold is older than this.
	Retention time.Duration

	Now func() time.Time
}

// New builds a Janitor.
func New(opts Options) *Janitor {
	if opts.Lookahead <= 0 {
		opts.Lookahead = 7 * 24 * time.Hour
	}
	if opts.Retention <= 0 {
		opts.Retention = 30 * 24 * time.Hour
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Janitor{
		partitions: opts.Partitions,
		dayOf:      opts.DayOf,
		nameOf:     opts.NameOf,
		log:        opts.Logger,
		lookahead:  opts.Lookahead,
		retention:  opts.Retention,
		now:        opts.Now,
	}
}

// Result reports what one cycle did.
type Result struct {
	Created     []string
	Dropped     []string
	DefaultRows int64
}

// RunOnce performs a full maintenance cycle.
//
// Creation happens before expiry on purpose. If both fail halfway, the system
// that has too many partitions still works and the one missing tomorrow's does
// not, so the cheap safety margin is bought first.
func (j *Janitor) RunOnce(ctx context.Context) (Result, error) {
	var result Result

	existing, err := j.partitions.ListPartitions(ctx)
	if err != nil {
		return result, fmt.Errorf("list partitions: %w", err)
	}

	present := make(map[string]bool, len(existing))
	for _, name := range existing {
		present[name] = true
	}

	today := j.today()
	for day := today; !day.After(today.Add(j.lookahead)); day = day.AddDate(0, 0, 1) {
		name := j.nameOf(day)
		if present[name] {
			continue
		}
		if err := j.partitions.CreatePartition(ctx, day); err != nil {
			return result, fmt.Errorf("create partition for %s: %w", day.Format(time.DateOnly), err)
		}
		result.Created = append(result.Created, name)
		j.log.InfoContext(ctx, "partition created",
			slog.String("partition", name),
			slog.String("day", day.Format(time.DateOnly)),
		)
	}

	// A partition is expirable only when its *newest* possible row is older than
	// the retention horizon, which is the day after the one it covers. Comparing
	// the day itself would delete a partition still holding data inside the
	// retention window.
	cutoff := today.Add(-j.retention)
	for _, name := range existing {
		day, ok := j.dayOf(name)
		if !ok {
			// The default partition and anything else unrecognised: never dropped.
			continue
		}
		if !day.AddDate(0, 0, 1).Before(cutoff) {
			continue
		}
		if err := j.partitions.DropPartition(ctx, name); err != nil {
			return result, fmt.Errorf("drop partition %s: %w", name, err)
		}
		result.Dropped = append(result.Dropped, name)
		j.log.InfoContext(ctx, "partition dropped past retention",
			slog.String("partition", name),
			slog.String("day", day.Format(time.DateOnly)),
			slog.Duration("retention", j.retention),
		)
	}

	rows, err := j.partitions.DefaultPartitionRows(ctx)
	if err != nil {
		return result, fmt.Errorf("count default partition rows: %w", err)
	}
	result.DefaultRows = rows

	if rows > 0 {
		// Not fatal — the rows are safely stored and queryable — but it means a
		// day went unpartitioned, so it is reported at error level rather than
		// left to be noticed.
		j.log.ErrorContext(ctx, "events are sitting in the default partition; a day went uncreated",
			slog.Int64("rows", rows),
			slog.String("partition", "telemetry_events_default"),
		)
	}

	return result, nil
}

// today is the current UTC day at midnight, which is the granularity every
// partition boundary is expressed in.
func (j *Janitor) today() time.Time {
	return j.now().UTC().Truncate(24 * time.Hour)
}
