package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acbspace/sentinel-flow-project/internal/obs"
)

// PartitionPrefix and DefaultPartitionName are the naming contract between this
// package, migration 0006 and the janitor. A daily partition is
// telemetry_events_YYYYMMDD; anything else attached to the parent is left alone.
const (
	PartitionPrefix      = "telemetry_events_"
	PartitionDateLayout  = "20060102"
	DefaultPartitionName = "telemetry_events_default"
)

// PartitionName returns the partition that holds the given UTC day.
func PartitionName(day time.Time) string {
	return PartitionPrefix + day.UTC().Format(PartitionDateLayout)
}

// PartitionDay parses a partition name back into its UTC day, reporting false
// for anything that is not a daily partition — the default partition above all,
// which must never be treated as expirable.
func PartitionDay(name string) (time.Time, bool) {
	if len(name) != len(PartitionPrefix)+len(PartitionDateLayout) {
		return time.Time{}, false
	}
	if name[:len(PartitionPrefix)] != PartitionPrefix {
		return time.Time{}, false
	}
	day, err := time.ParseInLocation(PartitionDateLayout, name[len(PartitionPrefix):], time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return day, true
}

const listPartitionsSQL = `
SELECT child.relname
FROM pg_inherits
JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
JOIN pg_class child  ON child.oid  = pg_inherits.inhrelid
WHERE parent.relname = 'telemetry_events'
ORDER BY child.relname`

const defaultPartitionRowsSQL = `SELECT count(*) FROM ` + DefaultPartitionName

// PartitionStore manages the daily partitions of telemetry_events.
type PartitionStore struct {
	pool    *pgxpool.Pool
	metrics *obs.DBMetrics
	timeout time.Duration
}

// NewPartitionStore builds a partition store over an existing pool.
func NewPartitionStore(pool *pgxpool.Pool, metrics *obs.DBMetrics, timeout time.Duration) *PartitionStore {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &PartitionStore{pool: pool, metrics: metrics, timeout: timeout}
}

// ListPartitions returns the names of every partition attached to
// telemetry_events, including the default one.
func (s *PartitionStore) ListPartitions(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	rows, err := s.pool.Query(ctx, listPartitionsSQL)
	if err != nil {
		s.metrics.Record(ctx, "list_partitions", "error", time.Since(start))
		return nil, fmt.Errorf("list partitions: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			s.metrics.Record(ctx, "list_partitions", "error", time.Since(start))
			return nil, fmt.Errorf("scan partition name: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		s.metrics.Record(ctx, "list_partitions", "error", time.Since(start))
		return nil, fmt.Errorf("list partitions: %w", err)
	}

	s.metrics.Record(ctx, "list_partitions", "ok", time.Since(start))
	return names, nil
}

// CreatePartition attaches the daily partition covering day.
//
// The straightforward CREATE fails if the default partition already holds rows
// that would belong to the new one — PostgreSQL will not let a partition come
// into existence that the default partition already contradicts. That is not an
// exotic case: it is exactly what happens after the janitor has been stopped for
// longer than its lookahead. So a conflict is handled rather than reported:
// the offending rows are moved out of the default partition, the partition is
// created, and they are inserted back through the parent so they route to it.
func (s *PartitionStore) CreatePartition(ctx context.Context, day time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	err := s.createPartition(ctx, day)
	elapsed := time.Since(start)

	if err != nil {
		s.metrics.Record(ctx, "create_partition", "error", elapsed)
		return err
	}
	s.metrics.Record(ctx, "create_partition", "ok", elapsed)
	return nil
}

func (s *PartitionStore) createPartition(ctx context.Context, day time.Time) error {
	if err := s.attach(ctx, day); err == nil {
		return nil
	} else if !isDefaultPartitionConflict(err) {
		return fmt.Errorf("create partition for %s: %w", day.Format(time.DateOnly), err)
	}

	// The failed statement aborted its transaction, so the drain runs in a fresh
	// one. Everything in it is a single unit: if any step fails, the rows stay in
	// the default partition rather than vanishing between two of them.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin partition drain for %s: %w", day.Format(time.DateOnly), err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	lower, upper := dayBounds(day)

	// Hold the default partition still for the whole move. Without this an insert
	// arriving mid-drain could land a row in the range that the CREATE is about
	// to claim, and the CREATE would fail again.
	if _, err := tx.Exec(ctx, `LOCK TABLE `+DefaultPartitionName+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		return fmt.Errorf("lock default partition: %w", err)
	}

	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE janitor_drain ON COMMIT DROP AS
        SELECT * FROM `+DefaultPartitionName+` WHERE event_timestamp >= $1 AND event_timestamp < $2`,
		lower, upper); err != nil {
		return fmt.Errorf("stage rows out of the default partition: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`DELETE FROM `+DefaultPartitionName+` WHERE event_timestamp >= $1 AND event_timestamp < $2`,
		lower, upper); err != nil {
		return fmt.Errorf("clear rows from the default partition: %w", err)
	}

	if _, err := tx.Exec(ctx, attachSQL(day)); err != nil {
		return fmt.Errorf("create partition for %s after drain: %w", day.Format(time.DateOnly), err)
	}

	// Back in through the parent, so they route to the partition just created.
	if _, err := tx.Exec(ctx, `INSERT INTO telemetry_events SELECT * FROM janitor_drain`); err != nil {
		return fmt.Errorf("reinsert drained rows for %s: %w", day.Format(time.DateOnly), err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit partition drain for %s: %w", day.Format(time.DateOnly), err)
	}
	return nil
}

func (s *PartitionStore) attach(ctx context.Context, day time.Time) error {
	_, err := s.pool.Exec(ctx, attachSQL(day))
	return err
}

// attachSQL builds the CREATE TABLE ... PARTITION OF statement.
//
// The bounds and the table name are interpolated rather than bound as
// parameters because DDL takes no parameters. That is safe here and only here:
// every part of the string is derived from a time.Time this package formatted
// itself, so none of it can carry anything a caller supplied.
func attachSQL(day time.Time) string {
	lower, upper := dayBounds(day)
	return fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s PARTITION OF telemetry_events FOR VALUES FROM ('%s') TO ('%s')`,
		PartitionName(day),
		lower.Format(time.RFC3339),
		upper.Format(time.RFC3339),
	)
}

func dayBounds(day time.Time) (time.Time, time.Time) {
	lower := day.UTC().Truncate(24 * time.Hour)
	return lower, lower.AddDate(0, 0, 1)
}

// DropPartition detaches and drops one partition, returning the disk to the
// filesystem immediately — which is the whole reason this table is partitioned
// rather than pruned with DELETE.
func (s *PartitionStore) DropPartition(ctx context.Context, name string) error {
	if _, ok := PartitionDay(name); !ok {
		// Refuse anything that is not a dated daily partition. The default
		// partition is the one this protects: dropping it would turn a late
		// arrival into an ingestion failure.
		return fmt.Errorf("refusing to drop %q: not a dated telemetry_events partition", name)
	}

	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	_, err := s.pool.Exec(ctx, `DROP TABLE IF EXISTS `+name)
	elapsed := time.Since(start)

	if err != nil {
		s.metrics.Record(ctx, "drop_partition", "error", elapsed)
		return fmt.Errorf("drop partition %s: %w", name, err)
	}

	s.metrics.Record(ctx, "drop_partition", "ok", elapsed)
	return nil
}

// DefaultPartitionRows counts what has landed in the default partition. Any
// non-zero answer means events arrived for a day nobody had created a partition
// for, so it is surfaced rather than left for someone to discover.
func (s *PartitionStore) DefaultPartitionRows(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	var count int64
	err := s.pool.QueryRow(ctx, defaultPartitionRowsSQL).Scan(&count)
	elapsed := time.Since(start)

	if err != nil {
		s.metrics.Record(ctx, "default_partition_rows", "error", elapsed)
		return 0, fmt.Errorf("count default partition rows: %w", err)
	}

	s.metrics.Record(ctx, "default_partition_rows", "ok", elapsed)
	return count, nil
}

// isDefaultPartitionConflict reports whether err is PostgreSQL refusing to
// create a partition because the default partition already holds rows for its
// range. From a CREATE TABLE ... PARTITION OF, a check violation can mean
// nothing else.
func isDefaultPartitionConflict(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "23514"
}
