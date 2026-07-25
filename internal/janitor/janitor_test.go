package janitor_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/janitor"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// day is a fixed "now" so every lookahead and retention boundary is exact.
var day = time.Date(2026, 7, 25, 9, 30, 0, 0, time.UTC)

// fakeMaintainer stands in for the partition catalogue, recording what the
// janitor asked it to do.
type fakeMaintainer struct {
	existing    []string
	created     []time.Time
	dropped     []string
	defaultRows int64

	createErr error
	dropErr   error
	listErr   error
}

func (f *fakeMaintainer) ListPartitions(context.Context) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]string(nil), f.existing...), nil
}

func (f *fakeMaintainer) CreatePartition(_ context.Context, d time.Time) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.created = append(f.created, d)
	f.existing = append(f.existing, store.PartitionName(d))
	return nil
}

func (f *fakeMaintainer) DropPartition(_ context.Context, name string) error {
	if f.dropErr != nil {
		return f.dropErr
	}
	f.dropped = append(f.dropped, name)
	return nil
}

func (f *fakeMaintainer) DefaultPartitionRows(context.Context) (int64, error) {
	return f.defaultRows, nil
}

func newJanitor(m janitor.Maintainer, lookahead, retention time.Duration) *janitor.Janitor {
	return janitor.New(janitor.Options{
		Partitions: m,
		DayOf:      store.PartitionDay,
		NameOf:     store.PartitionName,
		Logger:     discardLogger(),
		Lookahead:  lookahead,
		Retention:  retention,
		Now:        func() time.Time { return day },
	})
}

func TestRunOnceCreatesTodayAndTheLookahead(t *testing.T) {
	m := &fakeMaintainer{existing: []string{store.DefaultPartitionName}}

	result, err := newJanitor(m, 3*24*time.Hour, 30*24*time.Hour).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	// Today plus three days ahead, inclusive of both ends.
	want := []string{
		"telemetry_events_20260725",
		"telemetry_events_20260726",
		"telemetry_events_20260727",
		"telemetry_events_20260728",
	}
	if len(result.Created) != len(want) {
		t.Fatalf("created %v, want %v", result.Created, want)
	}
	for i, name := range want {
		if result.Created[i] != name {
			t.Errorf("created[%d] = %s, want %s", i, result.Created[i], name)
		}
	}
}

func TestRunOnceSkipsPartitionsThatExist(t *testing.T) {
	m := &fakeMaintainer{existing: []string{
		store.DefaultPartitionName,
		"telemetry_events_20260725",
		"telemetry_events_20260726",
	}}

	result, err := newJanitor(m, 2*24*time.Hour, 30*24*time.Hour).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if len(result.Created) != 1 || result.Created[0] != "telemetry_events_20260727" {
		t.Errorf("created %v, want only telemetry_events_20260727", result.Created)
	}
}

func TestRunOnceIsIdempotent(t *testing.T) {
	m := &fakeMaintainer{existing: []string{store.DefaultPartitionName}}
	j := newJanitor(m, 2*24*time.Hour, 30*24*time.Hour)

	if _, err := j.RunOnce(context.Background()); err != nil {
		t.Fatalf("first RunOnce: %v", err)
	}
	second, err := j.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("second RunOnce: %v", err)
	}

	// The loop runs every hour; all but the first pass of a day must be no-ops.
	if len(second.Created) != 0 {
		t.Errorf("second cycle created %v, want nothing", second.Created)
	}
	if len(second.Dropped) != 0 {
		t.Errorf("second cycle dropped %v, want nothing", second.Dropped)
	}
}

func TestRunOnceDropsOnlyPartitionsPastRetention(t *testing.T) {
	// Retention is 7 days from 2026-07-25, so the cutoff is 2026-07-18. A
	// partition is expirable only once its newest possible row (the end of its
	// day) is older than that.
	m := &fakeMaintainer{existing: []string{
		store.DefaultPartitionName,
		"telemetry_events_20260710", // long past  -> drop
		"telemetry_events_20260716", // ends 07-17 -> drop
		"telemetry_events_20260717", // ends 07-18 -> keep, exactly at the edge
		"telemetry_events_20260720", // inside     -> keep
		"telemetry_events_20260725", // today      -> keep
	}}

	result, err := newJanitor(m, 24*time.Hour, 7*24*time.Hour).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	want := map[string]bool{"telemetry_events_20260710": true, "telemetry_events_20260716": true}
	if len(result.Dropped) != len(want) {
		t.Fatalf("dropped %v, want %v", result.Dropped, want)
	}
	for _, name := range result.Dropped {
		if !want[name] {
			t.Errorf("dropped %s, which is still inside the retention window", name)
		}
	}
}

func TestRunOnceNeverDropsTheDefaultPartition(t *testing.T) {
	m := &fakeMaintainer{existing: []string{
		store.DefaultPartitionName,
		"telemetry_events_not_a_date",
		"some_other_table",
	}}

	result, err := newJanitor(m, 24*time.Hour, 7*24*time.Hour).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	// Dropping the default partition would turn a late arrival into an ingestion
	// failure, so nothing unrecognised may ever be dropped.
	if len(result.Dropped) != 0 {
		t.Errorf("dropped %v, want nothing — none of those are dated partitions", result.Dropped)
	}
}

func TestRunOnceReportsDefaultPartitionOccupancy(t *testing.T) {
	m := &fakeMaintainer{existing: []string{store.DefaultPartitionName}, defaultRows: 42}

	result, err := newJanitor(m, 24*time.Hour, 7*24*time.Hour).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if result.DefaultRows != 42 {
		t.Errorf("DefaultRows = %d, want 42", result.DefaultRows)
	}
}

func TestRunOnceCreatesBeforeItDrops(t *testing.T) {
	// If a cycle fails partway, having tomorrow's partition and too much history
	// is survivable; having neither is not. So a create failure must abort before
	// anything is destroyed.
	m := &fakeMaintainer{
		existing:  []string{store.DefaultPartitionName, "telemetry_events_20260101"},
		createErr: errors.New("disk full"),
	}

	if _, err := newJanitor(m, 24*time.Hour, 7*24*time.Hour).RunOnce(context.Background()); err == nil {
		t.Fatal("RunOnce() = nil, want the create error")
	}
	if len(m.dropped) != 0 {
		t.Errorf("dropped %v after a failed create; nothing may be destroyed once a cycle is failing", m.dropped)
	}
}

func TestRunOnceSurfacesListFailure(t *testing.T) {
	m := &fakeMaintainer{listErr: errors.New("connection refused")}

	if _, err := newJanitor(m, 24*time.Hour, 7*24*time.Hour).RunOnce(context.Background()); err == nil {
		t.Fatal("RunOnce() = nil, want the list error")
	}
	if len(m.created) != 0 || len(m.dropped) != 0 {
		t.Error("acted on an unknown catalogue; a failed list must stop the cycle")
	}
}
