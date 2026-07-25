package store

import (
	"testing"
	"time"
)

func TestPartitionNameAndDayRoundTrip(t *testing.T) {
	for _, want := range []time.Time{
		time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
		time.Date(2028, 2, 29, 0, 0, 0, 0, time.UTC),
	} {
		name := PartitionName(want)
		got, ok := PartitionDay(name)
		if !ok {
			t.Fatalf("PartitionDay(%q) reported not-a-partition", name)
		}
		if !got.Equal(want) {
			t.Errorf("round trip of %s gave %s via %q", want, got, name)
		}
	}
}

func TestPartitionNameUsesUTCDay(t *testing.T) {
	// A timestamp late on the 25th in UTC is already the 26th in some zones. The
	// name must follow the partition bounds, which are UTC, not the local clock.
	late := time.Date(2026, 7, 25, 23, 59, 59, 0, time.UTC)
	if got := PartitionName(late); got != "telemetry_events_20260725" {
		t.Errorf("PartitionName(%s) = %q, want telemetry_events_20260725", late, got)
	}

	east := time.FixedZone("UTC+9", 9*3600)
	sameInstant := late.In(east)
	if got := PartitionName(sameInstant); got != "telemetry_events_20260725" {
		t.Errorf("PartitionName(%s) = %q, want telemetry_events_20260725 — the same instant must name the same partition", sameInstant, got)
	}
}

func TestPartitionDayRejectsNonPartitions(t *testing.T) {
	// The default partition is the one that matters here: the janitor decides
	// what to drop from this function's answer, and dropping the default
	// partition would turn a late arrival into an ingestion failure.
	for _, name := range []string{
		DefaultPartitionName,
		"telemetry_events",
		"telemetry_events_",
		"telemetry_events_2026072",
		"telemetry_events_202607255",
		"telemetry_events_20261301",
		"telemetry_events_notadate",
		"incidents_20260725",
		"",
	} {
		if day, ok := PartitionDay(name); ok {
			t.Errorf("PartitionDay(%q) = %s, true; want false", name, day)
		}
	}
}

func TestAttachSQLBoundsAreUTCMidnightToMidnight(t *testing.T) {
	got := attachSQL(time.Date(2026, 7, 25, 14, 22, 3, 0, time.UTC))

	want := `CREATE TABLE IF NOT EXISTS telemetry_events_20260725 PARTITION OF telemetry_events ` +
		`FOR VALUES FROM ('2026-07-25T00:00:00Z') TO ('2026-07-26T00:00:00Z')`
	if got != want {
		t.Errorf("attachSQL:\n got %s\nwant %s", got, want)
	}
}

func TestDayBoundsAreExactlyOneDayApart(t *testing.T) {
	lower, upper := dayBounds(time.Date(2026, 7, 25, 6, 0, 0, 0, time.UTC))

	if !lower.Equal(time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("lower = %s, want 2026-07-25T00:00:00Z", lower)
	}
	if !upper.Equal(time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("upper = %s, want 2026-07-26T00:00:00Z", upper)
	}

	// Adjacent partitions must abut exactly: a gap loses events to the default
	// partition and an overlap is rejected outright by PostgreSQL.
	_, prevUpper := dayBounds(time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC))
	if !prevUpper.Equal(lower) {
		t.Errorf("previous day ends at %s but this one starts at %s", prevUpper, lower)
	}
}
