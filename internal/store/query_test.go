package store

// These tests are in-package (white box) because the query builders are
// unexported: their whole job is the placeholder arithmetic and clause
// composition that a black-box test could not see. The DB round-trips that use
// them are covered by the integration test.

import (
	"strings"
	"testing"
	"time"
)

func TestNormalizeLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		limit int
		want  int
	}{
		{"zero falls back to default", 0, defaultListLimit},
		{"negative falls back to default", -5, defaultListLimit},
		{"in range is preserved", 25, 25},
		{"at ceiling is preserved", maxListLimit, maxListLimit},
		{"above ceiling is clamped", maxListLimit + 1, maxListLimit},
		{"absurd value is clamped", 1_000_000, maxListLimit},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := normalizeLimit(tc.limit); got != tc.want {
				t.Errorf("normalizeLimit(%d) = %d, want %d", tc.limit, got, tc.want)
			}
		})
	}
}

func TestBuildIncidentListQuery(t *testing.T) {
	t.Parallel()

	t.Run("no filter uses defaults and no WHERE", func(t *testing.T) {
		t.Parallel()

		query, args, limit := buildIncidentListQuery(IncidentFilter{})

		if strings.Contains(query, "WHERE") {
			t.Errorf("unfiltered query should have no WHERE clause:\n%s", query)
		}
		// No OFFSET at all when none was asked for, and one row more than the
		// page so the caller can tell whether another page exists.
		if !strings.Contains(query, "ORDER BY last_seen_at DESC, id DESC LIMIT $1") {
			t.Errorf("query missing expected ordering/pagination tail:\n%s", query)
		}
		if strings.Contains(query, "OFFSET") {
			t.Errorf("query should carry no OFFSET when none was requested:\n%s", query)
		}
		if limit != defaultListLimit {
			t.Errorf("limit = %d, want %d", limit, defaultListLimit)
		}
		assertArgs(t, args, []any{defaultListLimit + 1})
	})

	t.Run("filters bind in order with pagination last", func(t *testing.T) {
		t.Parallel()

		query, args, limit := buildIncidentListQuery(IncidentFilter{
			Status:      "open",
			TenantID:    "tenant-a",
			ServiceName: "payment-service",
			Severity:    "error",
			Limit:       10,
			Offset:      20,
		})

		want := "WHERE status = $1 AND tenant_id = $2 AND service_name = $3 AND severity = $4"
		if !strings.Contains(query, want) {
			t.Errorf("query missing expected WHERE clause %q:\n%s", want, query)
		}
		if !strings.Contains(query, "LIMIT $5 OFFSET $6") {
			t.Errorf("pagination placeholders should follow the filters:\n%s", query)
		}
		if limit != 10 {
			t.Errorf("limit = %d, want 10", limit)
		}
		assertArgs(t, args, []any{"open", "tenant-a", "payment-service", "error", 11, 20})
	})

	t.Run("cursor becomes a row-value comparison", func(t *testing.T) {
		t.Parallel()

		seen := time.Date(2026, 7, 25, 9, 0, 0, 0, time.UTC)
		query, args, _ := buildIncidentListQuery(IncidentFilter{
			Status: "open",
			Limit:  10,
			After:  Cursor{Time: seen, ID: "abc"},
		})

		// A row-value comparison, not two separate conditions: only the former
		// can be served by one index seek.
		want := "WHERE status = $1 AND (last_seen_at, id) < ($2, $3)"
		if !strings.Contains(query, want) {
			t.Errorf("query missing expected keyset clause %q:\n%s", want, query)
		}
		assertArgs(t, args, []any{"open", seen, "abc", 11})
	})

	t.Run("oversized limit is clamped in the args", func(t *testing.T) {
		t.Parallel()

		_, args, limit := buildIncidentListQuery(IncidentFilter{Limit: 10_000})
		if limit != maxListLimit {
			t.Errorf("limit = %d, want %d", limit, maxListLimit)
		}
		assertArgs(t, args, []any{maxListLimit + 1})
	})
}

func TestBuildEventListQuery(t *testing.T) {
	t.Parallel()

	since := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	until := time.Date(2026, 7, 24, 11, 0, 0, 0, time.UTC)

	query, args, limit := buildEventListQuery(EventFilter{
		ServiceName: "payment-service",
		Severity:    "error",
		TraceID:     "abc123",
		Since:       since,
		Until:       until,
		Limit:       5,
	})

	want := "WHERE service_name = $1 AND severity = $2 AND trace_id = $3 AND event_timestamp >= $4 AND event_timestamp <= $5"
	if !strings.Contains(query, want) {
		t.Errorf("query missing expected WHERE clause %q:\n%s", want, query)
	}
	if !strings.Contains(query, "ORDER BY event_timestamp DESC, event_id DESC LIMIT $6") {
		t.Errorf("query missing expected ordering/pagination tail:\n%s", query)
	}
	if limit != 5 {
		t.Errorf("limit = %d, want 5", limit)
	}
	assertArgs(t, args, []any{"payment-service", "error", "abc123", since, until, 6})
}

func TestBuildEventListQueryOrdersByTheKeysetColumns(t *testing.T) {
	t.Parallel()

	// The ordering has to match the cursor's columns exactly, or a page boundary
	// can skip or repeat a row. Both are (timestamp, id) descending, and the
	// partitioned table's primary key is exactly that pair.
	query, _, _ := buildEventListQuery(EventFilter{})

	if !strings.Contains(query, "ORDER BY event_timestamp DESC, event_id DESC") {
		t.Errorf("event ordering must be total over the cursor columns:\n%s", query)
	}
}

// assertArgs compares a builder's argument slice against the expected values,
// which doubles as a check that every placeholder has a bound argument.
func assertArgs(t *testing.T, got, want []any) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("arg count = %d, want %d (got %v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("arg[%d] = %#v, want %#v", i, got[i], want[i])
		}
	}
}
