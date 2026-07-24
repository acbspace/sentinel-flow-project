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

		query, args := buildIncidentListQuery(IncidentFilter{})

		if strings.Contains(query, "WHERE") {
			t.Errorf("unfiltered query should have no WHERE clause:\n%s", query)
		}
		if !strings.Contains(query, "ORDER BY last_seen_at DESC LIMIT $1 OFFSET $2") {
			t.Errorf("query missing expected ordering/pagination tail:\n%s", query)
		}
		wantArgs := []any{defaultListLimit, 0}
		assertArgs(t, args, wantArgs)
	})

	t.Run("filters bind in order with pagination last", func(t *testing.T) {
		t.Parallel()

		query, args := buildIncidentListQuery(IncidentFilter{
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
		assertArgs(t, args, []any{"open", "tenant-a", "payment-service", "error", 10, 20})
	})

	t.Run("oversized limit is clamped in the args", func(t *testing.T) {
		t.Parallel()

		_, args := buildIncidentListQuery(IncidentFilter{Limit: 10_000})
		assertArgs(t, args, []any{maxListLimit, 0})
	})
}

func TestBuildEventListQuery(t *testing.T) {
	t.Parallel()

	since := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	until := time.Date(2026, 7, 24, 11, 0, 0, 0, time.UTC)

	query, args := buildEventListQuery(EventFilter{
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
	if !strings.Contains(query, "ORDER BY event_timestamp DESC LIMIT $6 OFFSET $7") {
		t.Errorf("query missing expected ordering/pagination tail:\n%s", query)
	}
	assertArgs(t, args, []any{"payment-service", "error", "abc123", since, until, 5, 0})
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
