package bench

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Report renders results as the Markdown table the README quotes, so a run's
// output can be pasted into the documentation without being retyped — which is
// how a benchmark table drifts away from the code that produced it.
func Report(results []Result) string {
	var b strings.Builder

	b.WriteString("| Path | Work per request | req/s | avg | p50 | p95 | p99 |\n")
	b.WriteString("|---|---|---:|---:|---:|---:|---:|\n")

	for _, r := range results {
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s | %s | %s | %s |\n",
			r.Name,
			r.Work,
			formatCount(r.RequestsPerSecond()),
			formatDuration(r.Average()),
			formatDuration(r.Percentile(0.50)),
			formatDuration(r.Percentile(0.95)),
			formatDuration(r.Percentile(0.99)),
		)
	}

	return b.String()
}

// Summary renders the per-run detail that the table deliberately leaves out:
// how many requests each row is based on, and whether any of them failed. A
// percentile is only meaningful alongside its sample size.
func Summary(results []Result) string {
	var b strings.Builder

	for _, r := range results {
		fmt.Fprintf(&b, "%s: %d requests in %s", r.Name, r.Requests(), r.Elapsed.Round(time.Millisecond))

		if r.Clean() {
			b.WriteString(", all 2xx\n")
			continue
		}

		// Anything less than a clean sweep is stated loudly: a run that was
		// mostly rejected still produces beautiful percentiles.
		b.WriteString(", NOT CLEAN — ")
		if r.Failures > 0 {
			fmt.Fprintf(&b, "%d transport failures; ", r.Failures)
		}
		fmt.Fprintf(&b, "statuses: %s\n", formatStatuses(r.Statuses))
	}

	return b.String()
}

// formatStatuses renders the status distribution in ascending code order, so
// two runs of the same target produce comparable text.
func formatStatuses(statuses map[int]int) string {
	if len(statuses) == 0 {
		return "none"
	}

	codes := make([]int, 0, len(statuses))
	for code := range statuses {
		codes = append(codes, code)
	}
	sort.Ints(codes)

	parts := make([]string, 0, len(codes))
	for _, code := range codes {
		parts = append(parts, fmt.Sprintf("%d×%d", statuses[code], code))
	}
	return strings.Join(parts, ", ")
}

// formatDuration renders a latency the way the README's table does: milliseconds
// to one decimal place, or microseconds when a value would round to 0.0 ms.
func formatDuration(d time.Duration) string {
	ms := float64(d) / float64(time.Millisecond)
	if ms < 0.1 && d > 0 {
		return fmt.Sprintf("%.0f µs", float64(d)/float64(time.Microsecond))
	}
	return fmt.Sprintf("%.1f ms", ms)
}

// formatCount renders a throughput figure with thousands separators.
func formatCount(v float64) string {
	digits := fmt.Sprintf("%.0f", v)

	var b strings.Builder
	for i, r := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	return b.String()
}
