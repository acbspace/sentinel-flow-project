// Command loadgen measures SentinelFlow's HTTP paths and prints the result as
// the Markdown table the README quotes.
//
// It is a development tool rather than a service: it is not built into an image,
// has no manifest, and talks to a running stack from outside it.
//
// Usage:
//
//	loadgen                              measure every path for 10s at 50 workers
//	loadgen -targets=events -duration=30s  measure just the ingest path
//	loadgen -requests=5000               send an exact count instead of running for a time
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/bench"
	"github.com/acbspace/sentinel-flow-project/internal/event"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "loadgen: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, out *os.File) error {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)

	var (
		ingestionURL = fs.String("ingestion-url", "http://localhost:8080", "base URL of the ingestion API")
		incidentsURL = fs.String("incidents-url", "http://localhost:8084", "base URL of the incidents API")
		targets      = fs.String("targets", "all", "which paths to measure: all, health, incidents, events, batch (comma separated)")
		batchSize    = fs.Int("batch-size", 100, "events per request for the batch target")
		workers      = fs.Int("workers", 50, "concurrent in-flight requests")
		duration     = fs.Duration("duration", 10*time.Second, "how long to measure each path")
		requests     = fs.Int("requests", 0, "exact requests per path; overrides -duration when set")
		warmup       = fs.Int("warmup", 500, "requests to send and discard before measuring each path")
		tenant       = fs.String("tenant", "bench-tenant", "tenant_id stamped on generated events")
		service      = fs.String("service", "bench-service", "service_name stamped on generated events")
		serviceCount = fs.Int("services", 1, "spread events across this many synthetic services; keys are hashed, so use ~4x the partition count to cover every partition (1 measures a single partition)")
		severity     = fs.String("severity", string(event.SeverityInfo), "severity stamped on generated events")
	)

	if err := fs.Parse(args); err != nil {
		return err
	}

	selected, err := selectTargets(*targets, *ingestionURL, *incidentsURL, *batchSize, bench.EventOptions{
		TenantID:     *tenant,
		ServiceName:  *service,
		ServiceCount: *serviceCount,
		Severity:     event.Severity(*severity),
	})
	if err != nil {
		return err
	}

	// Ctrl-C ends the run rather than killing it, so a partial measurement is
	// still reported instead of thrown away.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts := bench.Options{
		Workers:  *workers,
		Requests: *requests,
		Duration: *duration,
		Warmup:   *warmup,
		Client:   bench.NewClient(*workers),
	}

	budget := fmt.Sprintf("%s per path", *duration)
	if *requests > 0 {
		budget = fmt.Sprintf("%d requests per path", *requests)
	}
	fmt.Fprintf(out, "measuring %d path(s) at %d workers, %s (warmup %d)\n\n",
		len(selected), *workers, budget, *warmup)

	results := make([]bench.Result, 0, len(selected))
	for _, target := range selected {
		fmt.Fprintf(out, "  %-20s ", target.Name)

		result, err := bench.Run(ctx, target, opts)
		if err != nil {
			return fmt.Errorf("measure %s: %w", target.Name, err)
		}
		results = append(results, result)

		fmt.Fprintf(out, "%d requests in %s\n", result.Requests(), result.Elapsed.Round(time.Millisecond))

		if ctx.Err() != nil {
			fmt.Fprintf(out, "\ninterrupted; reporting what was measured so far\n")
			break
		}
	}

	fmt.Fprintf(out, "\n%s\n%s", bench.Report(results), bench.Summary(results))

	// An unclean run is an exit code, not a footnote: a benchmark that was
	// mostly rejected must not pass silently in a script or a CI step.
	for _, result := range results {
		if !result.Clean() {
			return fmt.Errorf("%s did not complete cleanly; its latencies are not a valid measurement", result.Name)
		}
	}
	return nil
}

// selectTargets turns the -targets flag into the targets to measure, in a stable
// order so two runs produce comparable tables.
func selectTargets(spec, ingestionURL, incidentsURL string, batchSize int, eventOpts bench.EventOptions) ([]bench.Target, error) {
	wanted := make(map[string]bool)
	for _, name := range strings.Split(spec, ",") {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" {
			continue
		}
		wanted[name] = true
	}
	if len(wanted) == 0 {
		return nil, fmt.Errorf("no targets selected")
	}

	all := wanted["all"]
	known := map[string]bool{"all": true, "health": true, "incidents": true, "events": true, "batch": true}
	for name := range wanted {
		if !known[name] {
			return nil, fmt.Errorf("unknown target %q (want all, health, incidents or events)", name)
		}
	}

	var selected []bench.Target

	if all || wanted["health"] {
		target, err := bench.HealthTarget(ingestionURL)
		if err != nil {
			return nil, err
		}
		selected = append(selected, target)
	}
	if all || wanted["incidents"] {
		target, err := bench.IncidentsTarget(incidentsURL)
		if err != nil {
			return nil, err
		}
		selected = append(selected, target)
	}
	if all || wanted["events"] {
		target, err := bench.EventsTarget(ingestionURL, eventOpts)
		if err != nil {
			return nil, err
		}
		selected = append(selected, target)
	}
	if all || wanted["batch"] {
		target, err := bench.EventsBatchTarget(ingestionURL, eventOpts, batchSize)
		if err != nil {
			return nil, err
		}
		selected = append(selected, target)
	}

	return selected, nil
}
