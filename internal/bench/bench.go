// Package bench measures the throughput and latency of SentinelFlow's HTTP
// paths.
//
// It exists because a performance number nobody can reproduce is not a
// measurement. The README quotes req/s and percentiles for three paths; this
// package is the thing that produced them, so a reader can re-run it and a
// later change can be held to a before/after comparison rather than a claim.
//
// The design choices here are all about not measuring the wrong thing:
// connections are pooled per worker so the numbers are not dominated by TCP and
// TLS setup, latencies are recorded into per-worker slices so the measurement
// itself takes no lock on the hot path, and a warmup phase is discarded so the
// first requests — which pay for connection establishment and a cold database
// cache — do not skew the percentiles.
package bench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Target is one HTTP path to measure.
type Target struct {
	// Name labels the target in the report, e.g. "POST /v1/events".
	Name string

	// Work describes what the path actually does, so the report explains why one
	// row is slower than another instead of just asserting that it is.
	Work string

	Method string
	URL    string

	// ContentType is sent only when Body is non-nil.
	ContentType string

	// Body builds a fresh request body. It is called once per request rather
	// than once per run, so a target that must send a unique identifier every
	// time — POST /v1/events needs a new event_id — can do so. A nil Body sends
	// no body.
	Body func() ([]byte, error)
}

// validate reports whether the target is usable.
func (t Target) validate() error {
	switch {
	case t.Name == "":
		return errors.New("target requires a name")
	case t.Method == "":
		return errors.New("target requires an HTTP method")
	case t.URL == "":
		return errors.New("target requires a URL")
	}
	return nil
}

// Options configures a run.
//
// Requests and Duration are alternative budgets: an exact request count makes a
// run deterministic (which is what the tests use), a duration makes it
// comparable across machines of different speeds (which is what the Makefile
// target uses). Requests wins when both are set.
type Options struct {
	// Workers is how many requests are in flight concurrently.
	Workers int

	// Requests is the exact total to send. Zero means use Duration instead.
	Requests int

	// Duration bounds a run when Requests is zero.
	Duration time.Duration

	// Warmup requests are sent, and their results discarded, before measuring.
	Warmup int

	// Client is the HTTP client to use. When nil, one tuned for this worker
	// count is built by NewClient.
	Client *http.Client
}

// withDefaults fills in the unset fields and rejects a combination that would
// measure nothing.
func (o Options) withDefaults() (Options, error) {
	if o.Workers <= 0 {
		o.Workers = 50
	}
	if o.Requests < 0 {
		return o, fmt.Errorf("request count %d must not be negative", o.Requests)
	}
	if o.Warmup < 0 {
		return o, fmt.Errorf("warmup count %d must not be negative", o.Warmup)
	}
	if o.Requests == 0 && o.Duration <= 0 {
		return o, errors.New("a run needs either a request count or a duration")
	}
	if o.Client == nil {
		o.Client = NewClient(o.Workers)
	}
	return o, nil
}

// NewClient builds an HTTP client suited to a run with the given concurrency.
//
// The default transport keeps only two idle connections per host, so a run with
// fifty workers would spend most of its time opening and closing sockets and
// would report that cost as service latency. Sizing the idle pool to the worker
// count keeps every connection hot for the whole run.
func NewClient(workers int) *http.Client {
	if workers <= 0 {
		workers = 50
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = workers * 2
	transport.MaxIdleConnsPerHost = workers
	transport.MaxConnsPerHost = workers

	return &http.Client{
		Transport: transport,
		// Long enough that a slow path is measured rather than cut off, short
		// enough that a hung server ends the run instead of stalling it.
		Timeout: 30 * time.Second,
	}
}

// Result is one target's measured outcome.
type Result struct {
	Name string

	// Work is carried through from the Target so a Result is self-describing in
	// the report without the caller having to pair it back up with its target.
	Work string

	// Elapsed is the wall-clock time the measured phase took, which is what
	// throughput is computed against.
	Elapsed time.Duration

	// Statuses counts responses by HTTP status code. It is reported rather than
	// reduced to a pass/fail because a run that is uniformly answering 503 would
	// otherwise look like an excellent result.
	Statuses map[int]int

	// Failures counts requests that produced no response at all: a connection
	// refused, a timeout, a body that could not be built.
	Failures int

	// latencies holds every completed request's duration, sorted once when the
	// run finishes so percentiles are a lookup.
	latencies []time.Duration
}

// Requests is how many requests completed with a response.
func (r Result) Requests() int { return len(r.latencies) }

// Successes is how many requests returned a 2xx status.
func (r Result) Successes() int {
	n := 0
	for status, count := range r.Statuses {
		if status >= 200 && status < 300 {
			n += count
		}
	}
	return n
}

// Clean reports whether every request that completed did so successfully. A run
// that is not clean should not have its latencies quoted.
func (r Result) Clean() bool {
	return r.Failures == 0 && r.Successes() == r.Requests() && r.Requests() > 0
}

// RequestsPerSecond is throughput over the measured phase.
func (r Result) RequestsPerSecond() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Requests()) / r.Elapsed.Seconds()
}

// Average is the mean latency of the completed requests.
func (r Result) Average() time.Duration {
	if len(r.latencies) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range r.latencies {
		total += d
	}
	return total / time.Duration(len(r.latencies))
}

// Percentile returns the q-th percentile latency, q in [0,1].
//
// This is the nearest-rank definition: the smallest sample at or above which q
// of the samples fall. It needs no interpolation, which means every number it
// reports is a latency that actually happened.
func (r Result) Percentile(q float64) time.Duration {
	if len(r.latencies) == 0 {
		return 0
	}
	if q <= 0 {
		return r.latencies[0]
	}
	if q >= 1 {
		return r.latencies[len(r.latencies)-1]
	}

	rank := int(math.Ceil(q * float64(len(r.latencies))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(r.latencies) {
		rank = len(r.latencies)
	}
	return r.latencies[rank-1]
}

// Run measures one target.
//
// It sends the warmup requests first and throws their results away, then starts
// the clock and measures. The returned error covers only setup problems; a
// request that fails during the run is counted in the Result, because a partial
// measurement with a visible failure count is more useful than no measurement.
func Run(ctx context.Context, target Target, opts Options) (Result, error) {
	if err := target.validate(); err != nil {
		return Result{}, err
	}
	opts, err := opts.withDefaults()
	if err != nil {
		return Result{}, err
	}

	if opts.Warmup > 0 {
		warmup := opts
		warmup.Requests = opts.Warmup
		warmup.Duration = 0
		warmup.Warmup = 0
		if _, err := measure(ctx, target, warmup); err != nil {
			return Result{}, fmt.Errorf("warm up %s: %w", target.Name, err)
		}
		if ctx.Err() != nil {
			return Result{}, ctx.Err()
		}
	}

	return measure(ctx, target, opts)
}

// measure runs the worker pool and collects the results.
func measure(ctx context.Context, target Target, opts Options) (Result, error) {
	runCtx := ctx
	var cancel context.CancelFunc
	if opts.Requests == 0 {
		runCtx, cancel = context.WithTimeout(ctx, opts.Duration)
		defer cancel()
	}

	// remaining is the shared budget in request-count mode. Each worker claims
	// one request at a time, so a slow worker cannot hold back the others.
	var remaining atomic.Int64
	remaining.Store(int64(opts.Requests))

	perWorker := make([][]time.Duration, opts.Workers)
	statuses := make([]map[int]int, opts.Workers)
	failures := make([]int, opts.Workers)

	// Size each worker's latency slice up front so that appending during the
	// run never triggers an allocation that would be measured as latency.
	capacity := 0
	if opts.Requests > 0 {
		capacity = opts.Requests/opts.Workers + 1
	}

	var wg sync.WaitGroup
	start := time.Now()

	for i := range opts.Workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			latencies := make([]time.Duration, 0, capacity)
			seen := make(map[int]int)
			failed := 0

			for {
				if runCtx.Err() != nil {
					break
				}
				if opts.Requests > 0 && remaining.Add(-1) < 0 {
					break
				}

				status, elapsed, err := send(runCtx, opts.Client, target)
				if err != nil {
					// A cancelled context is the end of the run, not a failure
					// of the service under test.
					if runCtx.Err() != nil {
						break
					}
					failed++
					continue
				}
				latencies = append(latencies, elapsed)
				seen[status]++
			}

			perWorker[worker] = latencies
			statuses[worker] = seen
			failures[worker] = failed
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	result := Result{
		Name:     target.Name,
		Work:     target.Work,
		Elapsed:  elapsed,
		Statuses: make(map[int]int),
	}

	total := 0
	for _, l := range perWorker {
		total += len(l)
	}
	result.latencies = make([]time.Duration, 0, total)

	for worker := range opts.Workers {
		result.latencies = append(result.latencies, perWorker[worker]...)
		for status, count := range statuses[worker] {
			result.Statuses[status] += count
		}
		result.Failures += failures[worker]
	}

	sort.Slice(result.latencies, func(i, j int) bool {
		return result.latencies[i] < result.latencies[j]
	})

	return result, nil
}

// send performs one request and returns its status and round-trip duration.
//
// The body is drained before closing so the connection returns to the idle pool
// instead of being torn down; without that, a run measures reconnection rather
// than the service.
func send(ctx context.Context, client *http.Client, target Target) (int, time.Duration, error) {
	var body io.Reader
	if target.Body != nil {
		payload, err := target.Body()
		if err != nil {
			return 0, 0, fmt.Errorf("build request body: %w", err)
		}
		body = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, target.Method, target.URL, body)
	if err != nil {
		return 0, 0, fmt.Errorf("build request: %w", err)
	}
	if target.Body != nil && target.ContentType != "" {
		req.Header.Set("Content-Type", target.ContentType)
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("send request: %w", err)
	}

	_, copyErr := io.Copy(io.Discard, resp.Body)
	closeErr := resp.Body.Close()
	elapsed := time.Since(start)

	if copyErr != nil {
		return 0, 0, fmt.Errorf("read response body: %w", copyErr)
	}
	if closeErr != nil {
		return 0, 0, fmt.Errorf("close response body: %w", closeErr)
	}

	return resp.StatusCode, elapsed, nil
}
