package correlate_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/correlate"
	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

// fakeSource returns a scripted set of window stats and records the lookback it
// was asked for, so a test can assert the window bound the evaluator computed.
type fakeSource struct {
	windows []store.ServiceWindow
	err     error

	calls int
	since time.Time
}

func (f *fakeSource) WindowStats(_ context.Context, since time.Time) ([]store.ServiceWindow, error) {
	f.calls++
	f.since = since
	return f.windows, f.err
}

// fakeSink stands in for the incident store. It simulates the store's dedup
// contract in memory: the first upsert of a fingerprint "opens" (returns true),
// a later upsert of the same fingerprint "groups" (returns false).
type fakeSink struct {
	upserts   []incident.Incident
	active    map[string]bool
	opened    int
	grouped   int
	upsertErr error

	resolveCount int64
	resolveErr   error
	resolveCalls int
	resolveArg   time.Time
}

func (f *fakeSink) UpsertOpen(_ context.Context, inc incident.Incident) (bool, error) {
	if f.upsertErr != nil {
		return false, f.upsertErr
	}
	f.upserts = append(f.upserts, inc)
	if f.active == nil {
		f.active = make(map[string]bool)
	}
	if f.active[inc.Fingerprint] {
		f.grouped++
		return false, nil
	}
	f.active[inc.Fingerprint] = true
	f.opened++
	return true, nil
}

func (f *fakeSink) AutoResolveStale(_ context.Context, olderThan time.Time) (int64, error) {
	f.resolveCalls++
	f.resolveArg = olderThan
	return f.resolveCount, f.resolveErr
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func seqID() func() string {
	n := 0
	return func() string {
		n++
		return fmt.Sprintf("incident-%d", n)
	}
}

// newEvaluator wires an evaluator with a fixed clock and deterministic ids over
// the supplied fakes.
func newEvaluator(src correlate.WindowSource, sink correlate.IncidentSink, now time.Time) *correlate.Evaluator {
	return correlate.NewEvaluator(correlate.EvaluatorOptions{
		Source:       src,
		Sink:         sink,
		Rules:        []correlate.Rule{errorRateRule()},
		Logger:       discardLogger(),
		ResolveAfter: 5 * time.Minute,
		Now:          func() time.Time { return now },
		NewID:        seqID(),
		// Metrics deliberately nil: the evaluator must run without a meter.
	})
}

func TestEvaluateOnceOpensIncident(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	src := &fakeSource{windows: []store.ServiceWindow{window(20, 15)}}
	sink := &fakeSink{}

	if err := newEvaluator(src, sink, now).EvaluateOnce(context.Background()); err != nil {
		t.Fatalf("EvaluateOnce() = %v", err)
	}

	// The lookback is exactly one window before the fixed clock.
	if wantSince := now.Add(-time.Minute); !src.since.Equal(wantSince) {
		t.Errorf("WindowStats lookback = %v, want %v", src.since, wantSince)
	}

	if sink.opened != 1 || len(sink.upserts) != 1 {
		t.Fatalf("opened = %d, upserts = %d, want exactly one opened incident", sink.opened, len(sink.upserts))
	}

	inc := sink.upserts[0]
	if want := incident.Fingerprint("error_rate", "tenant-a", "payment-service"); inc.Fingerprint != want {
		t.Errorf("Fingerprint = %q, want %q", inc.Fingerprint, want)
	}
	if inc.Status != incident.StatusOpen {
		t.Errorf("Status = %q, want %q", inc.Status, incident.StatusOpen)
	}
	if inc.EventCount != 15 {
		t.Errorf("EventCount = %d, want 15", inc.EventCount)
	}
	if !inc.FirstSeenAt.Equal(now) || !inc.LastSeenAt.Equal(now) {
		t.Errorf("first/last seen = %v/%v, want both %v", inc.FirstSeenAt, inc.LastSeenAt, now)
	}
	if err := inc.Validate(); err != nil {
		t.Errorf("built incident failed Validate: %v", err)
	}

	// Auto-resolution runs with a cutoff exactly ResolveAfter before now.
	if sink.resolveCalls != 1 {
		t.Errorf("AutoResolveStale calls = %d, want 1", sink.resolveCalls)
	}
	if wantCutoff := now.Add(-5 * time.Minute); !sink.resolveArg.Equal(wantCutoff) {
		t.Errorf("resolve cutoff = %v, want %v", sink.resolveArg, wantCutoff)
	}
}

func TestEvaluateOnceBelowThresholdOpensNothing(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	src := &fakeSource{windows: []store.ServiceWindow{window(20, 2)}} // 10%
	sink := &fakeSink{}

	if err := newEvaluator(src, sink, now).EvaluateOnce(context.Background()); err != nil {
		t.Fatalf("EvaluateOnce() = %v", err)
	}

	if len(sink.upserts) != 0 {
		t.Errorf("upserts = %d, want 0 when no rule fires", len(sink.upserts))
	}
	// Auto-resolution still runs even when nothing new fires.
	if sink.resolveCalls != 1 {
		t.Errorf("AutoResolveStale calls = %d, want 1", sink.resolveCalls)
	}
}

func TestEvaluateOnceGroupsRepeatDetection(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	src := &fakeSource{windows: []store.ServiceWindow{window(20, 15)}}
	sink := &fakeSink{}
	ev := newEvaluator(src, sink, now)
	ctx := context.Background()

	if err := ev.EvaluateOnce(ctx); err != nil {
		t.Fatalf("first EvaluateOnce() = %v", err)
	}
	if err := ev.EvaluateOnce(ctx); err != nil {
		t.Fatalf("second EvaluateOnce() = %v", err)
	}

	// Two cycles over the same firing condition: one incident opened, one repeat
	// detection grouped into it. This is the dedup guarantee in miniature.
	if sink.opened != 1 {
		t.Errorf("opened = %d, want 1", sink.opened)
	}
	if sink.grouped != 1 {
		t.Errorf("grouped = %d, want 1", sink.grouped)
	}
	if len(sink.active) != 1 {
		t.Errorf("active fingerprints = %d, want 1", len(sink.active))
	}
}

func TestEvaluateOnceOnlyFiresForOffendingService(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	src := &fakeSource{windows: []store.ServiceWindow{
		{TenantID: "tenant-a", ServiceName: "payment-service", Total: 20, Errors: 15}, // 75%
		{TenantID: "tenant-a", ServiceName: "order-service", Total: 20, Errors: 1},    // 5%
	}}
	sink := &fakeSink{}

	if err := newEvaluator(src, sink, now).EvaluateOnce(context.Background()); err != nil {
		t.Fatalf("EvaluateOnce() = %v", err)
	}

	if len(sink.upserts) != 1 {
		t.Fatalf("upserts = %d, want exactly 1", len(sink.upserts))
	}
	if got := sink.upserts[0].ServiceName; got != "payment-service" {
		t.Errorf("opened incident for %q, want payment-service", got)
	}
}

func TestEvaluateOnceDoesNotResolveWhenReadFails(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	src := &fakeSource{err: errors.New("database unreachable")}
	sink := &fakeSink{}

	err := newEvaluator(src, sink, now).EvaluateOnce(context.Background())
	if err == nil {
		t.Fatal("EvaluateOnce() = nil, want the source error to propagate")
	}

	// A failed read must not lead to auto-resolving incidents: absent data would
	// look like every incident has gone quiet and wrongly close them all.
	if sink.resolveCalls != 0 {
		t.Errorf("AutoResolveStale calls = %d, want 0 after a read failure", sink.resolveCalls)
	}
}
