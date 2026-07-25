//go:build integration

// This file exercises correlation against real PostgreSQL: the correlation
// engine reading window stats, the partial-unique-index dedup in UpsertOpen, the
// guarded lifecycle transitions, and auto-resolution. Each test isolates itself
// with a unique tenant id so a run never sees another run's (or the demo's)
// events when it computes an error rate.
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acbspace/sentinel-flow-project/internal/correlate"
	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/migrate"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
	"github.com/acbspace/sentinel-flow-project/internal/store"
	"github.com/acbspace/sentinel-flow-project/migrations"
)

// correlationHarness bundles the two stores and the pool for a correlation test.
type correlationHarness struct {
	events    *store.EventStore
	incidents *store.IncidentStore
	pool      *pgxpool.Pool
	tenant    string
}

// newCorrelationHarness connects to Postgres, applies migrations, and returns a
// harness scoped to a fresh tenant that is cleaned up when the test ends.
func newCorrelationHarness(ctx context.Context, t *testing.T) *correlationHarness {
	t.Helper()

	log := testLogger(t)
	providers := obs.NoopProviders()
	dbMetrics, err := obs.NewDBMetrics(providers.MeterProvider)
	if err != nil {
		t.Fatalf("create db metrics: %v", err)
	}

	dsn := envOr("POSTGRES_DSN", defaultDSN)
	pool, err := store.NewPool(ctx, store.PoolConfig{DSN: dsn, MaxConns: 4, ConnectTimeout: 10 * time.Second}, log)
	if err != nil {
		t.Fatalf("connect to postgres at %s: %v\nis the stack running? try: make up", dsn, err)
	}
	t.Cleanup(pool.Close)

	loaded, err := migrate.Load(migrations.FS)
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if err := migrate.Up(ctx, pool, loaded, log); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	tenant := "itest-" + uuid.NewString()[:8]
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Leave the schema; remove only this run's rows.
		if _, err := pool.Exec(cleanupCtx, "DELETE FROM incidents WHERE tenant_id = $1", tenant); err != nil {
			t.Logf("cleanup incidents: %v", err)
		}
		if _, err := pool.Exec(cleanupCtx, "DELETE FROM telemetry_events WHERE tenant_id = $1", tenant); err != nil {
			t.Logf("cleanup events: %v", err)
		}
	})

	return &correlationHarness{
		events:    store.NewEventStore(pool, dbMetrics, 10*time.Second),
		incidents: store.NewIncidentStore(pool, dbMetrics, 10*time.Second),
		pool:      pool,
		tenant:    tenant,
	}
}

// insertEvents writes n events for this harness's tenant with the given service
// and severity, all timestamped at.
func (h *correlationHarness) insertEvents(ctx context.Context, t *testing.T, service, severity string, n int, at time.Time) {
	t.Helper()

	for i := 0; i < n; i++ {
		ev := event.Event{
			EventID:       uuid.NewString(),
			SchemaVersion: event.SchemaVersion10,
			TenantID:      h.tenant,
			ServiceName:   service,
			Environment:   "test",
			EventType:     "request.completed",
			Severity:      event.Severity(severity),
			Timestamp:     event.NewTimestamp(at),
			Attributes:    map[string]any{},
		}
		ev.Normalize()
		if err := ev.Validate(); err != nil {
			t.Fatalf("crafted an invalid test event: %v", err)
		}
		if _, err := h.events.Insert(ctx, ev, at); err != nil {
			t.Fatalf("insert test event: %v", err)
		}
	}
}

// tenantIncidents returns every incident for this harness's tenant, newest
// activity first.
func (h *correlationHarness) tenantIncidents(ctx context.Context, t *testing.T) []incident.Incident {
	t.Helper()

	got, err := h.incidents.List(ctx, store.IncidentFilter{TenantID: h.tenant})
	if err != nil {
		t.Fatalf("list incidents: %v", err)
	}
	return got
}

// evaluatorAt builds an evaluator whose clock is pinned to now, so the window
// bounds and resolve cutoff are exact.
func (h *correlationHarness) evaluatorAt(t *testing.T, now time.Time, resolveAfter time.Duration) *correlate.Evaluator {
	rule := correlate.Rule{
		ID:               "error_rate",
		Name:             "elevated error rate",
		Kind:             correlate.RuleKindErrorRate,
		Window:           time.Minute,
		Threshold:        0.5,
		MinEvents:        5,
		IncidentSeverity: event.SeverityError,
	}
	return correlate.NewEvaluator(correlate.EvaluatorOptions{
		Source:       h.events,
		Sink:         h.incidents,
		Rules:        []correlate.Rule{rule},
		Logger:       testLogger(t),
		ResolveAfter: resolveAfter,
		Now:          func() time.Time { return now },
	})
}

// TestCorrelationOpensGroupsAndReopensAfterResolve walks a full lifecycle end to
// end against the database: an error spike opens one incident, a second cycle
// groups into it (not a duplicate), an operator acknowledges then resolves it,
// and because the condition persists a subsequent cycle opens a brand-new
// incident -- which is exactly what the partial unique index is there to allow.
func TestCorrelationOpensGroupsAndReopensAfterResolve(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	h := newCorrelationHarness(ctx, t)
	at := time.Now().UTC()

	// 8 errors, 0 successes: a 100% error rate over a sample well above the min.
	h.insertEvents(ctx, t, "payment-service", "error", 8, at)

	// Resolve-after is an hour so nothing auto-resolves mid-test.
	ev := h.evaluatorAt(t, at, time.Hour)

	// First cycle opens exactly one incident.
	if err := ev.EvaluateOnce(ctx); err != nil {
		t.Fatalf("first EvaluateOnce: %v", err)
	}
	opened := h.tenantIncidents(ctx, t)
	if len(opened) != 1 {
		t.Fatalf("after first cycle: %d incidents, want 1", len(opened))
	}
	inc := opened[0]
	if inc.Status != incident.StatusOpen {
		t.Errorf("status = %q, want open", inc.Status)
	}
	if inc.Severity != event.SeverityError {
		t.Errorf("severity = %q, want error", inc.Severity)
	}
	if inc.ServiceName != "payment-service" {
		t.Errorf("service = %q, want payment-service", inc.ServiceName)
	}
	if inc.EventCount != 8 {
		t.Errorf("event_count = %d, want 8", inc.EventCount)
	}

	// Second cycle over the same condition groups into the same incident: still
	// one row, with the event count accumulated rather than a duplicate opened.
	if err := ev.EvaluateOnce(ctx); err != nil {
		t.Fatalf("second EvaluateOnce: %v", err)
	}
	grouped := h.tenantIncidents(ctx, t)
	if len(grouped) != 1 {
		t.Fatalf("after grouping cycle: %d incidents, want still 1", len(grouped))
	}
	if grouped[0].ID != inc.ID {
		t.Errorf("grouping opened a new incident %s, want the original %s", grouped[0].ID, inc.ID)
	}
	if grouped[0].EventCount != 16 {
		t.Errorf("event_count = %d, want 16 after grouping", grouped[0].EventCount)
	}

	// Operator takes ownership, then resolves.
	if acked, err := h.incidents.Acknowledge(ctx, inc.ID); err != nil {
		t.Fatalf("acknowledge: %v", err)
	} else if acked.Status != incident.StatusAcknowledged {
		t.Errorf("status after acknowledge = %q, want acknowledged", acked.Status)
	}
	if resolved, err := h.incidents.Resolve(ctx, inc.ID); err != nil {
		t.Fatalf("resolve: %v", err)
	} else if resolved.Status != incident.StatusResolved || resolved.ResolvedAt == nil {
		t.Errorf("resolve did not set status/resolved_at: %+v", resolved)
	}

	// Resolving twice is not allowed.
	if _, err := h.incidents.Resolve(ctx, inc.ID); err == nil {
		t.Error("resolving an already-resolved incident succeeded, want an error")
	}

	// The condition is still firing, and the previous incident is resolved, so a
	// new cycle opens a fresh incident under the same fingerprint.
	if err := ev.EvaluateOnce(ctx); err != nil {
		t.Fatalf("reopen EvaluateOnce: %v", err)
	}
	all := h.tenantIncidents(ctx, t)
	if len(all) != 2 {
		t.Fatalf("after reopen: %d incidents, want 2 (one resolved, one open)", len(all))
	}
	var open, resolved int
	for _, i := range all {
		switch i.Status {
		case incident.StatusOpen:
			open++
			if i.ID == inc.ID {
				t.Error("the reopened incident reused the resolved incident's id")
			}
		case incident.StatusResolved:
			resolved++
		}
	}
	if open != 1 || resolved != 1 {
		t.Errorf("statuses = %d open / %d resolved, want 1 / 1", open, resolved)
	}
}

// TestCorrelationAutoResolvesQuietIncident opens an incident, then runs a later
// cycle in which the offending events have aged out of the window. With nothing
// re-firing, the incident's last detection is older than the resolve cutoff and
// the cycle closes it automatically.
func TestCorrelationAutoResolvesQuietIncident(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	h := newCorrelationHarness(ctx, t)
	at := time.Now().UTC()
	h.insertEvents(ctx, t, "payment-service", "error", 8, at)

	// Open the incident at time `at`.
	if err := h.evaluatorAt(t, at, time.Hour).EvaluateOnce(ctx); err != nil {
		t.Fatalf("open EvaluateOnce: %v", err)
	}
	if got := h.tenantIncidents(ctx, t); len(got) != 1 || got[0].Status != incident.StatusOpen {
		t.Fatalf("setup did not open exactly one open incident: %+v", got)
	}

	// Ten minutes later, the 1-minute window no longer covers those events, so
	// nothing re-fires; with a 5-minute quiet period the incident is now stale.
	later := at.Add(10 * time.Minute)
	if err := h.evaluatorAt(t, later, 5*time.Minute).EvaluateOnce(ctx); err != nil {
		t.Fatalf("resolve EvaluateOnce: %v", err)
	}

	got := h.tenantIncidents(ctx, t)
	if len(got) != 1 {
		t.Fatalf("auto-resolve changed the incident count: %d, want 1", len(got))
	}
	if got[0].Status != incident.StatusResolved {
		t.Errorf("status = %q, want resolved after the quiet period", got[0].Status)
	}
}

// TestCorrelationLeavesHealthyServiceAlone confirms a low error rate opens no
// incident: signal, not noise.
func TestCorrelationLeavesHealthyServiceAlone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	h := newCorrelationHarness(ctx, t)
	at := time.Now().UTC()

	// 19 clean, 1 error: 5%, well under the 50% threshold.
	h.insertEvents(ctx, t, "order-service", "info", 19, at)
	h.insertEvents(ctx, t, "order-service", "error", 1, at)

	if err := h.evaluatorAt(t, at, time.Hour).EvaluateOnce(ctx); err != nil {
		t.Fatalf("EvaluateOnce: %v", err)
	}

	if got := h.tenantIncidents(ctx, t); len(got) != 0 {
		t.Errorf("opened %d incidents for a healthy service, want 0: %+v", len(got), got)
	}
}
