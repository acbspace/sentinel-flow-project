//go:build integration

// This file exercises alerting against real PostgreSQL and a real Temporal
// server: the alert workflow pages the on-call responder, escalates when
// unacknowledged, and stops the moment the incident is acknowledged. Each test
// isolates itself with a unique tenant and Temporal task queue. It uses a policy
// with short (5s) level timeouts so escalation is observable in seconds.
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/acbspace/sentinel-flow-project/internal/alerting"
	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/migrate"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
	"github.com/acbspace/sentinel-flow-project/internal/oncall"
	"github.com/acbspace/sentinel-flow-project/internal/store"
	"github.com/acbspace/sentinel-flow-project/migrations"
)

const defaultTemporal = "localhost:7233"

type alertHarness struct {
	incidents     *store.IncidentStore
	notifications *store.NotificationStore
	temporal      client.Client
	taskQueue     string
	tenant        string
}

// newAlertHarness connects to Postgres and Temporal, applies migrations, and
// starts an in-process worker on a task queue unique to this run.
func newAlertHarness(ctx context.Context, t *testing.T) *alertHarness {
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

	temporalClient, err := alerting.DialTemporal(ctx, envOr("TEMPORAL_ADDRESS", defaultTemporal), "default", log)
	if err != nil {
		t.Fatalf("connect to temporal: %v\nis the stack running? try: make up", err)
	}
	t.Cleanup(temporalClient.Close)

	incidentStore := store.NewIncidentStore(pool, dbMetrics, 10*time.Second)
	notificationStore := store.NewNotificationStore(pool, dbMetrics, 10*time.Second)

	tenant := "itest-alert-" + uuid.NewString()[:8]
	taskQueue := "alerts-itest-" + uuid.NewString()[:8]

	// A worker for this run's task queue, wired to the real activities.
	notifier := alerting.NewNotifier(notificationStore, "", 0, log)
	activities := alerting.NewActivities(incidentStore, notifier, log)
	w := worker.New(temporalClient, taskQueue, worker.Options{})
	w.RegisterWorkflow(alerting.IncidentAlertWorkflow)
	w.RegisterActivity(activities)
	if err := w.Start(); err != nil {
		t.Fatalf("start temporal worker: %v", err)
	}
	t.Cleanup(w.Stop)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Deleting the incident cascades to its notifications.
		if _, err := pool.Exec(cleanupCtx, "DELETE FROM incidents WHERE tenant_id = $1", tenant); err != nil {
			t.Logf("cleanup incidents: %v", err)
		}
	})

	return &alertHarness{
		incidents:     incidentStore,
		notifications: notificationStore,
		temporal:      temporalClient,
		taskQueue:     taskQueue,
		tenant:        tenant,
	}
}

func shortPolicy() oncall.EscalationPolicy {
	return oncall.EscalationPolicy{Levels: []oncall.Level{
		{Target: "primary", AckTimeout: 5 * time.Second, Rotation: oncall.Rotation{Contacts: []oncall.Contact{{Name: "alice"}}}},
		{Target: "secondary", AckTimeout: 5 * time.Second, Rotation: oncall.Rotation{Contacts: []oncall.Contact{{Name: "bob"}}}},
	}}
}

func (h *alertHarness) openIncident(ctx context.Context, t *testing.T) string {
	t.Helper()

	now := time.Now().UTC()
	id := uuid.NewString()
	inc := incident.Incident{
		ID:          id,
		Fingerprint: incident.Fingerprint("error_rate", h.tenant, "payment-service"),
		TenantID:    h.tenant,
		ServiceName: "payment-service",
		RuleID:      "error_rate",
		Title:       "elevated error rate",
		Severity:    event.SeverityError,
		Status:      incident.StatusOpen,
		EventCount:  8,
		FirstSeenAt: now,
		LastSeenAt:  now,
	}
	opened, err := h.incidents.UpsertOpen(ctx, inc)
	if err != nil {
		t.Fatalf("open incident: %v", err)
	}
	if !opened {
		t.Fatal("expected a freshly opened incident")
	}
	return id
}

func (h *alertHarness) startWorkflow(ctx context.Context, t *testing.T, incidentID string) {
	t.Helper()

	starter := alerting.NewTemporalStarter(h.temporal, h.taskQueue)
	in := alerting.IncidentAlertInput{
		Incident: alerting.IncidentRef{
			ID: incidentID, TenantID: h.tenant, ServiceName: "payment-service",
			Severity: "error", Title: "elevated error rate",
		},
		Policy: shortPolicy(),
	}
	if err := starter.StartAlertWorkflow(ctx, in); err != nil {
		t.Fatalf("start alert workflow: %v", err)
	}
}

// waitForTarget polls until a notification for the given target appears.
func (h *alertHarness) waitForTarget(ctx context.Context, t *testing.T, incidentID, target string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if h.hasTarget(ctx, t, incidentID, target) {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("no notification for target %q on incident %s within %s", target, incidentID, timeout)
}

func (h *alertHarness) hasTarget(ctx context.Context, t *testing.T, incidentID, target string) bool {
	t.Helper()

	notes, err := h.notifications.ListByIncident(ctx, incidentID)
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	for _, n := range notes {
		if n.Target == target {
			return true
		}
	}
	return false
}

// TestAlertingAcknowledgeStopsEscalation opens an incident, lets its alert
// workflow page level 1, acknowledges it, and asserts escalation never reaches
// level 2. Acknowledgement goes through both paths this system uses — the
// database transition and the Temporal signal — so the assertion holds even if
// the signal races the escalation timer, because the workflow re-checks the
// database before each level.
func TestAlertingAcknowledgeStopsEscalation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	h := newAlertHarness(ctx, t)
	id := h.openIncident(ctx, t)
	h.startWorkflow(ctx, t, id)

	h.waitForTarget(ctx, t, id, "primary", 20*time.Second)

	if _, err := h.incidents.Acknowledge(ctx, id); err != nil {
		t.Fatalf("acknowledge incident: %v", err)
	}
	if err := alerting.NewTemporalSignaler(h.temporal).SignalAcknowledged(ctx, id); err != nil {
		t.Fatalf("signal acknowledge: %v", err)
	}

	// Well past the 5s level timeout: escalation must not have reached level 2.
	time.Sleep(8 * time.Second)
	if h.hasTarget(ctx, t, id, "secondary") {
		t.Error("escalation reached level 2 after the incident was acknowledged")
	}
}

// TestAlertingEscalatesWhenUnacknowledged opens an incident and, with no
// acknowledgement, asserts the workflow escalates from level 1 to level 2.
func TestAlertingEscalatesWhenUnacknowledged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	h := newAlertHarness(ctx, t)
	id := h.openIncident(ctx, t)
	h.startWorkflow(ctx, t, id)

	h.waitForTarget(ctx, t, id, "primary", 20*time.Second)
	h.waitForTarget(ctx, t, id, "secondary", 20*time.Second)
}
