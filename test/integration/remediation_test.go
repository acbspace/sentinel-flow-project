//go:build integration

// This file exercises remediation against real PostgreSQL and Temporal: a
// runbook's unattended step runs on its own, its gated step waits for a human,
// and approving or rejecting that step decides whether anything else happens.
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
	"github.com/acbspace/sentinel-flow-project/internal/remediate"
	"github.com/acbspace/sentinel-flow-project/internal/runbook"
	"github.com/acbspace/sentinel-flow-project/internal/store"
	"github.com/acbspace/sentinel-flow-project/migrations"
)

type remediationHarness struct {
	incidents   *store.IncidentStore
	remediation *store.RemediationStore
	temporal    client.Client
	taskQueue   string
	tenant      string
}

func newRemediationHarness(ctx context.Context, t *testing.T) *remediationHarness {
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
	remediationStore := store.NewRemediationStore(pool, dbMetrics, 10*time.Second)

	tenant := "itest-remed-" + uuid.NewString()[:8]
	taskQueue := "remediation-itest-" + uuid.NewString()[:8]

	activities := remediate.NewActivities(
		incidentStore, remediationStore,
		remediate.NewExecutor(5*time.Second, log), log,
	)
	w := worker.New(temporalClient, taskQueue, worker.Options{})
	w.RegisterWorkflow(remediate.IncidentRemediationWorkflow)
	w.RegisterActivity(activities)
	if err := w.Start(); err != nil {
		t.Fatalf("start temporal worker: %v", err)
	}
	t.Cleanup(w.Stop)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Deleting the incident cascades to its remediation actions.
		if _, err := pool.Exec(cleanupCtx, "DELETE FROM incidents WHERE tenant_id = $1", tenant); err != nil {
			t.Logf("cleanup incidents: %v", err)
		}
	})

	return &remediationHarness{
		incidents:   incidentStore,
		remediation: remediationStore,
		temporal:    temporalClient,
		taskQueue:   taskQueue,
		tenant:      tenant,
	}
}

// testRunbook is one unattended step followed by one gated step, with a short
// approval window so the timeout path is reachable in a test.
func testRunbook() runbook.Runbook {
	return runbook.Runbook{
		ID:              "itest-runbook",
		Name:            "integration test runbook",
		Match:           runbook.Matcher{RuleID: "error_rate"},
		ApprovalTimeout: 30 * time.Second,
		Steps: []runbook.Step{
			{Name: "capture diagnostics", Kind: runbook.ActionNoop, Mode: runbook.ModeAuto},
			{Name: "restart instances", Kind: runbook.ActionNoop, Mode: runbook.ModeApproval},
		},
	}
}

func (h *remediationHarness) openIncident(ctx context.Context, t *testing.T) string {
	t.Helper()

	now := time.Now().UTC()
	id := uuid.NewString()
	opened, err := h.incidents.UpsertOpen(ctx, incident.Incident{
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
	})
	if err != nil {
		t.Fatalf("open incident: %v", err)
	}
	if !opened {
		t.Fatal("expected a freshly opened incident")
	}
	return id
}

func (h *remediationHarness) startRun(ctx context.Context, t *testing.T, incidentID string) {
	t.Helper()

	starter := remediate.NewTemporalStarter(h.temporal, h.taskQueue)
	if err := starter.StartRemediationWorkflow(ctx, remediate.IncidentRemediationInput{
		Incident: remediate.IncidentRef{
			ID: incidentID, TenantID: h.tenant, ServiceName: "payment-service",
			Severity: "error", Title: "elevated error rate",
		},
		Runbook: testRunbook(),
	}); err != nil {
		t.Fatalf("start remediation workflow: %v", err)
	}
}

// waitForStepStatus polls until the given step reaches the expected status.
func (h *remediationHarness) waitForStepStatus(ctx context.Context, t *testing.T, incidentID string, step int, want string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		if got, ok := h.stepStatus(ctx, t, incidentID, step); ok {
			if got == want {
				return
			}
			last = got
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("step %d of incident %s did not reach %q within %s (last seen %q)", step, incidentID, want, timeout, last)
}

func (h *remediationHarness) stepStatus(ctx context.Context, t *testing.T, incidentID string, step int) (string, bool) {
	t.Helper()

	actions, err := h.remediation.ListByIncident(ctx, incidentID)
	if err != nil {
		t.Fatalf("list remediation actions: %v", err)
	}
	for _, a := range actions {
		if a.StepIndex == step {
			return a.Status, true
		}
	}
	return "", false
}

// TestRemediationRunsAutoStepThenGatesOnApproval covers remediation's core
// promise: the safe step runs unattended, the dangerous one stops and waits, and
// it only proceeds once a human approves.
func TestRemediationRunsAutoStepThenGatesOnApproval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	h := newRemediationHarness(ctx, t)
	id := h.openIncident(ctx, t)
	h.startRun(ctx, t, id)

	// The unattended step completes on its own.
	h.waitForStepStatus(ctx, t, id, 1, store.RemediationSucceeded, 20*time.Second)

	// The gated step stops and waits rather than acting.
	h.waitForStepStatus(ctx, t, id, 2, store.RemediationPending, 20*time.Second)

	// Nothing has executed it while it waits.
	if got, _ := h.stepStatus(ctx, t, id, 2); got != store.RemediationPending {
		t.Fatalf("gated step status = %q, want it still pending before approval", got)
	}

	if err := remediate.NewTemporalSignaler(h.temporal).Approve(ctx, id, "alice"); err != nil {
		t.Fatalf("approve: %v", err)
	}

	h.waitForStepStatus(ctx, t, id, 2, store.RemediationSucceeded, 20*time.Second)
}

// TestRemediationHaltsWhenRejected proves the safety property: a rejected step is
// recorded as rejected and never executed.
func TestRemediationHaltsWhenRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	h := newRemediationHarness(ctx, t)
	id := h.openIncident(ctx, t)
	h.startRun(ctx, t, id)

	h.waitForStepStatus(ctx, t, id, 2, store.RemediationPending, 20*time.Second)

	if err := remediate.NewTemporalSignaler(h.temporal).Reject(ctx, id, "bob"); err != nil {
		t.Fatalf("reject: %v", err)
	}

	h.waitForStepStatus(ctx, t, id, 2, store.RemediationRejected, 20*time.Second)

	// Give the workflow a moment; a rejection must not later flip to executed.
	time.Sleep(3 * time.Second)
	if got, _ := h.stepStatus(ctx, t, id, 2); got != store.RemediationRejected {
		t.Errorf("step 2 status = %q, want it to stay rejected", got)
	}
}
