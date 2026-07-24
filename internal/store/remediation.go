package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acbspace/sentinel-flow-project/internal/obs"
)

// Remediation action statuses, mirroring the CHECK constraint on the table.
const (
	RemediationPending   = "pending"
	RemediationApproved  = "approved"
	RemediationRejected  = "rejected"
	RemediationTimedOut  = "timed_out"
	RemediationSucceeded = "succeeded"
	RemediationFailed    = "failed"
	RemediationSkipped   = "skipped"
)

// ErrNoPendingAction is returned when an approve/reject arrives but no step of
// the incident's runbook is waiting for a decision.
var ErrNoPendingAction = errors.New("no remediation action is awaiting approval")

// RemediationAction is one step of a runbook as it was actually carried out.
type RemediationAction struct {
	ID         string         `json:"id"`
	IncidentID string         `json:"incident_id"`
	RunbookID  string         `json:"runbook_id"`
	StepIndex  int            `json:"step_index"`
	StepName   string         `json:"step_name"`
	ActionKind string         `json:"action_kind"`
	Mode       string         `json:"mode"`
	Status     string         `json:"status"`
	Actor      string         `json:"actor,omitempty"`
	Detail     map[string]any `json:"detail,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

// upsertActionSQL records a step or advances its status. The workflow derives the
// id deterministically, so a retry or replay updates the same row rather than
// creating a second one.
const upsertActionSQL = `
INSERT INTO remediation_actions (
    id, incident_id, runbook_id, step_index, step_name, action_kind, mode, status, actor, detail
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (id) DO UPDATE SET
    status     = EXCLUDED.status,
    actor      = EXCLUDED.actor,
    detail     = EXCLUDED.detail,
    updated_at = now()`

const listActionsSQL = `
SELECT id, incident_id, runbook_id, step_index, step_name, action_kind, mode,
       status, actor, detail, created_at, updated_at
FROM remediation_actions
WHERE incident_id = $1
ORDER BY step_index`

// pendingActionSQL finds the step awaiting a decision. At most one step of a
// runbook waits at a time, so this is unambiguous.
const pendingActionSQL = `
SELECT id, incident_id, runbook_id, step_index, step_name, action_kind, mode,
       status, actor, detail, created_at, updated_at
FROM remediation_actions
WHERE incident_id = $1 AND status = 'pending'
ORDER BY step_index
LIMIT 1`

// RemediationStore records and reads the automated-action audit trail.
type RemediationStore struct {
	pool    *pgxpool.Pool
	metrics *obs.DBMetrics
	timeout time.Duration
}

// NewRemediationStore builds a remediation store over an existing pool.
func NewRemediationStore(pool *pgxpool.Pool, metrics *obs.DBMetrics, timeout time.Duration) *RemediationStore {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &RemediationStore{pool: pool, metrics: metrics, timeout: timeout}
}

// Upsert records a step or advances its status.
func (s *RemediationStore) Upsert(ctx context.Context, a RemediationAction) error {
	detail, err := json.Marshal(a.Detail)
	if err != nil {
		s.metrics.Record(ctx, "upsert_remediation", "error", 0)
		return fmt.Errorf("encode remediation %s detail: %w", a.ID, err)
	}
	if a.Detail == nil {
		detail = []byte("{}")
	}

	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	_, err = s.pool.Exec(ctx, upsertActionSQL,
		a.ID, a.IncidentID, a.RunbookID, a.StepIndex, a.StepName,
		a.ActionKind, a.Mode, a.Status, a.Actor, detail,
	)
	elapsed := time.Since(start)

	if err != nil {
		s.metrics.Record(ctx, "upsert_remediation", "error", elapsed)
		return fmt.Errorf("upsert remediation action %s: %w", a.ID, err)
	}

	s.metrics.Record(ctx, "upsert_remediation", "ok", elapsed)
	return nil
}

// ListByIncident returns an incident's remediation actions in runbook order.
func (s *RemediationStore) ListByIncident(ctx context.Context, incidentID string) ([]RemediationAction, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	rows, err := s.pool.Query(ctx, listActionsSQL, incidentID)
	if err != nil {
		s.metrics.Record(ctx, "list_remediations", "error", time.Since(start))
		return nil, fmt.Errorf("list remediation actions for incident %s: %w", incidentID, err)
	}
	defer rows.Close()

	var actions []RemediationAction
	for rows.Next() {
		a, err := scanRemediationAction(rows)
		if err != nil {
			s.metrics.Record(ctx, "list_remediations", "error", time.Since(start))
			return nil, fmt.Errorf("scan remediation action: %w", err)
		}
		actions = append(actions, a)
	}
	if err := rows.Err(); err != nil {
		s.metrics.Record(ctx, "list_remediations", "error", time.Since(start))
		return nil, fmt.Errorf("list remediation actions for incident %s: %w", incidentID, err)
	}

	s.metrics.Record(ctx, "list_remediations", "ok", time.Since(start))
	return actions, nil
}

// Pending returns the step awaiting a decision, or ErrNoPendingAction.
func (s *RemediationStore) Pending(ctx context.Context, incidentID string) (RemediationAction, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	a, err := scanRemediationAction(s.pool.QueryRow(ctx, pendingActionSQL, incidentID))
	elapsed := time.Since(start)

	if errors.Is(err, pgx.ErrNoRows) {
		s.metrics.Record(ctx, "pending_remediation", "not_found", elapsed)
		return RemediationAction{}, ErrNoPendingAction
	}
	if err != nil {
		s.metrics.Record(ctx, "pending_remediation", "error", elapsed)
		return RemediationAction{}, fmt.Errorf("read pending remediation action for incident %s: %w", incidentID, err)
	}

	s.metrics.Record(ctx, "pending_remediation", "ok", elapsed)
	return a, nil
}

func scanRemediationAction(row rowScanner) (RemediationAction, error) {
	var (
		a           RemediationAction
		detailBytes []byte
	)

	if err := row.Scan(
		&a.ID, &a.IncidentID, &a.RunbookID, &a.StepIndex, &a.StepName,
		&a.ActionKind, &a.Mode, &a.Status, &a.Actor, &detailBytes,
		&a.CreatedAt, &a.UpdatedAt,
	); err != nil {
		return RemediationAction{}, err
	}

	if len(detailBytes) > 0 {
		if err := json.Unmarshal(detailBytes, &a.Detail); err != nil {
			return RemediationAction{}, fmt.Errorf("decode remediation %s detail: %w", a.ID, err)
		}
	}
	return a, nil
}
