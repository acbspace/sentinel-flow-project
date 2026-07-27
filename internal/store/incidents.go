package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
)

// ErrIncidentNotFound is returned when no incident matches the given id.
var ErrIncidentNotFound = errors.New("incident not found")

// InvalidTransitionError reports that a lifecycle change was rejected because
// the incident is not in a state the transition is allowed from. The store
// surfaces this so the API can answer 409 rather than a generic 500.
type InvalidTransitionError struct {
	ID   string
	From incident.Status
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("incident %s cannot transition from status %q", e.ID, e.From)
}

// incidentColumns is the canonical column order shared by every read path and
// the transition RETURNING clauses, so scanIncident can serve all of them.
const incidentColumns = `id, fingerprint, tenant_id, service_name, rule_id, title,
	severity, status, event_count, first_seen_at, last_seen_at, opened_at,
	acknowledged_at, resolved_at, updated_at, details`

// upsertIncidentSQL opens a new incident or groups a repeat detection into the
// existing active one.
//
// The conflict target is the partial unique index (fingerprint) WHERE status <>
// 'resolved', so the UPSERT collides only with a still-active incident: a
// resolved one is invisible to it and a recurrence therefore opens a fresh row.
// severity and title are not updated on conflict because the fingerprint
// includes the rule id, so they cannot change within one active incident.
// RETURNING (xmax = 0) reports whether this call inserted (true) or grouped
// (false): xmax is zero only on a freshly inserted row.
//
// The two event counts are deliberately different values. Opening uses $8, the
// whole window's tally, because that is the evidence the incident opened on.
// Grouping adds $12, only the events new since the previous cycle: consecutive
// windows overlap, so adding EXCLUDED.event_count here would count every event
// once per cycle it remains inside the window.
const upsertIncidentSQL = `
INSERT INTO incidents (
    id, fingerprint, tenant_id, service_name, rule_id, title, severity, status,
    event_count, first_seen_at, last_seen_at, opened_at, updated_at, details
) VALUES ($1, $2, $3, $4, $5, $6, $7, 'open', $8, $9, $10, now(), now(), $11)
ON CONFLICT (fingerprint) WHERE status <> 'resolved'
DO UPDATE SET
    last_seen_at = GREATEST(incidents.last_seen_at, EXCLUDED.last_seen_at),
    event_count  = incidents.event_count + $12,
    updated_at   = now()
RETURNING (xmax = 0) AS inserted`

const getIncidentSQL = `SELECT ` + incidentColumns + ` FROM incidents WHERE id = $1`

const currentStatusSQL = `SELECT status FROM incidents WHERE id = $1`

// acknowledgeIncidentSQL and resolveIncidentSQL guard the transition in the
// WHERE clause: the update matches no row when the incident is already past the
// state it may transition from, which the caller turns into a 404 or 409.
const acknowledgeIncidentSQL = `
UPDATE incidents
SET status = 'acknowledged', acknowledged_at = now(), updated_at = now()
WHERE id = $1 AND status = 'open'
RETURNING ` + incidentColumns

const resolveIncidentSQL = `
UPDATE incidents
SET status = 'resolved', resolved_at = now(), updated_at = now()
WHERE id = $1 AND status <> 'resolved'
RETURNING ` + incidentColumns

// autoResolveStaleSQL closes every active incident whose most recent detection
// is older than the cutoff. This is the auto-resolution the correlation engine
// runs each tick so that a condition which has stopped firing does not stay open
// forever waiting for a human.
const autoResolveStaleSQL = `
UPDATE incidents
SET status = 'resolved', resolved_at = now(), updated_at = now()
WHERE status <> 'resolved' AND last_seen_at < $1`

// listOpenUnalertedSQL feeds the alerting starter: open incidents that have not
// yet had an alert workflow kicked off (alerted_at IS NULL), oldest first.
const listOpenUnalertedSQL = `SELECT ` + incidentColumns + `
FROM incidents
WHERE status = 'open' AND alerted_at IS NULL
ORDER BY opened_at
LIMIT $1`

// markAlertedSQL stamps an incident once its alert workflow has been started. The
// alerted_at IS NULL guard makes it a no-op on a second pass.
const markAlertedSQL = `UPDATE incidents SET alerted_at = now() WHERE id = $1 AND alerted_at IS NULL`

// listOpenUnremediatedSQL and markRemediatedSQL are the remediation starter's
// equivalents of the two above.
const listOpenUnremediatedSQL = `SELECT ` + incidentColumns + `
FROM incidents
WHERE status = 'open' AND remediated_at IS NULL
ORDER BY opened_at
LIMIT $1`

const markRemediatedSQL = `UPDATE incidents SET remediated_at = now() WHERE id = $1 AND remediated_at IS NULL`

// IncidentFilter selects and paginates a slice of incidents. A zero-valued field
// is not filtered on; validation of the values themselves belongs to the caller.
//
// After and Offset are alternative ways to page. After is the one to use; Offset
// remains for callers written against the older API.
type IncidentFilter struct {
	Status      string
	TenantID    string
	ServiceName string
	Severity    string
	Limit       int
	Offset      int
	After       Cursor
}

// IncidentStore persists and queries incidents.
type IncidentStore struct {
	pool    *pgxpool.Pool
	metrics *obs.DBMetrics
	timeout time.Duration
}

// NewIncidentStore builds an incident store over an existing pool.
func NewIncidentStore(pool *pgxpool.Pool, metrics *obs.DBMetrics, timeout time.Duration) *IncidentStore {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &IncidentStore{pool: pool, metrics: metrics, timeout: timeout}
}

// UpsertOpen opens the incident or groups it into the active one sharing its
// fingerprint, and reports which happened: true means a new incident was opened,
// false means an existing one absorbed this detection.
//
// inc.EventCount seeds a newly opened incident; groupedIncrement is what an
// already-open one advances by. They differ because correlation windows overlap
// — see upsertIncidentSQL.
func (s *IncidentStore) UpsertOpen(ctx context.Context, inc incident.Incident, groupedIncrement int64) (bool, error) {
	if err := inc.Validate(); err != nil {
		s.metrics.Record(ctx, "upsert_incident", "error", 0)
		return false, err
	}

	details, err := json.Marshal(inc.Details)
	if err != nil {
		// Non-retryable: the same details will fail to encode every time.
		s.metrics.Record(ctx, "upsert_incident", "error", 0)
		return false, fmt.Errorf("encode details for incident %s: %w", inc.Fingerprint, err)
	}
	if inc.Details == nil {
		details = []byte("{}")
	}

	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	var inserted bool
	err = s.pool.QueryRow(ctx, upsertIncidentSQL,
		inc.ID,
		inc.Fingerprint,
		inc.TenantID,
		inc.ServiceName,
		inc.RuleID,
		inc.Title,
		string(inc.Severity),
		inc.EventCount,
		inc.FirstSeenAt,
		inc.LastSeenAt,
		details,
		groupedIncrement,
	).Scan(&inserted)
	elapsed := time.Since(start)

	if err != nil {
		s.metrics.Record(ctx, "upsert_incident", "error", elapsed)
		return false, fmt.Errorf("upsert incident %s: %w", inc.Fingerprint, err)
	}

	outcome := "grouped"
	if inserted {
		outcome = "opened"
	}
	s.metrics.Record(ctx, "upsert_incident", outcome, elapsed)
	return inserted, nil
}

// List returns the incidents matching filter, newest activity first, and the
// cursor for the next page. The cursor is empty when this page is the last.
func (s *IncidentStore) List(ctx context.Context, filter IncidentFilter) ([]incident.Incident, Cursor, error) {
	query, args, limit := buildIncidentListQuery(filter)

	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		s.metrics.Record(ctx, "list_incidents", "error", time.Since(start))
		return nil, Cursor{}, fmt.Errorf("list incidents: %w", err)
	}
	defer rows.Close()

	var incidents []incident.Incident
	for rows.Next() {
		inc, err := scanIncident(rows)
		if err != nil {
			s.metrics.Record(ctx, "list_incidents", "error", time.Since(start))
			return nil, Cursor{}, fmt.Errorf("scan incident: %w", err)
		}
		incidents = append(incidents, inc)
	}
	if err := rows.Err(); err != nil {
		s.metrics.Record(ctx, "list_incidents", "error", time.Since(start))
		return nil, Cursor{}, fmt.Errorf("list incidents: %w", err)
	}

	var next Cursor
	if len(incidents) > limit {
		incidents = incidents[:limit]
		last := incidents[len(incidents)-1]
		next = Cursor{Time: last.LastSeenAt, ID: last.ID}
	}

	s.metrics.Record(ctx, "list_incidents", "ok", time.Since(start))
	return incidents, next, nil
}

// Get returns one incident by id, or ErrIncidentNotFound.
func (s *IncidentStore) Get(ctx context.Context, id string) (incident.Incident, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	inc, err := scanIncident(s.pool.QueryRow(ctx, getIncidentSQL, id))
	elapsed := time.Since(start)

	if errors.Is(err, pgx.ErrNoRows) {
		s.metrics.Record(ctx, "get_incident", "not_found", elapsed)
		return incident.Incident{}, ErrIncidentNotFound
	}
	if err != nil {
		s.metrics.Record(ctx, "get_incident", "error", elapsed)
		return incident.Incident{}, fmt.Errorf("get incident %s: %w", id, err)
	}

	s.metrics.Record(ctx, "get_incident", "ok", elapsed)
	return inc, nil
}

// Acknowledge moves an open incident to acknowledged. It returns
// ErrIncidentNotFound if no such incident exists, or an *InvalidTransitionError
// if the incident exists but is not open.
func (s *IncidentStore) Acknowledge(ctx context.Context, id string) (incident.Incident, error) {
	return s.transition(ctx, "acknowledge_incident", acknowledgeIncidentSQL, id)
}

// Resolve moves an open or acknowledged incident to resolved. It returns
// ErrIncidentNotFound if no such incident exists, or an *InvalidTransitionError
// if it is already resolved.
func (s *IncidentStore) Resolve(ctx context.Context, id string) (incident.Incident, error) {
	return s.transition(ctx, "resolve_incident", resolveIncidentSQL, id)
}

// AutoResolveStale resolves every active incident whose last detection predates
// olderThan, and reports how many it closed.
func (s *IncidentStore) AutoResolveStale(ctx context.Context, olderThan time.Time) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	tag, err := s.pool.Exec(ctx, autoResolveStaleSQL, olderThan)
	elapsed := time.Since(start)

	if err != nil {
		s.metrics.Record(ctx, "auto_resolve_incidents", "error", elapsed)
		return 0, fmt.Errorf("auto-resolve stale incidents: %w", err)
	}

	s.metrics.Record(ctx, "auto_resolve_incidents", "ok", elapsed)
	return tag.RowsAffected(), nil
}

// ListOpenUnalerted returns up to limit open incidents that have not yet had an
// alert workflow started, oldest first.
func (s *IncidentStore) ListOpenUnalerted(ctx context.Context, limit int) ([]incident.Incident, error) {
	return s.listOpenPending(ctx, "list_open_unalerted", listOpenUnalertedSQL, limit)
}

// ListOpenUnremediated returns up to limit open incidents that have not yet had a
// remediation workflow started, oldest first.
func (s *IncidentStore) ListOpenUnremediated(ctx context.Context, limit int) ([]incident.Incident, error) {
	return s.listOpenPending(ctx, "list_open_unremediated", listOpenUnremediatedSQL, limit)
}

// listOpenPending runs one of the "open incidents still needing X" queries. The
// two starters differ only in which marker column they filter on.
func (s *IncidentStore) listOpenPending(ctx context.Context, op, query string, limit int) ([]incident.Incident, error) {
	if limit <= 0 {
		limit = defaultListLimit
	}

	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	rows, err := s.pool.Query(ctx, query, limit)
	if err != nil {
		s.metrics.Record(ctx, op, "error", time.Since(start))
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	defer rows.Close()

	var incidents []incident.Incident
	for rows.Next() {
		inc, err := scanIncident(rows)
		if err != nil {
			s.metrics.Record(ctx, op, "error", time.Since(start))
			return nil, fmt.Errorf("scan incident: %w", err)
		}
		incidents = append(incidents, inc)
	}
	if err := rows.Err(); err != nil {
		s.metrics.Record(ctx, op, "error", time.Since(start))
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	s.metrics.Record(ctx, op, "ok", time.Since(start))
	return incidents, nil
}

// MarkAlerted records that an alert workflow has been started for the incident.
// It is a no-op if one was already recorded, so repeated poller passes are safe.
func (s *IncidentStore) MarkAlerted(ctx context.Context, id string) error {
	return s.mark(ctx, "mark_alerted", markAlertedSQL, id)
}

// MarkRemediated records that a remediation workflow has been started.
func (s *IncidentStore) MarkRemediated(ctx context.Context, id string) error {
	return s.mark(ctx, "mark_remediated", markRemediatedSQL, id)
}

func (s *IncidentStore) mark(ctx context.Context, op, query, id string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	_, err := s.pool.Exec(ctx, query, id)
	elapsed := time.Since(start)

	if err != nil {
		s.metrics.Record(ctx, op, "error", elapsed)
		return fmt.Errorf("%s for incident %s: %w", op, id, err)
	}

	s.metrics.Record(ctx, op, "ok", elapsed)
	return nil
}

// transition runs a guarded lifecycle update. When the update matches a row it
// returns the updated incident; when it matches none it distinguishes a missing
// incident from an illegal transition so the caller can choose the right status
// code.
func (s *IncidentStore) transition(ctx context.Context, op, query, id string) (incident.Incident, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	inc, err := scanIncident(s.pool.QueryRow(ctx, query, id))
	elapsed := time.Since(start)

	if err == nil {
		s.metrics.Record(ctx, op, "ok", elapsed)
		return inc, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		s.metrics.Record(ctx, op, "error", elapsed)
		return incident.Incident{}, fmt.Errorf("%s %s: %w", op, id, err)
	}

	// The guarded update changed nothing: the incident is either absent or in a
	// state this transition is not allowed from. One cheap lookup tells them apart.
	status, statusErr := s.currentStatus(ctx, id)
	if errors.Is(statusErr, ErrIncidentNotFound) {
		s.metrics.Record(ctx, op, "not_found", elapsed)
		return incident.Incident{}, ErrIncidentNotFound
	}
	if statusErr != nil {
		s.metrics.Record(ctx, op, "error", elapsed)
		return incident.Incident{}, statusErr
	}

	s.metrics.Record(ctx, op, "invalid_transition", elapsed)
	return incident.Incident{}, &InvalidTransitionError{ID: id, From: status}
}

// Status returns just an incident's current status, or ErrIncidentNotFound. The
// alert workflow uses it to re-check the database as the authority before each
// escalation, so a lost signal still converges to the right behaviour.
func (s *IncidentStore) Status(ctx context.Context, id string) (incident.Status, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	status, err := s.currentStatus(ctx, id)
	elapsed := time.Since(start)

	if errors.Is(err, ErrIncidentNotFound) {
		s.metrics.Record(ctx, "incident_status", "not_found", elapsed)
		return "", err
	}
	if err != nil {
		s.metrics.Record(ctx, "incident_status", "error", elapsed)
		return "", err
	}
	s.metrics.Record(ctx, "incident_status", "ok", elapsed)
	return status, nil
}

// currentStatus reads just the status column, used to explain why a guarded
// transition matched no row.
func (s *IncidentStore) currentStatus(ctx context.Context, id string) (incident.Status, error) {
	var status string
	err := s.pool.QueryRow(ctx, currentStatusSQL, id).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrIncidentNotFound
	}
	if err != nil {
		return "", fmt.Errorf("read incident %s status: %w", id, err)
	}
	return incident.Status(status), nil
}

// buildIncidentListQuery renders the SELECT for List. It is a pure function so
// the placeholder numbering and clause composition can be unit tested without a
// database.
func buildIncidentListQuery(f IncidentFilter) (string, []any, int) {
	var b filterBuilder
	if f.Status != "" {
		b.add("status = $%d", f.Status)
	}
	if f.TenantID != "" {
		b.add("tenant_id = $%d", f.TenantID)
	}
	if f.ServiceName != "" {
		b.add("service_name = $%d", f.ServiceName)
	}
	if f.Severity != "" {
		b.add("severity = $%d", f.Severity)
	}
	b.after("last_seen_at", "id", f.After)

	page, limit := b.paginate(f.Limit, f.Offset)

	query := "SELECT " + incidentColumns + " FROM incidents" + b.where() +
		" ORDER BY last_seen_at DESC, id DESC" + page
	return query, b.args, limit
}

// scanIncident reads one incident row in incidentColumns order. It is written
// against rowScanner so it serves both QueryRow and an iterated Rows.
func scanIncident(row rowScanner) (incident.Incident, error) {
	var (
		inc          incident.Incident
		severity     string
		status       string
		detailsBytes []byte
	)

	if err := row.Scan(
		&inc.ID,
		&inc.Fingerprint,
		&inc.TenantID,
		&inc.ServiceName,
		&inc.RuleID,
		&inc.Title,
		&severity,
		&status,
		&inc.EventCount,
		&inc.FirstSeenAt,
		&inc.LastSeenAt,
		&inc.OpenedAt,
		&inc.AcknowledgedAt,
		&inc.ResolvedAt,
		&inc.UpdatedAt,
		&detailsBytes,
	); err != nil {
		return incident.Incident{}, err
	}

	inc.Severity = event.Severity(severity)
	inc.Status = incident.Status(status)
	if len(detailsBytes) > 0 {
		if err := json.Unmarshal(detailsBytes, &inc.Details); err != nil {
			return incident.Incident{}, fmt.Errorf("decode incident %s details: %w", inc.ID, err)
		}
	}
	return inc, nil
}
