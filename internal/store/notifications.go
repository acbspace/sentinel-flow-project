package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acbspace/sentinel-flow-project/internal/obs"
)

// Notification is one dispatch the alert workflow made for an incident.
type Notification struct {
	ID         string         `json:"id"`
	IncidentID string         `json:"incident_id"`
	Level      int            `json:"level"`
	Target     string         `json:"target"`
	Contact    string         `json:"contact"`
	Channel    string         `json:"channel"`
	Status     string         `json:"status"`
	Detail     map[string]any `json:"detail,omitempty"`
	SentAt     time.Time      `json:"sent_at"`
}

// recordNotificationSQL is idempotent on the workflow-supplied id, so an activity
// retry (Temporal will retry a failed activity) records the dispatch once rather
// than once per attempt.
const recordNotificationSQL = `
INSERT INTO notifications (id, incident_id, level, target, contact, channel, status, detail)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (id) DO NOTHING`

const listNotificationsSQL = `
SELECT id, incident_id, level, target, contact, channel, status, detail, sent_at
FROM notifications
WHERE incident_id = $1
ORDER BY sent_at, level`

// NotificationStore records and reads the alerting audit trail.
type NotificationStore struct {
	pool    *pgxpool.Pool
	metrics *obs.DBMetrics
	timeout time.Duration
}

// NewNotificationStore builds a notification store over an existing pool.
func NewNotificationStore(pool *pgxpool.Pool, metrics *obs.DBMetrics, timeout time.Duration) *NotificationStore {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &NotificationStore{pool: pool, metrics: metrics, timeout: timeout}
}

// Record writes one notification. It is idempotent on the id.
func (s *NotificationStore) Record(ctx context.Context, n Notification) error {
	detail, err := json.Marshal(n.Detail)
	if err != nil {
		s.metrics.Record(ctx, "record_notification", "error", 0)
		return fmt.Errorf("encode notification %s detail: %w", n.ID, err)
	}
	if n.Detail == nil {
		detail = []byte("{}")
	}

	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	_, err = s.pool.Exec(ctx, recordNotificationSQL,
		n.ID, n.IncidentID, n.Level, n.Target, n.Contact, n.Channel, n.Status, detail,
	)
	elapsed := time.Since(start)

	if err != nil {
		s.metrics.Record(ctx, "record_notification", "error", elapsed)
		return fmt.Errorf("record notification %s: %w", n.ID, err)
	}

	s.metrics.Record(ctx, "record_notification", "ok", elapsed)
	return nil
}

// ListByIncident returns an incident's notifications in dispatch order.
func (s *NotificationStore) ListByIncident(ctx context.Context, incidentID string) ([]Notification, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	start := time.Now()
	rows, err := s.pool.Query(ctx, listNotificationsSQL, incidentID)
	if err != nil {
		s.metrics.Record(ctx, "list_notifications", "error", time.Since(start))
		return nil, fmt.Errorf("list notifications for incident %s: %w", incidentID, err)
	}
	defer rows.Close()

	var notifications []Notification
	for rows.Next() {
		var (
			n           Notification
			detailBytes []byte
		)
		if err := rows.Scan(
			&n.ID, &n.IncidentID, &n.Level, &n.Target, &n.Contact,
			&n.Channel, &n.Status, &detailBytes, &n.SentAt,
		); err != nil {
			s.metrics.Record(ctx, "list_notifications", "error", time.Since(start))
			return nil, fmt.Errorf("scan notification: %w", err)
		}
		if len(detailBytes) > 0 {
			if err := json.Unmarshal(detailBytes, &n.Detail); err != nil {
				s.metrics.Record(ctx, "list_notifications", "error", time.Since(start))
				return nil, fmt.Errorf("decode notification %s detail: %w", n.ID, err)
			}
		}
		notifications = append(notifications, n)
	}
	if err := rows.Err(); err != nil {
		s.metrics.Record(ctx, "list_notifications", "error", time.Since(start))
		return nil, fmt.Errorf("list notifications for incident %s: %w", incidentID, err)
	}

	s.metrics.Record(ctx, "list_notifications", "ok", time.Since(start))
	return notifications, nil
}
