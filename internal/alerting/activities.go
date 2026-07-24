package alerting

import (
	"context"
	"errors"
	"log/slog"

	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

// IncidentStatusStore reads an incident's current status. *store.IncidentStore
// satisfies it; the interface keeps the activities testable without a database.
type IncidentStatusStore interface {
	Status(ctx context.Context, id string) (incident.Status, error)
}

// NotificationRecorder persists a notification. *store.NotificationStore
// satisfies it.
type NotificationRecorder interface {
	Record(ctx context.Context, n store.Notification) error
}

// SendNotificationArgs is what the workflow hands the SendNotification activity.
type SendNotificationArgs struct {
	NotificationID string
	IncidentID     string
	Level          int
	Target         string
	Contact        string
	ContactAddress string
	Title          string
	Severity       string
	Reason         string
}

// Activities holds the non-deterministic dependencies the workflow reaches
// through: the database and the notifier. It is registered on the worker.
type Activities struct {
	incidents IncidentStatusStore
	notifier  *Notifier
	log       *slog.Logger
}

// NewActivities builds the activity set.
func NewActivities(incidents IncidentStatusStore, notifier *Notifier, log *slog.Logger) *Activities {
	return &Activities{incidents: incidents, notifier: notifier, log: log}
}

// CheckIncidentStatus returns the incident's current status as a string.
//
// A missing incident is reported as resolved: it can no longer escalate, and
// treating "gone" as "done" stops the workflow cleanly instead of erroring and
// retrying forever.
func (a *Activities) CheckIncidentStatus(ctx context.Context, incidentID string) (string, error) {
	status, err := a.incidents.Status(ctx, incidentID)
	if err != nil {
		if errors.Is(err, store.ErrIncidentNotFound) {
			return string(incident.StatusResolved), nil
		}
		return "", err
	}
	return string(status), nil
}

// SendNotification delivers and records one notification.
//
// Only a failure to record (a database problem) fails the activity, which
// Temporal then retries; a delivery failure is captured in the recorded row so
// the audit trail is written exactly once and escalation proceeds on schedule.
func (a *Activities) SendNotification(ctx context.Context, args SendNotificationArgs) error {
	return a.notifier.Dispatch(ctx, args)
}
