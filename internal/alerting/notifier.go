package alerting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/store"
)

const (
	channelLog     = "log"
	channelWebhook = "webhook"
	statusSent     = "sent"
	statusFailed   = "failed"
)

// Notifier delivers a notification and records it to the audit trail. Delivery is
// always a structured log line, plus an optional webhook POST when a URL is
// configured (globally, or per contact). Real Slack/email/PagerDuty integrations
// are out of scope here; the webhook is the seam for them.
type Notifier struct {
	recorder   NotificationRecorder
	webhookURL string
	httpClient *http.Client
	log        *slog.Logger
}

// NewNotifier builds a notifier. An empty webhookURL means log-only delivery.
func NewNotifier(recorder NotificationRecorder, webhookURL string, timeout time.Duration, log *slog.Logger) *Notifier {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Notifier{
		recorder:   recorder,
		webhookURL: webhookURL,
		httpClient: &http.Client{Timeout: timeout},
		log:        log,
	}
}

// Dispatch delivers one notification and records it.
//
// A webhook failure is recorded as status=failed but does not return an error:
// the dispatch decision is still faithfully in the audit trail, and escalation
// should not stall on a flaky webhook. Only a failure to record the row is
// returned, so Temporal retries the (idempotent) write.
func (n *Notifier) Dispatch(ctx context.Context, args SendNotificationArgs) error {
	channel := channelLog
	status := statusSent
	detail := map[string]any{
		"title":    args.Title,
		"severity": args.Severity,
		"reason":   args.Reason,
	}

	if url := n.webhookTarget(args); url != "" {
		channel = channelWebhook
		if err := n.postWebhook(ctx, url, args); err != nil {
			status = statusFailed
			detail["error"] = err.Error()
			n.log.WarnContext(ctx, "notification webhook delivery failed",
				slog.String("incident_id", args.IncidentID),
				slog.Int("level", args.Level),
				slog.String("error", err.Error()),
			)
		}
	}

	n.log.InfoContext(ctx, "notification dispatched",
		slog.String("incident_id", args.IncidentID),
		slog.Int("level", args.Level),
		slog.String("target", args.Target),
		slog.String("contact", args.Contact),
		slog.String("channel", channel),
		slog.String("status", status),
		slog.String("reason", args.Reason),
	)

	return n.recorder.Record(ctx, store.Notification{
		ID:         args.NotificationID,
		IncidentID: args.IncidentID,
		Level:      args.Level,
		Target:     args.Target,
		Contact:    args.Contact,
		Channel:    channel,
		Status:     status,
		Detail:     detail,
	})
}

// webhookTarget prefers a per-contact address, falling back to the global URL.
func (n *Notifier) webhookTarget(args SendNotificationArgs) string {
	if args.ContactAddress != "" {
		return args.ContactAddress
	}
	return n.webhookURL
}

func (n *Notifier) postWebhook(ctx context.Context, url string, args SendNotificationArgs) error {
	payload, err := json.Marshal(map[string]any{
		"incident_id": args.IncidentID,
		"level":       args.Level,
		"target":      args.Target,
		"contact":     args.Contact,
		"title":       args.Title,
		"severity":    args.Severity,
		"reason":      args.Reason,
	})
	if err != nil {
		return fmt.Errorf("encode webhook payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("post webhook: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}
	return nil
}
