// Package incidentapi implements the read and lifecycle HTTP surface over
// incidents and stored telemetry events.
//
// It is the read side of the platform: the ingestion API is a write-only front
// door, and this API is the query-and-act side an operator or a UI talks to. It
// owns no business logic beyond request parsing and status-code mapping; the
// lifecycle rules and dedup live in the store and the incident domain.
package incidentapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/httpx"
	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/remediate"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

// IncidentStore is the incident access the handler needs. *store.IncidentStore
// satisfies it; the interface exists so handler tests run without a database.
type IncidentStore interface {
	List(ctx context.Context, filter store.IncidentFilter) ([]incident.Incident, store.Cursor, error)
	Get(ctx context.Context, id string) (incident.Incident, error)
	Acknowledge(ctx context.Context, id string) (incident.Incident, error)
	Resolve(ctx context.Context, id string) (incident.Incident, error)
}

// EventStore is the stored-event access the handler needs. *store.EventStore
// satisfies it.
type EventStore interface {
	ListEvents(ctx context.Context, filter store.EventFilter) ([]store.StoredEvent, store.Cursor, error)
}

// NotificationStore reads an incident's alert timeline. *store.NotificationStore
// satisfies it.
type NotificationStore interface {
	ListByIncident(ctx context.Context, incidentID string) ([]store.Notification, error)
}

// Signaler tells an incident's alert workflow that its lifecycle changed, so
// escalation stops promptly. A no-op implementation is used when alerting is not
// wired; implementations must treat a missing workflow as success.
type Signaler interface {
	SignalAcknowledged(ctx context.Context, incidentID string) error
	SignalResolved(ctx context.Context, incidentID string) error
}

// noopSignaler is used when no Temporal address is configured: the read API still
// works, it just does not push signals to the (absent) alert workflows.
type noopSignaler struct{}

func (noopSignaler) SignalAcknowledged(context.Context, string) error { return nil }
func (noopSignaler) SignalResolved(context.Context, string) error     { return nil }

// RemediationStore reads an incident's automated-action audit trail.
// *store.RemediationStore satisfies it.
type RemediationStore interface {
	ListByIncident(ctx context.Context, incidentID string) ([]store.RemediationAction, error)
	Pending(ctx context.Context, incidentID string) (store.RemediationAction, error)
}

// Approver releases or stops the remediation step awaiting a decision.
// *remediate.TemporalSignaler satisfies it.
type Approver interface {
	Approve(ctx context.Context, incidentID, actor string) error
	Reject(ctx context.Context, incidentID, actor string) error
}

// Handler serves the incidents and events read/lifecycle routes.
type Handler struct {
	incidents     IncidentStore
	events        EventStore
	notifications NotificationStore
	remediation   RemediationStore
	signaler      Signaler
	approver      Approver
	log           *slog.Logger
}

// Options configures the handler.
type Options struct {
	Incidents     IncidentStore
	Events        EventStore
	Notifications NotificationStore
	Remediation   RemediationStore
	Signaler      Signaler
	Approver      Approver
	Logger        *slog.Logger
}

// NewHandler builds the handler. A nil Signaler becomes a no-op, so the read API
// runs whether or not alerting is wired.
func NewHandler(opts Options) *Handler {
	if opts.Signaler == nil {
		opts.Signaler = noopSignaler{}
	}
	return &Handler{
		incidents:     opts.Incidents,
		events:        opts.Events,
		notifications: opts.Notifications,
		remediation:   opts.Remediation,
		signaler:      opts.Signaler,
		approver:      opts.Approver,
		log:           opts.Logger,
	}
}

// Mount registers every route this handler serves on r under /v1.
func (h *Handler) Mount(r chi.Router) {
	r.Route("/v1", func(r chi.Router) {
		r.Get("/incidents", h.ListIncidents)
		r.Get("/incidents/{id}", h.GetIncident)
		r.Get("/incidents/{id}/notifications", h.ListNotifications)
		r.Get("/incidents/{id}/remediation", h.ListRemediation)
		r.Post("/incidents/{id}/remediation/approve", h.ApproveRemediation)
		r.Post("/incidents/{id}/remediation/reject", h.RejectRemediation)
		r.Post("/incidents/{id}/acknowledge", h.AcknowledgeIncident)
		r.Post("/incidents/{id}/resolve", h.ResolveIncident)
		r.Get("/events", h.ListEvents)
	})
}

// errorResponse is the single error shape this API returns, matching the
// ingestion API's shape so callers see one convention across services.
type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// NextCursor is the token to pass back as ?cursor= for the following page. It is
// omitted when this page is the last, so its absence is the end-of-list signal.
type incidentListResponse struct {
	Incidents  []incident.Incident `json:"incidents"`
	Count      int                 `json:"count"`
	NextCursor string              `json:"next_cursor,omitempty"`
}

type eventListResponse struct {
	Events     []store.StoredEvent `json:"events"`
	Count      int                 `json:"count"`
	NextCursor string              `json:"next_cursor,omitempty"`
}

type notificationListResponse struct {
	Notifications []store.Notification `json:"notifications"`
	Count         int                  `json:"count"`
}

type remediationListResponse struct {
	Actions []store.RemediationAction `json:"actions"`
	Count   int                       `json:"count"`
}

type remediationDecisionResponse struct {
	Status     string `json:"status"`
	IncidentID string `json:"incident_id"`
	StepIndex  int    `json:"step_index"`
	StepName   string `json:"step_name"`
	Actor      string `json:"actor,omitempty"`
}

// ListIncidents serves GET /v1/incidents.
func (h *Handler) ListIncidents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	filter, err := parseIncidentFilter(r)
	if err != nil {
		h.writeError(ctx, w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}

	incidents, next, err := h.incidents.List(ctx, filter)
	if err != nil {
		h.log.ErrorContext(ctx, "list incidents failed", slog.String("error", err.Error()))
		h.writeError(ctx, w, http.StatusInternalServerError, "internal_error", "could not list incidents")
		return
	}
	if incidents == nil {
		incidents = []incident.Incident{}
	}

	httpx.WriteJSON(ctx, w, http.StatusOK, incidentListResponse{
		Incidents:  incidents,
		Count:      len(incidents),
		NextCursor: next.Encode(),
	}, h.log)
}

// GetIncident serves GET /v1/incidents/{id}.
func (h *Handler) GetIncident(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, ok := h.incidentID(ctx, w, r)
	if !ok {
		return
	}

	inc, err := h.incidents.Get(ctx, id)
	if err != nil {
		h.writeIncidentError(ctx, w, "get", id, err)
		return
	}

	httpx.WriteJSON(ctx, w, http.StatusOK, inc, h.log)
}

// AcknowledgeIncident serves POST /v1/incidents/{id}/acknowledge.
func (h *Handler) AcknowledgeIncident(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, ok := h.incidentID(ctx, w, r)
	if !ok {
		return
	}

	inc, err := h.incidents.Acknowledge(ctx, id)
	if err != nil {
		h.writeIncidentError(ctx, w, "acknowledge", id, err)
		return
	}

	// The database transition is authoritative; signalling the workflow just lets
	// it stop escalating sooner, so a signal failure is logged, not returned.
	h.signalBestEffort(ctx, id, "acknowledge", h.signaler.SignalAcknowledged)

	httpx.WriteJSON(ctx, w, http.StatusOK, inc, h.log)
}

// ResolveIncident serves POST /v1/incidents/{id}/resolve.
func (h *Handler) ResolveIncident(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, ok := h.incidentID(ctx, w, r)
	if !ok {
		return
	}

	inc, err := h.incidents.Resolve(ctx, id)
	if err != nil {
		h.writeIncidentError(ctx, w, "resolve", id, err)
		return
	}

	h.signalBestEffort(ctx, id, "resolve", h.signaler.SignalResolved)

	httpx.WriteJSON(ctx, w, http.StatusOK, inc, h.log)
}

// signalBestEffort pushes a lifecycle signal to the alert workflow and only logs
// on failure: the incident's state is already committed, and the workflow's own
// database re-check will catch up even if the signal is lost.
func (h *Handler) signalBestEffort(ctx context.Context, id, action string, signal func(context.Context, string) error) {
	if err := signal(ctx, id); err != nil {
		h.log.WarnContext(ctx, "signal alert workflow failed",
			slog.String("incident_id", id),
			slog.String("action", action),
			slog.String("error", err.Error()),
		)
	}
}

// ListNotifications serves GET /v1/incidents/{id}/notifications.
func (h *Handler) ListNotifications(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, ok := h.incidentID(ctx, w, r)
	if !ok {
		return
	}

	notes, err := h.notifications.ListByIncident(ctx, id)
	if err != nil {
		h.log.ErrorContext(ctx, "list notifications failed",
			slog.String("incident_id", id), slog.String("error", err.Error()))
		h.writeError(ctx, w, http.StatusInternalServerError, "internal_error", "could not list notifications")
		return
	}
	if notes == nil {
		notes = []store.Notification{}
	}

	httpx.WriteJSON(ctx, w, http.StatusOK, notificationListResponse{Notifications: notes, Count: len(notes)}, h.log)
}

// ListRemediation serves GET /v1/incidents/{id}/remediation — every automated
// action taken (or refused) for the incident, in runbook order.
func (h *Handler) ListRemediation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, ok := h.incidentID(ctx, w, r)
	if !ok || !h.remediationConfigured(ctx, w) {
		return
	}

	actions, err := h.remediation.ListByIncident(ctx, id)
	if err != nil {
		h.log.ErrorContext(ctx, "list remediation actions failed",
			slog.String("incident_id", id), slog.String("error", err.Error()))
		h.writeError(ctx, w, http.StatusInternalServerError, "internal_error", "could not list remediation actions")
		return
	}
	if actions == nil {
		actions = []store.RemediationAction{}
	}

	httpx.WriteJSON(ctx, w, http.StatusOK, remediationListResponse{Actions: actions, Count: len(actions)}, h.log)
}

// ApproveRemediation serves POST /v1/incidents/{id}/remediation/approve.
func (h *Handler) ApproveRemediation(w http.ResponseWriter, r *http.Request) {
	h.decideRemediation(w, r, "approved", func(ctx context.Context, id, actor string) error {
		return h.approver.Approve(ctx, id, actor)
	})
}

// RejectRemediation serves POST /v1/incidents/{id}/remediation/reject.
func (h *Handler) RejectRemediation(w http.ResponseWriter, r *http.Request) {
	h.decideRemediation(w, r, "rejected", func(ctx context.Context, id, actor string) error {
		return h.approver.Reject(ctx, id, actor)
	})
}

// decideRemediation delivers an approve/reject decision to the remediation
// workflow.
//
// It answers 202, not 200: the decision is delivered to a workflow that applies
// it asynchronously, so claiming the step is already done would be a lie. The
// resulting status shows up in the audit trail.
func (h *Handler) decideRemediation(w http.ResponseWriter, r *http.Request, decision string, deliver func(context.Context, string, string) error) {
	ctx := r.Context()

	id, ok := h.incidentID(ctx, w, r)
	if !ok || !h.remediationConfigured(ctx, w) {
		return
	}

	// Nothing to decide on is a conflict, not a 404: the incident exists, it just
	// has no step waiting.
	pending, err := h.remediation.Pending(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNoPendingAction) {
			h.writeError(ctx, w, http.StatusConflict, "no_pending_action",
				"no remediation step is awaiting a decision for this incident")
			return
		}
		h.log.ErrorContext(ctx, "read pending remediation action failed",
			slog.String("incident_id", id), slog.String("error", err.Error()))
		h.writeError(ctx, w, http.StatusInternalServerError, "internal_error", "could not read the pending remediation action")
		return
	}

	actor := r.URL.Query().Get("actor")
	if err := deliver(ctx, id, actor); err != nil {
		// The workflow finished between our read and our signal.
		if errors.Is(err, remediate.ErrNoRemediationRun) {
			h.writeError(ctx, w, http.StatusConflict, "no_pending_action",
				"the remediation run for this incident is no longer in progress")
			return
		}
		h.log.ErrorContext(ctx, "deliver remediation decision failed",
			slog.String("incident_id", id),
			slog.String("decision", decision),
			slog.String("error", err.Error()))
		h.writeError(ctx, w, http.StatusInternalServerError, "internal_error", "could not deliver the decision")
		return
	}

	h.log.InfoContext(ctx, "remediation decision delivered",
		slog.String("incident_id", id),
		slog.String("decision", decision),
		slog.String("actor", actor),
		slog.Int("step_index", pending.StepIndex),
	)

	httpx.WriteJSON(ctx, w, http.StatusAccepted, remediationDecisionResponse{
		Status:     decision,
		IncidentID: id,
		StepIndex:  pending.StepIndex,
		StepName:   pending.StepName,
		Actor:      actor,
	}, h.log)
}

// remediationConfigured reports whether remediation is wired, writing a 503 if
// not, so a deployment running without the remediation service gives a clear
// answer instead of a panic.
func (h *Handler) remediationConfigured(ctx context.Context, w http.ResponseWriter) bool {
	if h.remediation == nil {
		h.writeError(ctx, w, http.StatusServiceUnavailable, "remediation_unavailable",
			"remediation is not configured on this deployment")
		return false
	}
	return true
}

// ListEvents serves GET /v1/events.
func (h *Handler) ListEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	filter, err := parseEventFilter(r)
	if err != nil {
		h.writeError(ctx, w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}

	events, next, err := h.events.ListEvents(ctx, filter)
	if err != nil {
		h.log.ErrorContext(ctx, "list events failed", slog.String("error", err.Error()))
		h.writeError(ctx, w, http.StatusInternalServerError, "internal_error", "could not list events")
		return
	}
	if events == nil {
		events = []store.StoredEvent{}
	}

	httpx.WriteJSON(ctx, w, http.StatusOK, eventListResponse{
		Events:     events,
		Count:      len(events),
		NextCursor: next.Encode(),
	}, h.log)
}

// incidentID extracts and validates the {id} path parameter, writing a 400 and
// returning false if it is not a UUID. Validating here turns a malformed id into
// a clean 400 rather than a 500 from a failed cast deep in the database.
func (h *Handler) incidentID(ctx context.Context, w http.ResponseWriter, r *http.Request) (string, bool) {
	id := chi.URLParam(r, "id")
	if _, err := uuid.Parse(id); err != nil {
		h.writeError(ctx, w, http.StatusBadRequest, "invalid_id", "incident id must be a UUID")
		return "", false
	}
	return id, true
}

// writeIncidentError maps a store error onto the right status code: a missing
// incident is 404, an illegal lifecycle transition is 409, anything else is 500.
func (h *Handler) writeIncidentError(ctx context.Context, w http.ResponseWriter, op, id string, err error) {
	switch {
	case errors.Is(err, store.ErrIncidentNotFound):
		h.writeError(ctx, w, http.StatusNotFound, "not_found", "no incident with that id")
	default:
		var transition *store.InvalidTransitionError
		if errors.As(err, &transition) {
			h.writeError(ctx, w, http.StatusConflict, "invalid_transition", transition.Error())
			return
		}
		h.log.ErrorContext(ctx, op+" incident failed",
			slog.String("incident_id", id),
			slog.String("error", err.Error()),
		)
		h.writeError(ctx, w, http.StatusInternalServerError, "internal_error", "could not "+op+" incident")
	}
}

func (h *Handler) writeError(ctx context.Context, w http.ResponseWriter, status int, code, message string) {
	httpx.WriteJSON(ctx, w, status, errorResponse{Error: code, Message: message}, h.log)
}

// parseIncidentFilter builds a store filter from the query string, rejecting
// values outside the closed vocabularies so a typo becomes a 400 rather than a
// silently empty result.
func parseIncidentFilter(r *http.Request) (store.IncidentFilter, error) {
	q := r.URL.Query()

	filter := store.IncidentFilter{
		Status:      q.Get("status"),
		TenantID:    q.Get("tenant_id"),
		ServiceName: q.Get("service"),
		Severity:    q.Get("severity"),
	}

	if filter.Status != "" && !incident.Status(filter.Status).Valid() {
		return filter, fmt.Errorf("status must be one of %s", strings.Join(incident.SupportedStatuses(), ", "))
	}
	if err := validateSeverity(filter.Severity); err != nil {
		return filter, err
	}

	limit, offset, after, err := parsePagination(q)
	if err != nil {
		return filter, err
	}
	filter.Limit = limit
	filter.Offset = offset
	filter.After = after
	return filter, nil
}

// parseEventFilter builds a stored-event filter, parsing the time bounds as
// RFC3339 and validating severity against the event contract's vocabulary.
func parseEventFilter(r *http.Request) (store.EventFilter, error) {
	q := r.URL.Query()

	filter := store.EventFilter{
		TenantID:    q.Get("tenant_id"),
		ServiceName: q.Get("service"),
		Severity:    q.Get("severity"),
		EventType:   q.Get("event_type"),
		TraceID:     q.Get("trace_id"),
	}

	if err := validateSeverity(filter.Severity); err != nil {
		return filter, err
	}

	since, err := parseTime(q.Get("since"))
	if err != nil {
		return filter, fmt.Errorf("since must be an RFC3339 timestamp")
	}
	filter.Since = since

	until, err := parseTime(q.Get("until"))
	if err != nil {
		return filter, fmt.Errorf("until must be an RFC3339 timestamp")
	}
	filter.Until = until

	limit, offset, after, err := parsePagination(q)
	if err != nil {
		return filter, err
	}
	filter.Limit = limit
	filter.Offset = offset
	filter.After = after
	return filter, nil
}

func validateSeverity(severity string) error {
	if severity == "" {
		return nil
	}
	for _, s := range event.SupportedSeverities() {
		if severity == s {
			return nil
		}
	}
	return fmt.Errorf("severity must be one of %s", strings.Join(event.SupportedSeverities(), ", "))
}

func parseTime(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, raw)
}

func parsePagination(q url.Values) (limit, offset int, after store.Cursor, err error) {
	limit, err = parseNonNegative(q.Get("limit"), "limit")
	if err != nil {
		return 0, 0, store.Cursor{}, err
	}
	offset, err = parseNonNegative(q.Get("offset"), "offset")
	if err != nil {
		return 0, 0, store.Cursor{}, err
	}

	after, err = store.DecodeCursor(q.Get("cursor"))
	if err != nil {
		return 0, 0, store.Cursor{}, err
	}

	// Mixing the two is a client bug worth naming rather than silently resolving:
	// a cursor already encodes a position, so an offset on top of it skips rows
	// the caller has never seen.
	if !after.IsZero() && offset > 0 {
		return 0, 0, store.Cursor{}, fmt.Errorf("cursor and offset cannot be combined; use one or the other")
	}

	return limit, offset, after, nil
}

func parseNonNegative(raw, name string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return v, nil
}
