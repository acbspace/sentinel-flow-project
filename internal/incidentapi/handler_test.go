package incidentapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/incident"
	"github.com/acbspace/sentinel-flow-project/internal/incidentapi"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

const sampleID = "11111111-1111-1111-1111-111111111111"

type fakeIncidents struct {
	list       []incident.Incident
	nextCursor store.Cursor
	listErr    error
	listFilter store.IncidentFilter // captured from the last List call

	get    incident.Incident
	getErr error

	ackResult     incident.Incident
	ackErr        error
	resolveResult incident.Incident
	resolveErr    error
}

func (f *fakeIncidents) List(_ context.Context, filter store.IncidentFilter) ([]incident.Incident, store.Cursor, error) {
	f.listFilter = filter
	return f.list, f.nextCursor, f.listErr
}

func (f *fakeIncidents) Get(_ context.Context, _ string) (incident.Incident, error) {
	return f.get, f.getErr
}

func (f *fakeIncidents) Acknowledge(_ context.Context, _ string) (incident.Incident, error) {
	return f.ackResult, f.ackErr
}

func (f *fakeIncidents) Resolve(_ context.Context, _ string) (incident.Incident, error) {
	return f.resolveResult, f.resolveErr
}

type fakeEvents struct {
	events     []store.StoredEvent
	nextCursor store.Cursor
	err        error
	filter     store.EventFilter // captured
}

func (f *fakeEvents) ListEvents(_ context.Context, filter store.EventFilter) ([]store.StoredEvent, store.Cursor, error) {
	f.filter = filter
	return f.events, f.nextCursor, f.err
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

type fakeNotifications struct {
	notes []store.Notification
	err   error
}

func (f *fakeNotifications) ListByIncident(context.Context, string) ([]store.Notification, error) {
	return f.notes, f.err
}

type fakeSignaler struct {
	ackCalls     []string
	resolveCalls []string
	err          error
}

func (f *fakeSignaler) SignalAcknowledged(_ context.Context, id string) error {
	f.ackCalls = append(f.ackCalls, id)
	return f.err
}

func (f *fakeSignaler) SignalResolved(_ context.Context, id string) error {
	f.resolveCalls = append(f.resolveCalls, id)
	return f.err
}

type fakeRemediation struct {
	actions    []store.RemediationAction
	pending    store.RemediationAction
	pendingErr error
	listErr    error
}

func (f *fakeRemediation) ListByIncident(context.Context, string) ([]store.RemediationAction, error) {
	return f.actions, f.listErr
}

func (f *fakeRemediation) Pending(context.Context, string) (store.RemediationAction, error) {
	return f.pending, f.pendingErr
}

type fakeApprover struct {
	approved []string
	rejected []string
	err      error
}

func (f *fakeApprover) Approve(_ context.Context, id, actor string) error {
	if f.err != nil {
		return f.err
	}
	f.approved = append(f.approved, id+":"+actor)
	return nil
}

func (f *fakeApprover) Reject(_ context.Context, id, actor string) error {
	if f.err != nil {
		return f.err
	}
	f.rejected = append(f.rejected, id+":"+actor)
	return nil
}

func newServerWith(opts incidentapi.Options) http.Handler {
	opts.Logger = discardLogger()
	h := incidentapi.NewHandler(opts)
	r := chi.NewRouter()
	h.Mount(r)
	return r
}

func newServer(inc incidentapi.IncidentStore, ev incidentapi.EventStore) http.Handler {
	return newServerWith(incidentapi.Options{Incidents: inc, Events: ev})
}

func do(srv http.Handler, method, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

func sampleIncident() incident.Incident {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	return incident.Incident{
		ID:          sampleID,
		Fingerprint: incident.Fingerprint("error_rate", "tenant-a", "payment-service"),
		TenantID:    "tenant-a",
		ServiceName: "payment-service",
		RuleID:      "error_rate",
		Title:       "elevated error rate on payment-service",
		Severity:    event.SeverityError,
		Status:      incident.StatusOpen,
		EventCount:  15,
		FirstSeenAt: now,
		LastSeenAt:  now,
		OpenedAt:    now,
		UpdatedAt:   now,
	}
}

func TestListIncidentsReturnsAndParsesFilter(t *testing.T) {
	t.Parallel()

	inc := &fakeIncidents{list: []incident.Incident{sampleIncident(), sampleIncident()}}
	srv := newServer(inc, &fakeEvents{})

	rec := do(srv, http.MethodGet, "/v1/incidents?status=open&service=payment-service&tenant_id=tenant-a&limit=10&offset=5")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}

	var body struct {
		Incidents []incident.Incident `json:"incidents"`
		Count     int                 `json:"count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Count != 2 || len(body.Incidents) != 2 {
		t.Errorf("count/len = %d/%d, want 2/2", body.Count, len(body.Incidents))
	}

	// The query string must be parsed into the store filter faithfully.
	got := inc.listFilter
	if got.Status != "open" || got.ServiceName != "payment-service" || got.TenantID != "tenant-a" {
		t.Errorf("filter = %+v, want status/service/tenant populated", got)
	}
	if got.Limit != 10 || got.Offset != 5 {
		t.Errorf("pagination = limit %d offset %d, want 10/5", got.Limit, got.Offset)
	}
}

func TestListIncidentsEmptyIsJSONArrayNotNull(t *testing.T) {
	t.Parallel()

	srv := newServer(&fakeIncidents{list: nil}, &fakeEvents{})
	rec := do(srv, http.MethodGet, "/v1/incidents")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	// A nil slice must serialise as [] so clients can iterate unconditionally.
	if body := rec.Body.String(); !strings.Contains(body, `"incidents":[]`) {
		t.Errorf("body = %s, want an empty incidents array", body)
	}
}

func TestListIncidentsRejectsBadQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
	}{
		{"unknown status", "/v1/incidents?status=exploded"},
		{"unknown severity", "/v1/incidents?severity=nuclear"},
		{"non-numeric limit", "/v1/incidents?limit=lots"},
		{"negative offset", "/v1/incidents?offset=-3"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inc := &fakeIncidents{}
			rec := do(newServer(inc, &fakeEvents{}), http.MethodGet, tc.target)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400 for %s", rec.Code, tc.name)
			}
		})
	}
}

func TestGetIncident(t *testing.T) {
	t.Parallel()

	t.Run("found returns the incident", func(t *testing.T) {
		t.Parallel()

		srv := newServer(&fakeIncidents{get: sampleIncident()}, &fakeEvents{})
		rec := do(srv, http.MethodGet, "/v1/incidents/"+sampleID)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var got incident.Incident
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.ID != sampleID {
			t.Errorf("id = %q, want %q", got.ID, sampleID)
		}
	})

	t.Run("non-UUID id is a 400", func(t *testing.T) {
		t.Parallel()

		srv := newServer(&fakeIncidents{get: sampleIncident()}, &fakeEvents{})
		rec := do(srv, http.MethodGet, "/v1/incidents/not-a-uuid")

		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("missing incident is a 404", func(t *testing.T) {
		t.Parallel()

		srv := newServer(&fakeIncidents{getErr: store.ErrIncidentNotFound}, &fakeEvents{})
		rec := do(srv, http.MethodGet, "/v1/incidents/"+sampleID)

		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want 404", rec.Code)
		}
	})
}

func TestAcknowledgeAndResolve(t *testing.T) {
	t.Parallel()

	t.Run("acknowledge succeeds", func(t *testing.T) {
		t.Parallel()

		ackd := sampleIncident()
		ackd.Status = incident.StatusAcknowledged
		srv := newServer(&fakeIncidents{ackResult: ackd}, &fakeEvents{})

		rec := do(srv, http.MethodPost, "/v1/incidents/"+sampleID+"/acknowledge")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var got incident.Incident
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Status != incident.StatusAcknowledged {
			t.Errorf("status = %q, want acknowledged", got.Status)
		}
	})

	t.Run("illegal transition is a 409", func(t *testing.T) {
		t.Parallel()

		srv := newServer(&fakeIncidents{
			ackErr: &store.InvalidTransitionError{ID: sampleID, From: incident.StatusResolved},
		}, &fakeEvents{})

		rec := do(srv, http.MethodPost, "/v1/incidents/"+sampleID+"/acknowledge")
		if rec.Code != http.StatusConflict {
			t.Errorf("status = %d, want 409", rec.Code)
		}
	})

	t.Run("resolve of a missing incident is a 404", func(t *testing.T) {
		t.Parallel()

		srv := newServer(&fakeIncidents{resolveErr: store.ErrIncidentNotFound}, &fakeEvents{})

		rec := do(srv, http.MethodPost, "/v1/incidents/"+sampleID+"/resolve")
		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want 404", rec.Code)
		}
	})
}

func TestTransitionsSignalTheAlertWorkflow(t *testing.T) {
	t.Parallel()

	t.Run("acknowledge signals the workflow", func(t *testing.T) {
		t.Parallel()

		sig := &fakeSignaler{}
		srv := newServerWith(incidentapi.Options{
			Incidents: &fakeIncidents{ackResult: sampleIncident()},
			Signaler:  sig,
		})

		if rec := do(srv, http.MethodPost, "/v1/incidents/"+sampleID+"/acknowledge"); rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if len(sig.ackCalls) != 1 || sig.ackCalls[0] != sampleID {
			t.Errorf("acknowledge signals = %v, want [%s]", sig.ackCalls, sampleID)
		}
	})

	t.Run("resolve signals the workflow", func(t *testing.T) {
		t.Parallel()

		sig := &fakeSignaler{}
		srv := newServerWith(incidentapi.Options{
			Incidents: &fakeIncidents{resolveResult: sampleIncident()},
			Signaler:  sig,
		})

		if rec := do(srv, http.MethodPost, "/v1/incidents/"+sampleID+"/resolve"); rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if len(sig.resolveCalls) != 1 {
			t.Errorf("resolve signals = %v, want one call", sig.resolveCalls)
		}
	})

	t.Run("a failing signal does not fail the request", func(t *testing.T) {
		t.Parallel()

		sig := &fakeSignaler{err: errors.New("temporal unreachable")}
		srv := newServerWith(incidentapi.Options{
			Incidents: &fakeIncidents{ackResult: sampleIncident()},
			Signaler:  sig,
		})

		// The DB transition is authoritative; a signal failure is logged, not surfaced.
		if rec := do(srv, http.MethodPost, "/v1/incidents/"+sampleID+"/acknowledge"); rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200 despite the signal failure", rec.Code)
		}
	})
}

func TestListNotifications(t *testing.T) {
	t.Parallel()

	t.Run("returns the timeline", func(t *testing.T) {
		t.Parallel()

		notes := &fakeNotifications{notes: []store.Notification{
			{ID: "n1", IncidentID: sampleID, Level: 1, Target: "primary", Contact: "alice", Channel: "log", Status: "sent"},
			{ID: "n2", IncidentID: sampleID, Level: 2, Target: "secondary", Contact: "bob", Channel: "log", Status: "sent"},
		}}
		srv := newServerWith(incidentapi.Options{Notifications: notes})

		rec := do(srv, http.MethodGet, "/v1/incidents/"+sampleID+"/notifications")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
		}
		var body struct {
			Notifications []store.Notification `json:"notifications"`
			Count         int                  `json:"count"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if body.Count != 2 || len(body.Notifications) != 2 {
			t.Errorf("count/len = %d/%d, want 2/2", body.Count, len(body.Notifications))
		}
	})

	t.Run("empty timeline is a JSON array", func(t *testing.T) {
		t.Parallel()

		srv := newServerWith(incidentapi.Options{Notifications: &fakeNotifications{notes: nil}})
		rec := do(srv, http.MethodGet, "/v1/incidents/"+sampleID+"/notifications")
		if !strings.Contains(rec.Body.String(), `"notifications":[]`) {
			t.Errorf("body = %s, want an empty notifications array", rec.Body.String())
		}
	})

	t.Run("non-UUID id is a 400", func(t *testing.T) {
		t.Parallel()

		srv := newServerWith(incidentapi.Options{Notifications: &fakeNotifications{}})
		if rec := do(srv, http.MethodGet, "/v1/incidents/nope/notifications"); rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})
}

func TestRemediationEndpoints(t *testing.T) {
	t.Parallel()

	pendingStep := store.RemediationAction{
		ID: "a1", IncidentID: sampleID, RunbookID: "error-rate-response",
		StepIndex: 2, StepName: "restart instances", ActionKind: "webhook",
		Mode: "approval", Status: store.RemediationPending,
	}

	t.Run("lists the action trail", func(t *testing.T) {
		t.Parallel()

		rem := &fakeRemediation{actions: []store.RemediationAction{pendingStep}}
		rec := do(newServerWith(incidentapi.Options{Remediation: rem}), http.MethodGet,
			"/v1/incidents/"+sampleID+"/remediation")

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
		}
		var body struct {
			Actions []store.RemediationAction `json:"actions"`
			Count   int                       `json:"count"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if body.Count != 1 || body.Actions[0].StepName != "restart instances" {
			t.Errorf("body = %+v, want the one pending step", body)
		}
	})

	t.Run("approve delivers the decision and answers 202", func(t *testing.T) {
		t.Parallel()

		rem := &fakeRemediation{pending: pendingStep}
		app := &fakeApprover{}
		srv := newServerWith(incidentapi.Options{Remediation: rem, Approver: app})

		rec := do(srv, http.MethodPost, "/v1/incidents/"+sampleID+"/remediation/approve?actor=alice")
		// 202, not 200: the workflow applies the decision asynchronously.
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 (body: %s)", rec.Code, rec.Body.String())
		}
		if len(app.approved) != 1 || app.approved[0] != sampleID+":alice" {
			t.Errorf("approved = %v, want one entry naming the actor", app.approved)
		}
	})

	t.Run("reject delivers the decision", func(t *testing.T) {
		t.Parallel()

		rem := &fakeRemediation{pending: pendingStep}
		app := &fakeApprover{}
		srv := newServerWith(incidentapi.Options{Remediation: rem, Approver: app})

		if rec := do(srv, http.MethodPost, "/v1/incidents/"+sampleID+"/remediation/reject"); rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", rec.Code)
		}
		if len(app.rejected) != 1 {
			t.Errorf("rejected = %v, want one entry", app.rejected)
		}
	})

	t.Run("deciding with nothing pending is a 409", func(t *testing.T) {
		t.Parallel()

		rem := &fakeRemediation{pendingErr: store.ErrNoPendingAction}
		app := &fakeApprover{}
		srv := newServerWith(incidentapi.Options{Remediation: rem, Approver: app})

		if rec := do(srv, http.MethodPost, "/v1/incidents/"+sampleID+"/remediation/approve"); rec.Code != http.StatusConflict {
			t.Errorf("status = %d, want 409", rec.Code)
		}
		if len(app.approved) != 0 {
			t.Error("a decision was delivered even though nothing was pending")
		}
	})

	t.Run("unconfigured remediation is a 503", func(t *testing.T) {
		t.Parallel()

		// No remediation store wired: say so plainly rather than panicking.
		srv := newServerWith(incidentapi.Options{})
		if rec := do(srv, http.MethodGet, "/v1/incidents/"+sampleID+"/remediation"); rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", rec.Code)
		}
	})
}

func TestListEvents(t *testing.T) {
	t.Parallel()

	t.Run("parses filter including time bounds", func(t *testing.T) {
		t.Parallel()

		ev := &fakeEvents{}
		srv := newServer(&fakeIncidents{}, ev)

		rec := do(srv, http.MethodGet, "/v1/events?service=payment-service&severity=error&since=2026-07-24T10:00:00Z")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
		}
		if ev.filter.ServiceName != "payment-service" || ev.filter.Severity != "error" {
			t.Errorf("filter = %+v, want service/severity populated", ev.filter)
		}
		if ev.filter.Since.IsZero() {
			t.Error("since was not parsed into the filter")
		}
	})

	t.Run("rejects a non-RFC3339 since", func(t *testing.T) {
		t.Parallel()

		rec := do(newServer(&fakeIncidents{}, &fakeEvents{}), http.MethodGet, "/v1/events?since=yesterday")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("rejects an unknown severity", func(t *testing.T) {
		t.Parallel()

		rec := do(newServer(&fakeIncidents{}, &fakeEvents{}), http.MethodGet, "/v1/events?severity=spicy")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})
}

func TestListIncidentsReturnsTheNextCursor(t *testing.T) {
	t.Parallel()

	seen := time.Date(2026, 7, 25, 9, 0, 0, 0, time.UTC)
	next := store.Cursor{Time: seen, ID: "abc"}
	inc := &fakeIncidents{list: []incident.Incident{sampleIncident()}, nextCursor: next}

	rec := do(newServer(inc, &fakeEvents{}), http.MethodGet, "/v1/incidents")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.NextCursor != next.Encode() {
		t.Errorf("next_cursor = %q, want %q", resp.NextCursor, next.Encode())
	}
}

func TestListIncidentsOmitsTheCursorOnTheLastPage(t *testing.T) {
	t.Parallel()

	inc := &fakeIncidents{list: []incident.Incident{sampleIncident()}}

	rec := do(newServer(inc, &fakeEvents{}), http.MethodGet, "/v1/incidents")

	// The absence of the field is the end-of-list signal, so it must not appear
	// as an empty string that a client could mistake for a valid position.
	if strings.Contains(rec.Body.String(), "next_cursor") {
		t.Errorf("last page should carry no next_cursor:\n%s", rec.Body.String())
	}
}

func TestListIncidentsPassesTheCursorToTheStore(t *testing.T) {
	t.Parallel()

	seen := time.Date(2026, 7, 25, 9, 0, 0, 0, time.UTC)
	token := store.Cursor{Time: seen, ID: "abc"}.Encode()
	inc := &fakeIncidents{}

	rec := do(newServer(inc, &fakeEvents{}), http.MethodGet, "/v1/incidents?cursor="+token)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}

	if !inc.listFilter.After.Time.Equal(seen) || inc.listFilter.After.ID != "abc" {
		t.Errorf("filter.After = %+v, want %s/abc", inc.listFilter.After, seen)
	}
}

func TestListRejectsAMalformedCursor(t *testing.T) {
	t.Parallel()

	srv := newServer(&fakeIncidents{}, &fakeEvents{})

	for _, target := range []string{"/v1/incidents?cursor=!!!", "/v1/events?cursor=!!!"} {
		rec := do(srv, http.MethodGet, target)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", target, rec.Code)
		}
	}
}

func TestListRejectsCursorCombinedWithOffset(t *testing.T) {
	t.Parallel()

	token := store.Cursor{Time: time.Now().UTC(), ID: "abc"}.Encode()

	// A cursor already encodes a position; an offset on top of it silently skips
	// rows the caller has never seen, so it is a 400 rather than a guess.
	rec := do(newServer(&fakeIncidents{}, &fakeEvents{}), http.MethodGet,
		"/v1/incidents?cursor="+token+"&offset=10")
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body: %s", rec.Code, rec.Body.String())
	}
}
