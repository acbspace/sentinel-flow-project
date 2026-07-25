// Package ingest implements the HTTP front door: it validates telemetry events
// submitted by instrumented services and hands the accepted ones to Kafka.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/httpx"
)

// Publisher writes accepted events to the durable log.
//
// The handler depends on this interface rather than on the Kafka producer so
// that its tests can assert on what would have been published without running a
// broker.
//
// PublishBatch is all-or-nothing: it must not report success unless every event
// is durable, because the handler turns that into a 202 for the whole set.
type Publisher interface {
	Publish(ctx context.Context, ev event.Event) error
	PublishBatch(ctx context.Context, evs []event.Event) error
}

// Handler serves POST /v1/events and POST /v1/events:batch.
type Handler struct {
	publisher      Publisher
	log            *slog.Logger
	maxBodyBytes   int64
	maxBatchBytes  int64
	maxBatchEvents int
	bounds         event.TimeBounds
	now            func() time.Time
}

// Options configures the handler. now is injectable so tests are deterministic.
type Options struct {
	Publisher    Publisher
	Logger       *slog.Logger
	MaxBodyBytes int64

	// MaxBatchBytes and MaxBatchEvents bound the batch endpoint. They are
	// separate from MaxBodyBytes because a batch is legitimately far larger than
	// a single event, and capping both at the same value would make the batch
	// endpoint useless.
	MaxBatchBytes  int64
	MaxBatchEvents int

	// Bounds rejects events whose timestamp is implausibly far from now. The
	// zero value disables both checks.
	Bounds event.TimeBounds

	Now func() time.Time
}

// NewHandler builds the ingestion handler.
func NewHandler(opts Options) *Handler {
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = 64 * 1024
	}
	if opts.MaxBatchBytes <= 0 {
		opts.MaxBatchBytes = 5 * 1024 * 1024
	}
	if opts.MaxBatchEvents <= 0 {
		opts.MaxBatchEvents = 1000
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Handler{
		publisher:      opts.Publisher,
		log:            opts.Logger,
		maxBodyBytes:   opts.MaxBodyBytes,
		maxBatchBytes:  opts.MaxBatchBytes,
		maxBatchEvents: opts.MaxBatchEvents,
		bounds:         opts.Bounds,
		now:            opts.Now,
	}
}

// errorResponse is the single error shape this API returns.
type errorResponse struct {
	Error   string             `json:"error"`
	Message string             `json:"message"`
	Details []event.FieldError `json:"details,omitempty"`
}

// acceptedResponse confirms the event was durably published.
type acceptedResponse struct {
	Status  string `json:"status"`
	EventID string `json:"event_id"`
	TraceID string `json:"trace_id,omitempty"`
}

// PostEvent accepts one telemetry event.
//
// It answers 202 only after Kafka has acknowledged the write, so a 202 means the
// event is durable rather than merely received into a buffer that a crash would
// lose.
func (h *Handler) PostEvent(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if ct := r.Header.Get("Content-Type"); ct != "" && !isJSONContentType(ct) {
		h.writeError(w, r, http.StatusUnsupportedMediaType, errorResponse{
			Error:   "unsupported_media_type",
			Message: "Content-Type must be application/json",
		})
		return
	}

	// MaxBytesReader caps the body before it is buffered, so an oversized
	// payload cannot exhaust memory on the way to being rejected.
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)

	decoder := json.NewDecoder(r.Body)
	// Unknown fields are rejected rather than ignored: at the ingestion boundary
	// a misspelled field is far more often a producer bug than a forward
	// compatible extension, and schema_version is the intended way to evolve.
	decoder.DisallowUnknownFields()

	var ev event.Event
	if err := decoder.Decode(&ev); err != nil {
		h.writeDecodeError(w, r, err)
		return
	}

	// Exactly one JSON value per request; trailing content is a client bug.
	if err := decoder.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		h.writeError(w, r, http.StatusBadRequest, errorResponse{
			Error:   "invalid_json",
			Message: "request body must contain exactly one JSON object",
		})
		return
	}

	ev.Normalize()

	// An event that arrives without a trace ID inherits the one from the
	// incoming request, so the stored row still joins to the caller's trace.
	if ev.TraceID == "" {
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			ev.TraceID = sc.TraceID().String()
		}
	}

	// Both time bounds apply here: this is the last point at which a producer
	// with a wrong clock can be told about it.
	if err := ev.ValidateWithin(h.bounds, h.now()); err != nil {
		var validationErrs event.ValidationErrors
		if errors.As(err, &validationErrs) {
			h.log.WarnContext(ctx, "event rejected",
				slog.String("event_id", ev.EventID),
				slog.String("tenant_id", ev.TenantID),
				slog.String("service_name", ev.ServiceName),
				slog.String("reason", err.Error()),
			)
			h.writeError(w, r, http.StatusBadRequest, errorResponse{
				Error:   "validation_failed",
				Message: "the event does not satisfy the telemetry event contract",
				Details: validationErrs,
			})
			return
		}
		h.writeError(w, r, http.StatusBadRequest, errorResponse{
			Error:   "validation_failed",
			Message: err.Error(),
		})
		return
	}

	if err := h.publisher.Publish(ctx, ev); err != nil {
		// The event was never durably written. Say so rather than returning 202
		// and losing it: the producer is expected to retry.
		h.log.ErrorContext(ctx, "publish failed",
			slog.String("event_id", ev.EventID),
			slog.String("tenant_id", ev.TenantID),
			slog.String("service_name", ev.ServiceName),
			slog.String("error", err.Error()),
		)
		w.Header().Set("Retry-After", "1")
		h.writeError(w, r, http.StatusServiceUnavailable, errorResponse{
			Error:   "publish_failed",
			Message: "the event could not be durably queued; retry the request",
		})
		return
	}

	h.log.InfoContext(ctx, "event accepted",
		slog.String("event_id", ev.EventID),
		slog.String("tenant_id", ev.TenantID),
		slog.String("service_name", ev.ServiceName),
		slog.String("event_type", ev.EventType),
		slog.String("severity", string(ev.Severity)),
		slog.String("event_trace_id", ev.TraceID),
	)

	httpx.WriteJSON(ctx, w, http.StatusAccepted, acceptedResponse{
		Status:  "accepted",
		EventID: ev.EventID,
		TraceID: ev.TraceID,
	}, h.log)
}

// writeDecodeError maps a JSON decoding failure onto a specific status code and
// a message the caller can act on.
func (h *Handler) writeDecodeError(w http.ResponseWriter, r *http.Request, err error) {
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		h.writeError(w, r, http.StatusRequestEntityTooLarge, errorResponse{
			Error:   "payload_too_large",
			Message: "request body exceeds the maximum accepted size",
		})
		return
	}

	if errors.Is(err, io.EOF) {
		h.writeError(w, r, http.StatusBadRequest, errorResponse{
			Error:   "invalid_json",
			Message: "request body is empty",
		})
		return
	}

	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		h.writeError(w, r, http.StatusBadRequest, errorResponse{
			Error:   "invalid_json",
			Message: "field " + typeErr.Field + " must be of type " + typeErr.Type.String(),
		})
		return
	}

	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		h.writeError(w, r, http.StatusBadRequest, errorResponse{
			Error:   "invalid_json",
			Message: "request body is not valid JSON: " + syntaxErr.Error(),
		})
		return
	}

	// Covers DisallowUnknownFields and the RFC3339 timestamp error, both of
	// which arrive as plain errors with an already-descriptive message.
	h.writeError(w, r, http.StatusBadRequest, errorResponse{
		Error:   "invalid_json",
		Message: err.Error(),
	})
}

func (h *Handler) writeError(w http.ResponseWriter, r *http.Request, status int, body errorResponse) {
	httpx.WriteJSON(r.Context(), w, status, body, h.log)
}

func isJSONContentType(contentType string) bool {
	mediaType := contentType
	if idx := strings.IndexByte(mediaType, ';'); idx >= 0 {
		mediaType = mediaType[:idx]
	}
	mediaType = strings.ToLower(strings.TrimSpace(mediaType))
	return mediaType == "application/json" || strings.HasSuffix(mediaType, "+json")
}
