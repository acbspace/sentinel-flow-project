package ingest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"go.opentelemetry.io/otel/trace"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/httpx"
)

// batchRequest is the envelope for POST /v1/events:batch.
//
// Events are held as raw JSON so each one can be decoded on its own. Decoding
// the whole array at once would mean a single misspelled field rejected the
// entire batch, when the point of a batch endpoint is that one bad event does
// not cost the caller the other nine hundred.
type batchRequest struct {
	Events []json.RawMessage `json:"events"`
}

// batchItemError names one rejected event by its position in the request. The
// index is what makes the response actionable: the caller knows which of its
// events to fix without matching on content.
type batchItemError struct {
	Index   int                `json:"index"`
	EventID string             `json:"event_id,omitempty"`
	Error   string             `json:"error"`
	Message string             `json:"message,omitempty"`
	Details []event.FieldError `json:"details,omitempty"`
}

// batchResponse reports what happened to each part of the batch.
//
// A 202 means every counted-as-accepted event is durable in Kafka. Rejected
// events are permanently rejected: they failed validation, and resending them
// unchanged will fail again. Callers must read `rejected` rather than treating
// the 202 as blanket success.
type batchResponse struct {
	Status   string           `json:"status"`
	Accepted int              `json:"accepted"`
	Rejected int              `json:"rejected"`
	Errors   []batchItemError `json:"errors,omitempty"`
}

// PostEventBatch accepts many telemetry events in one request.
//
// It exists because the single-event path costs one HTTP round trip and one
// synchronous Kafka round trip per event, which is the ingest ceiling. A batch
// amortises both without weakening durability: the produce is still acks=all and
// still synchronous, just once for the whole set.
func (h *Handler) PostEventBatch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if ct := r.Header.Get("Content-Type"); ct != "" && !isJSONContentType(ct) {
		h.writeError(w, r, http.StatusUnsupportedMediaType, errorResponse{
			Error:   "unsupported_media_type",
			Message: "Content-Type must be application/json",
		})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, h.maxBatchBytes)

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var req batchRequest
	if err := decoder.Decode(&req); err != nil {
		h.writeDecodeError(w, r, err)
		return
	}
	if err := decoder.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		h.writeError(w, r, http.StatusBadRequest, errorResponse{
			Error:   "invalid_json",
			Message: "request body must contain exactly one JSON object",
		})
		return
	}

	if len(req.Events) == 0 {
		h.writeError(w, r, http.StatusBadRequest, errorResponse{
			Error:   "empty_batch",
			Message: "events must contain at least one event",
		})
		return
	}
	if len(req.Events) > h.maxBatchEvents {
		h.writeError(w, r, http.StatusRequestEntityTooLarge, errorResponse{
			Error:   "batch_too_large",
			Message: fmt.Sprintf("a batch may contain at most %d events", h.maxBatchEvents),
		})
		return
	}

	now := h.now()
	traceID := ""
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		traceID = sc.TraceID().String()
	}

	accepted := make([]event.Event, 0, len(req.Events))
	var rejected []batchItemError

	for i, raw := range req.Events {
		ev, err := decodeBatchEvent(raw, traceID)
		if err != nil {
			rejected = append(rejected, batchItemError{
				Index:   i,
				Error:   "invalid_json",
				Message: err.Error(),
			})
			continue
		}

		if err := ev.ValidateWithin(h.bounds, now); err != nil {
			item := batchItemError{Index: i, EventID: ev.EventID, Error: "validation_failed"}
			var validationErrs event.ValidationErrors
			if errors.As(err, &validationErrs) {
				item.Details = validationErrs
			}
			rejected = append(rejected, item)
			continue
		}

		accepted = append(accepted, ev)
	}

	// Nothing publishable means the request as a whole was wrong, so it is a 400
	// rather than a 202 reporting that everything failed.
	if len(accepted) == 0 {
		h.log.WarnContext(ctx, "event batch rejected entirely",
			slog.Int("events", len(req.Events)),
		)
		httpx.WriteJSON(ctx, w, http.StatusBadRequest, batchResponse{
			Status:   "rejected",
			Rejected: len(rejected),
			Errors:   rejected,
		}, h.log)
		return
	}

	if err := h.publisher.PublishBatch(ctx, accepted); err != nil {
		h.log.ErrorContext(ctx, "batch publish failed",
			slog.Int("events", len(accepted)),
			slog.String("error", err.Error()),
		)
		w.Header().Set("Retry-After", "1")
		h.writeError(w, r, http.StatusServiceUnavailable, errorResponse{
			Error:   "publish_failed",
			Message: "the batch could not be durably queued; retry the request",
		})
		return
	}

	h.log.InfoContext(ctx, "event batch accepted",
		slog.Int("accepted", len(accepted)),
		slog.Int("rejected", len(rejected)),
	)

	httpx.WriteJSON(ctx, w, http.StatusAccepted, batchResponse{
		Status:   "accepted",
		Accepted: len(accepted),
		Rejected: len(rejected),
		Errors:   rejected,
	}, h.log)
}

// decodeBatchEvent decodes and normalizes one event from the batch, applying the
// same strict decoding the single-event path uses.
func decodeBatchEvent(raw json.RawMessage, traceID string) (event.Event, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	var ev event.Event
	if err := decoder.Decode(&ev); err != nil {
		return event.Event{}, err
	}

	ev.Normalize()
	if ev.TraceID == "" {
		ev.TraceID = traceID
	}
	return ev, nil
}
