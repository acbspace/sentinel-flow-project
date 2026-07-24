package demo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/httpx"
)

// Emitter is the telemetry sink used by the demo handler.
type telemetrySink interface {
	Emit(ctx context.Context, ev event.Event) error
}

// Downstream is an optional call the handler makes before completing, used by
// order-service to invoke payment-service so the demo produces a multi-service
// trace. A nil Downstream means the service has no dependencies.
type Downstream func(ctx context.Context) error

// ErrDownstreamRejected reports that a downstream service declined the request
// (as opposed to being unreachable).
var ErrDownstreamRejected = errors.New("downstream rejected the request")

// HandlerConfig describes one demo endpoint.
type HandlerConfig struct {
	ServiceName string
	Route       string
	TenantID    string
	Environment string
	// IDPrefix labels the generated business object, e.g. "ord" or "pay".
	IDPrefix string
	// SuccessStatus is returned when the simulated operation succeeds.
	SuccessStatus int
	// FailureStatus is returned when the simulation decides to fail.
	FailureStatus int
	// DownstreamFailureStatus is returned when Downstream rejects the request.
	DownstreamFailureStatus int

	Simulator  *Simulator
	Sink       telemetrySink
	Downstream Downstream
	Logger     *slog.Logger
	// NewID and Now are injectable to keep handler tests deterministic.
	NewID func() string
	Now   func() time.Time
}

// Handler serves one simulated business endpoint and reports what happened as a
// telemetry event.
type Handler struct {
	cfg HandlerConfig
}

// NewHandler builds a demo endpoint handler.
func NewHandler(cfg HandlerConfig) *Handler {
	if cfg.SuccessStatus == 0 {
		cfg.SuccessStatus = http.StatusOK
	}
	if cfg.FailureStatus == 0 {
		cfg.FailureStatus = http.StatusInternalServerError
	}
	if cfg.DownstreamFailureStatus == 0 {
		cfg.DownstreamFailureStatus = http.StatusBadGateway
	}
	if cfg.NewID == nil {
		cfg.NewID = func() string { return uuid.NewString() }
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Handler{cfg: cfg}
}

// response is the demo endpoint's JSON body.
type response struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	LatencyMS int64  `json:"latency_ms"`
	TraceID   string `json:"trace_id,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

// ServeHTTP simulates the operation, emits telemetry, and answers the caller.
//
// Telemetry emission failures never change the HTTP response: the simulated
// business operation already succeeded or failed on its own terms, and losing an
// observability event must not be reported to the caller as a business failure.
// It is logged at error level so the loss is still visible.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	start := h.cfg.Now()

	objectID := h.cfg.IDPrefix + "_" + h.cfg.NewID()

	status := h.cfg.SuccessStatus
	outcome := "completed"
	detail := ""

	// Simulate the work this service would really be doing.
	if delay := h.cfg.Simulator.Latency(); delay > 0 {
		if err := sleepContext(ctx, delay); err != nil {
			// The client hung up mid-request; nothing useful left to write.
			h.cfg.Logger.WarnContext(ctx, "request cancelled during simulated work",
				slog.String("id", objectID),
				slog.String("error", err.Error()),
			)
			return
		}
	}

	switch {
	case h.cfg.Simulator.ShouldFail():
		status = h.cfg.FailureStatus
		outcome = "failed"
		detail = "simulated internal failure"
	case h.cfg.Downstream != nil:
		if err := h.cfg.Downstream(ctx); err != nil {
			outcome = "failed"
			if errors.Is(err, ErrDownstreamRejected) {
				status = h.cfg.DownstreamFailureStatus
				detail = "downstream service rejected the request"
			} else {
				status = http.StatusBadGateway
				detail = "downstream service is unavailable"
			}
			h.cfg.Logger.WarnContext(ctx, "downstream call failed",
				slog.String("id", objectID),
				slog.String("error", err.Error()),
			)
		}
	}

	elapsed := h.cfg.Now().Sub(start)
	traceID := traceIDFrom(ctx)

	ev := event.Event{
		EventID:       uuid.NewString(),
		SchemaVersion: event.SchemaVersion10,
		TenantID:      h.cfg.TenantID,
		ServiceName:   h.cfg.ServiceName,
		Environment:   h.cfg.Environment,
		EventType:     "request.completed",
		Severity:      severityForStatus(status),
		Timestamp:     event.NewTimestamp(h.cfg.Now().UTC()),
		TraceID:       traceID,
		Attributes: event.Attributes{
			"http_method":      r.Method,
			"http_route":       h.cfg.Route,
			"http_status_code": status,
			"latency_ms":       elapsed.Milliseconds(),
			"outcome":          outcome,
			"object_id":        objectID,
		},
	}
	if detail != "" {
		ev.Attributes["detail"] = detail
	}

	if err := h.cfg.Sink.Emit(ctx, ev); err != nil {
		h.cfg.Logger.ErrorContext(ctx, "telemetry emission failed",
			slog.String("event_id", ev.EventID),
			slog.String("id", objectID),
			slog.String("error", err.Error()),
		)
	}

	httpx.WriteJSON(ctx, w, status, response{
		ID:        objectID,
		Status:    outcome,
		LatencyMS: elapsed.Milliseconds(),
		TraceID:   traceID,
		Detail:    detail,
	}, h.cfg.Logger)
}

// severityForStatus maps an HTTP status onto the event severity vocabulary.
func severityForStatus(status int) event.Severity {
	switch {
	case status >= 500:
		return event.SeverityError
	case status >= 400:
		return event.SeverityWarn
	default:
		return event.SeverityInfo
	}
}

func traceIDFrom(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		return sc.TraceID().String()
	}
	return ""
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return fmt.Errorf("simulated work interrupted: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}
