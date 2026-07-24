package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// ReadinessCheck reports whether one dependency is usable right now.
type ReadinessCheck struct {
	Name  string
	Check func(context.Context) error
}

// HealthHandler answers liveness probes.
//
// Liveness means "this process is running and its event loop is responsive"; it
// deliberately does not consult dependencies, because restarting a healthy
// process when Kafka is down makes an outage worse rather than better.
func HealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(r.Context(), w, http.StatusOK, map[string]string{"status": "ok"}, nil)
	}
}

// ReadinessHandler answers readiness probes by running every check.
//
// Readiness means "this process can serve traffic right now". A failing
// dependency returns 503 so the orchestrator stops routing to it without
// killing it.
func ReadinessHandler(log *slog.Logger, timeout time.Duration, checks ...ReadinessCheck) http.HandlerFunc {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		results := make(map[string]string, len(checks))
		var failures []error

		for _, c := range checks {
			if err := c.Check(ctx); err != nil {
				results[c.Name] = "error: " + err.Error()
				failures = append(failures, err)
				continue
			}
			results[c.Name] = "ok"
		}

		status := http.StatusOK
		body := map[string]any{"status": "ready", "checks": results}

		if len(failures) > 0 {
			status = http.StatusServiceUnavailable
			body["status"] = "not_ready"
			log.WarnContext(ctx, "readiness check failed", slog.String("error", errors.Join(failures...).Error()))
		}

		writeJSON(ctx, w, status, body, log)
	}
}

// writeJSON serialises v as JSON. A write failure cannot be reported to the
// client (the status line is already gone), so it is logged instead of dropped.
func writeJSON(ctx context.Context, w http.ResponseWriter, status int, v any, log *slog.Logger) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil && log != nil {
		log.WarnContext(ctx, "write response body", slog.String("error", err.Error()))
	}
}

// WriteJSON serialises v as a JSON response body.
func WriteJSON(ctx context.Context, w http.ResponseWriter, status int, v any, log *slog.Logger) {
	writeJSON(ctx, w, status, v, log)
}
