package httpx

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/acbspace/sentinel-flow-project/internal/obs"
)

// statusRecorder captures the status code so middleware can report it after the
// handler has run.
type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wrote {
		r.status = code
		r.wrote = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wrote {
		// A handler that writes without calling WriteHeader implies 200.
		r.status = http.StatusOK
		r.wrote = true
	}
	return r.ResponseWriter.Write(b)
}

// Observe records request count, duration and a structured access log line.
//
// The route label comes from chi's route pattern rather than the raw path, so a
// high-cardinality URL never explodes the metric's label set.
func Observe(metrics *obs.HTTPMetrics, log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r)

			elapsed := time.Since(start)
			route := routePattern(r)

			metrics.Record(r.Context(), r.Method, route, rec.status, elapsed)

			log.InfoContext(r.Context(), "http request",
				slog.String("http.method", r.Method),
				slog.String("http.route", route),
				slog.String("http.path", r.URL.Path),
				slog.Int("http.status_code", rec.status),
				slog.Int64("duration_ms", elapsed.Milliseconds()),
				slog.String("request_id", middleware.GetReqID(r.Context())),
			)
		})
	}
}

// routePattern returns the matched chi pattern, falling back to a constant for
// unmatched requests so 404 floods cannot create unbounded metric series.
func routePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if pattern := rctx.RoutePattern(); pattern != "" {
			return pattern
		}
	}
	return "unmatched"
}
