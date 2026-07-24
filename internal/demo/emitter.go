package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/obs"
)

// maxErrorBodyBytes bounds how much of a failed response we read back for logs.
const maxErrorBodyBytes = 2 * 1024

// Emitter posts telemetry events to the ingestion API.
type Emitter struct {
	client  *http.Client
	url     string
	log     *slog.Logger
	timeout time.Duration
}

// NewEmitter builds an emitter targeting the ingestion API at baseURL.
//
// The transport is wrapped by otelhttp so the outgoing request carries the
// current trace context; that is what lets one trace span the demo service, the
// ingestion API and (via Kafka headers) the incident engine.
func NewEmitter(baseURL string, providers *obs.Providers, log *slog.Logger, timeout time.Duration) *Emitter {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	transport := otelhttp.NewTransport(
		http.DefaultTransport,
		otelhttp.WithTracerProvider(providers.TracerProvider),
		otelhttp.WithMeterProvider(providers.MeterProvider),
		otelhttp.WithPropagators(providers.Propagator),
	)

	return &Emitter{
		client:  &http.Client{Transport: transport, Timeout: timeout},
		url:     strings.TrimRight(baseURL, "/") + "/v1/events",
		log:     log,
		timeout: timeout,
	}
}

// Emit sends one event and reports whether the ingestion API accepted it.
func (e *Emitter) Emit(ctx context.Context, ev event.Event) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("encode telemetry event: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build telemetry request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("post telemetry event to %s: %w", e.url, err)
	}
	defer func() {
		// Drain before closing so the connection can be reused.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBodyBytes))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		return fmt.Errorf("ingestion API rejected event %s: status %d: %s",
			ev.EventID, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
}
