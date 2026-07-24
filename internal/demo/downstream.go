package demo

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/acbspace/sentinel-flow-project/internal/obs"
)

// HTTPDownstream calls another demo service over HTTP with trace propagation.
//
// It exists so that order-service can invoke payment-service, producing a trace
// that spans two services plus the ingestion API. Errors distinguish "the
// service said no" from "the service was unreachable", because the two deserve
// different HTTP statuses and different severities.
func HTTPDownstream(baseURL, path string, providers *obs.Providers, timeout time.Duration) Downstream {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	client := &http.Client{
		Transport: otelhttp.NewTransport(
			http.DefaultTransport,
			otelhttp.WithTracerProvider(providers.TracerProvider),
			otelhttp.WithMeterProvider(providers.MeterProvider),
			otelhttp.WithPropagators(providers.Propagator),
		),
		Timeout: timeout,
	}

	url := strings.TrimRight(baseURL, "/") + path

	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader("{}"))
		if err != nil {
			return fmt.Errorf("build downstream request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("call %s: %w", url, err)
		}
		defer func() {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBodyBytes))
			_ = resp.Body.Close()
		}()

		if resp.StatusCode >= 400 {
			return fmt.Errorf("%w: %s returned %d", ErrDownstreamRejected, url, resp.StatusCode)
		}
		return nil
	}
}
