package alerting

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/client"
)

// DialTemporal connects to Temporal, retrying until it is reachable or the
// deadline passes, so a service can start in any order relative to the Temporal
// server. Both the alert worker and the incidents-api use it.
func DialTemporal(ctx context.Context, address, namespace string, log *slog.Logger) (client.Client, error) {
	logger := temporalLogger{log: log}
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error

	for attempt := 1; ; attempt++ {
		c, err := client.Dial(client.Options{
			HostPort:  address,
			Namespace: namespace,
			Logger:    logger,
		})
		if err == nil {
			log.Info("temporal client connected",
				slog.String("address", address),
				slog.String("namespace", namespace),
			)
			return c, nil
		}
		lastErr = err

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("temporal at %s did not become reachable: %w", address, lastErr)
		}
		log.Warn("waiting for temporal to become reachable",
			slog.Int("attempt", attempt),
			slog.String("error", err.Error()),
		)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// temporalLogger adapts slog to the Temporal SDK's logger interface, so the
// SDK's logs share the service's structured JSON format.
type temporalLogger struct{ log *slog.Logger }

func (t temporalLogger) Debug(msg string, kv ...any) { t.log.Debug(msg, kv...) }
func (t temporalLogger) Info(msg string, kv ...any)  { t.log.Info(msg, kv...) }
func (t temporalLogger) Warn(msg string, kv ...any)  { t.log.Warn(msg, kv...) }
func (t temporalLogger) Error(msg string, kv ...any) { t.log.Error(msg, kv...) }
