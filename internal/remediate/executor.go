package remediate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/runbook"
)

// Executor performs a runbook step.
//
// This project deliberately ships only two action kinds: noop, which records
// intent and touches nothing, and webhook, which POSTs the step's context to a
// URL. Restarting deployments or draining nodes belongs behind that webhook, in
// a system that owns those credentials — not in the incident platform itself.
type Executor struct {
	httpClient *http.Client
	log        *slog.Logger
}

// NewExecutor builds an action executor.
func NewExecutor(timeout time.Duration, log *slog.Logger) *Executor {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Executor{
		httpClient: &http.Client{Timeout: timeout},
		log:        log,
	}
}

// Execute runs one step and returns the detail to record against it. An error
// means the step failed, which halts the runbook.
func (e *Executor) Execute(ctx context.Context, args ExecuteArgs) (map[string]any, error) {
	switch runbook.ActionKind(args.Kind) {
	case runbook.ActionNoop:
		e.log.InfoContext(ctx, "remediation noop step",
			slog.String("incident_id", args.IncidentID),
			slog.String("step", args.StepName),
		)
		return map[string]any{"executed": "noop", "params": args.Params}, nil

	case runbook.ActionWebhook:
		return e.postWebhook(ctx, args)

	default:
		// Unreachable through a validated catalog, but an unknown action must
		// fail loudly rather than silently succeed having done nothing.
		return nil, fmt.Errorf("unknown remediation action kind %q", args.Kind)
	}
}

func (e *Executor) postWebhook(ctx context.Context, args ExecuteArgs) (map[string]any, error) {
	if args.Target == "" {
		return nil, fmt.Errorf("step %q is a webhook action with no target", args.StepName)
	}

	payload, err := json.Marshal(map[string]any{
		"incident_id":  args.IncidentID,
		"service_name": args.ServiceName,
		"title":        args.Title,
		"step":         args.StepName,
		"params":       args.Params,
	})
	if err != nil {
		return nil, fmt.Errorf("encode remediation payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, args.Target, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build remediation request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call remediation target: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("remediation target returned status %d", resp.StatusCode)
	}

	e.log.InfoContext(ctx, "remediation webhook step executed",
		slog.String("incident_id", args.IncidentID),
		slog.String("step", args.StepName),
		slog.Int("status_code", resp.StatusCode),
	)
	return map[string]any{
		"executed":    "webhook",
		"target":      args.Target,
		"status_code": resp.StatusCode,
		"params":      args.Params,
	}, nil
}
