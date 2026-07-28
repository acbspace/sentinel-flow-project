// Package correlate turns the stored telemetry stream into incidents. It
// evaluates detection rules over per-service event windows on a fixed cadence,
// opens or groups incidents when a rule trips, and auto-resolves incidents whose
// condition has gone quiet.
//
// Correlation is deliberately windowed and database-backed rather than
// per-event: an error *rate* is a property of a window, not of a single event,
// and keeping all state in PostgreSQL means the engine can restart mid-cycle
// without losing or double-counting incidents.
//
// Across replicas, incident identity is still a database guarantee — the partial
// unique index admits one active incident per fingerprint however many engines
// run. The cycle itself is not idempotent, though: consecutive windows overlap,
// so two replicas evaluating the same tick would each add their slice to
// event_count, and each would repeat the window scan. One evaluator is therefore
// elected by a PostgreSQL advisory lock; see Runner and store.CycleLock.
package correlate

import (
	"fmt"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

// RuleKind names a detection algorithm. The set is closed: adding a kind means
// adding a case to Rule.Evaluate, which is the single place that maps a kind to
// its logic.
type RuleKind string

const (
	// RuleKindErrorRate fires when the fraction of error/critical events in a
	// service's window meets or exceeds a threshold, given enough samples.
	RuleKindErrorRate RuleKind = "error_rate"
)

// Rule is one detection rule. Rules are data, so a deployment can carry several
// with different windows and thresholds without new code; only a new RuleKind
// requires new logic.
type Rule struct {
	// ID is the stable identifier that, with tenant and service, forms an
	// incident's fingerprint. Changing it detaches new detections from incidents
	// opened under the old id, so it is part of the operational contract.
	ID   string
	Name string
	Kind RuleKind

	// Window is how far back the rule looks. It is the lookback passed to the
	// event window query, so a shorter window reacts faster but on less signal.
	Window time.Duration

	// Threshold is the error fraction (0..1) at or above which error_rate fires.
	Threshold float64

	// MinEvents is the smallest sample the rule will draw a conclusion from. It
	// stops a single failed request in an otherwise idle service from opening an
	// incident at a 100% error rate over a sample of one.
	MinEvents int64

	// IncidentSeverity is the severity stamped on incidents this rule opens.
	IncidentSeverity event.Severity
}

// Detection is the outcome of evaluating a rule against one service window.
type Detection struct {
	// Fires reports whether the rule tripped. When false, the other fields are
	// zero and the caller opens no incident.
	Fires bool

	// Title is a human-readable one-line summary for the incident.
	Title string

	// EventCount is how many offending events this detection observed across the
	// whole window. It seeds the incident's total when this detection opens one.
	EventCount int64

	// NewEventCount is how many of those events arrived since the previous cycle.
	// Windows overlap, so this — not EventCount — is what a repeat detection adds
	// to an incident that is already open.
	NewEventCount int64

	// Details is the evidence behind the detection, stored on the incident so an
	// operator can see why it opened without re-running the query.
	Details map[string]any
}

// Evaluate runs the rule against one service's window tally.
func (r Rule) Evaluate(w store.ServiceWindow) Detection {
	switch r.Kind {
	case RuleKindErrorRate:
		return r.evaluateErrorRate(w)
	default:
		// An unknown kind never fires. This is unreachable through normal wiring
		// (kinds are constructed in code, not parsed from input) but keeps
		// Evaluate total rather than panicking on a future half-added kind.
		return Detection{}
	}
}

func (r Rule) evaluateErrorRate(w store.ServiceWindow) Detection {
	// Too little signal to conclude anything: report no detection rather than
	// extrapolating a rate from a handful of events.
	if w.Total < r.MinEvents {
		return Detection{}
	}

	rate := float64(w.Errors) / float64(w.Total)
	if rate < r.Threshold {
		return Detection{}
	}

	return Detection{
		Fires: true,
		Title: fmt.Sprintf("%s: %.0f%% error rate on %s (%d of %d events over %s)",
			r.Name, rate*100, w.ServiceName, w.Errors, w.Total, r.Window),
		EventCount:    w.Errors,
		NewEventCount: w.NewErrors,
		Details: map[string]any{
			"rule_id":      r.ID,
			"kind":         string(r.Kind),
			"error_rate":   rate,
			"threshold":    r.Threshold,
			"error_count":  w.Errors,
			"total_events": w.Total,
			"window":       r.Window.String(),
		},
	}
}
