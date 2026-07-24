// Package incident defines the incident domain: the record the correlation
// engine opens when the event stream trips a rule, and the lifecycle it moves
// through until an operator (or a quiet period) closes it.
//
// Like the event package, this package is transport and storage agnostic. It
// knows nothing about SQL or HTTP so that the correlation engine and the read
// API can share one definition of what an incident is and which state
// transitions are legal.
package incident

import (
	"strings"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/event"
)

// Status is where an incident sits in its lifecycle. The set is closed so that
// both the store's transition guards and the API's responses draw on one
// vocabulary.
type Status string

// The three lifecycle states. An incident is opened by the correlation engine,
// may be acknowledged by an operator taking ownership, and ends resolved either
// manually or by auto-resolution after a quiet period.
const (
	StatusOpen         Status = "open"
	StatusAcknowledged Status = "acknowledged"
	StatusResolved     Status = "resolved"
)

var supportedStatuses = map[Status]struct{}{
	StatusOpen:         {},
	StatusAcknowledged: {},
	StatusResolved:     {},
}

// Valid reports whether s is a recognised status.
func (s Status) Valid() bool {
	_, ok := supportedStatuses[s]
	return ok
}

// Active reports whether an incident in this status is still ongoing, meaning a
// repeat detection should group into it rather than open a new incident. Only
// resolved incidents are inactive.
func (s Status) Active() bool {
	return s == StatusOpen || s == StatusAcknowledged
}

// CanTransitionTo reports whether an incident may move from s to next.
//
// The lifecycle is deliberately one-directional: an incident can be
// acknowledged, then resolved, but never reopened. A recurrence of the same
// condition opens a fresh incident instead, so that each incident describes one
// contiguous episode with an honest start and end.
func (s Status) CanTransitionTo(next Status) bool {
	switch s {
	case StatusOpen:
		return next == StatusAcknowledged || next == StatusResolved
	case StatusAcknowledged:
		return next == StatusResolved
	default:
		return false
	}
}

// SupportedStatuses lists the recognised statuses in lifecycle order.
func SupportedStatuses() []string {
	return []string{
		string(StatusOpen),
		string(StatusAcknowledged),
		string(StatusResolved),
	}
}

// Fingerprint is the deterministic identity that groups detections into one
// incident. Every detection produced by the same rule, for the same tenant and
// service, shares a fingerprint, and at most one non-resolved incident may exist
// per fingerprint at a time. This is what turns a storm of detections into a
// single incident with a rising event count instead of a flood of duplicates.
func Fingerprint(ruleID, tenantID, serviceName string) string {
	return ruleID + "\x1f" + tenantID + "\x1f" + serviceName
}

// Incident is one detected episode of unhealthy behaviour.
type Incident struct {
	ID          string         `json:"id"`
	Fingerprint string         `json:"fingerprint"`
	TenantID    string         `json:"tenant_id"`
	ServiceName string         `json:"service_name"`
	RuleID      string         `json:"rule_id"`
	Title       string         `json:"title"`
	Severity    event.Severity `json:"severity"`
	Status      Status         `json:"status"`

	// EventCount is how many offending events have been folded into this
	// incident across every detection that has grouped into it.
	EventCount int64 `json:"event_count"`

	FirstSeenAt time.Time `json:"first_seen_at"`
	LastSeenAt  time.Time `json:"last_seen_at"`
	OpenedAt    time.Time `json:"opened_at"`

	// AcknowledgedAt and ResolvedAt are set only once the incident reaches those
	// states; a nil pointer means the transition has not happened.
	AcknowledgedAt *time.Time `json:"acknowledged_at,omitempty"`
	ResolvedAt     *time.Time `json:"resolved_at,omitempty"`
	UpdatedAt      time.Time  `json:"updated_at"`

	// Details carries the rule-specific evidence for the detection: the observed
	// rate, the threshold it crossed, the sample size and window. Stored as JSONB
	// so a new rule can attach new evidence without a migration.
	Details map[string]any `json:"details,omitempty"`
}

// Validate reports whether the incident is internally consistent enough to
// store. The correlation engine builds incidents from trusted, already-validated
// events, so this guards against programming mistakes rather than hostile input;
// it exists so a malformed incident fails at the boundary with a clear message
// instead of as a constraint violation deep in the store.
func (i Incident) Validate() error {
	var problems []string

	if strings.TrimSpace(i.Fingerprint) == "" {
		problems = append(problems, "fingerprint is required")
	}
	if strings.TrimSpace(i.TenantID) == "" {
		problems = append(problems, "tenant_id is required")
	}
	if strings.TrimSpace(i.ServiceName) == "" {
		problems = append(problems, "service_name is required")
	}
	if strings.TrimSpace(i.RuleID) == "" {
		problems = append(problems, "rule_id is required")
	}
	if strings.TrimSpace(i.Title) == "" {
		problems = append(problems, "title is required")
	}
	if !i.Status.Valid() {
		problems = append(problems, "status "+string(i.Status)+" is not recognised")
	}
	if _, ok := supportedSeverity(i.Severity); !ok {
		problems = append(problems, "severity "+string(i.Severity)+" is not recognised")
	}

	if len(problems) == 0 {
		return nil
	}
	return &ValidationError{Problems: problems}
}

// ValidationError reports why an incident could not be accepted.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return "invalid incident: " + strings.Join(e.Problems, "; ")
}

// supportedSeverity reports whether sev is one of the event contract's
// severities, reusing that vocabulary rather than defining a second one.
func supportedSeverity(sev event.Severity) (event.Severity, bool) {
	for _, s := range event.SupportedSeverities() {
		if string(sev) == s {
			return sev, true
		}
	}
	return "", false
}
