// Package runbook defines what SentinelFlow is allowed to do about an incident
// automatically: an ordered list of steps, each either safe enough to run on its
// own or gated behind a human approval.
//
// Like the event, incident and oncall packages this is pure domain — no Temporal,
// no HTTP, no database — so the remediation workflow and its tests share one
// definition of "which runbook applies and what may run unattended".
package runbook

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ActionKind names what a step actually does.
type ActionKind string

const (
	// ActionNoop records the step and does nothing else. It is the safe default
	// and what the demo runbook uses for diagnostic steps.
	ActionNoop ActionKind = "noop"
	// ActionWebhook POSTs the step's context to a URL. It is the seam where real
	// automation (restart a deployment, drain a node, roll back a release) hangs
	// off, without this project shipping the ability to do those things itself.
	ActionWebhook ActionKind = "webhook"
)

// Mode decides whether a step runs unattended or waits for a human.
type Mode string

const (
	// ModeAuto runs as soon as the step is reached.
	ModeAuto Mode = "auto"
	// ModeApproval pauses the runbook until somebody approves or rejects.
	ModeApproval Mode = "approval"
)

// Step is one action in a runbook.
type Step struct {
	Name string     `json:"name"`
	Kind ActionKind `json:"kind"`
	Mode Mode       `json:"mode"`
	// Target is the webhook URL for ActionWebhook; unused by ActionNoop.
	Target string `json:"target,omitempty"`
	// Params is free-form context handed to the action and recorded in the audit
	// trail, so an operator can see exactly what was asked for.
	Params map[string]string `json:"params,omitempty"`
}

// Matcher selects which incidents a runbook applies to. An empty field matches
// anything; RuleID is required so a runbook can never apply to everything by
// accident.
type Matcher struct {
	RuleID      string `json:"rule_id"`
	ServiceName string `json:"service_name,omitempty"`
	Severity    string `json:"severity,omitempty"`
}

// Matches reports whether an incident's identity satisfies the matcher.
func (m Matcher) Matches(ruleID, serviceName, severity string) bool {
	if m.RuleID != ruleID {
		return false
	}
	if m.ServiceName != "" && m.ServiceName != serviceName {
		return false
	}
	if m.Severity != "" && m.Severity != severity {
		return false
	}
	return true
}

// Runbook is the automated response to a class of incident.
type Runbook struct {
	ID    string
	Name  string
	Match Matcher
	Steps []Step
	// ApprovalTimeout bounds how long an approval step waits. On expiry the step
	// is recorded as timed out and the runbook halts rather than proceeding
	// unattended.
	ApprovalTimeout time.Duration
}

// Catalog is the ordered set of runbooks; the first match wins.
type Catalog struct {
	Runbooks []Runbook
}

// Find returns the first runbook matching the incident, if any.
func (c Catalog) Find(ruleID, serviceName, severity string) (Runbook, bool) {
	for _, rb := range c.Runbooks {
		if rb.Match.Matches(ruleID, serviceName, severity) {
			return rb, true
		}
	}
	return Runbook{}, false
}

// Validate reports whether the catalog is well formed enough to execute.
func (c Catalog) Validate() error {
	if len(c.Runbooks) == 0 {
		return errors.New("runbook catalog must contain at least one runbook")
	}

	seen := make(map[string]struct{}, len(c.Runbooks))
	for _, rb := range c.Runbooks {
		if strings.TrimSpace(rb.ID) == "" {
			return errors.New("every runbook needs an id")
		}
		if _, dup := seen[rb.ID]; dup {
			return fmt.Errorf("runbook id %q is defined more than once", rb.ID)
		}
		seen[rb.ID] = struct{}{}

		if strings.TrimSpace(rb.Match.RuleID) == "" {
			return fmt.Errorf("runbook %q: match.rule_id is required", rb.ID)
		}
		if len(rb.Steps) == 0 {
			return fmt.Errorf("runbook %q: at least one step is required", rb.ID)
		}
		if rb.ApprovalTimeout <= 0 {
			return fmt.Errorf("runbook %q: approval_timeout must be greater than zero", rb.ID)
		}

		for i, s := range rb.Steps {
			n := i + 1
			if strings.TrimSpace(s.Name) == "" {
				return fmt.Errorf("runbook %q step %d: name is required", rb.ID, n)
			}
			switch s.Kind {
			case ActionNoop:
			case ActionWebhook:
				if strings.TrimSpace(s.Target) == "" {
					return fmt.Errorf("runbook %q step %d: a webhook step needs a target", rb.ID, n)
				}
			default:
				return fmt.Errorf("runbook %q step %d: unknown action kind %q", rb.ID, n, s.Kind)
			}
			if s.Mode != ModeAuto && s.Mode != ModeApproval {
				return fmt.Errorf("runbook %q step %d: mode must be %q or %q", rb.ID, n, ModeAuto, ModeApproval)
			}
		}
	}
	return nil
}

// catalogWire is the JSON shape. approval_timeout is a duration string so a
// catalog file stays readable.
type catalogWire struct {
	Runbooks []struct {
		ID              string  `json:"id"`
		Name            string  `json:"name"`
		Match           Matcher `json:"match"`
		ApprovalTimeout string  `json:"approval_timeout"`
		Steps           []Step  `json:"steps"`
	} `json:"runbooks"`
}

// ParseCatalog decodes a catalog from JSON and validates it. Unknown fields are
// rejected: in a file that decides what runs automatically against production, a
// silently ignored key is unacceptable.
func ParseCatalog(data []byte) (Catalog, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	var wire catalogWire
	if err := dec.Decode(&wire); err != nil {
		return Catalog{}, fmt.Errorf("parse runbook catalog: %w", err)
	}

	var catalog Catalog
	for _, rb := range wire.Runbooks {
		timeout, err := time.ParseDuration(rb.ApprovalTimeout)
		if err != nil {
			return Catalog{}, fmt.Errorf("runbook %q: approval_timeout %q is not a valid duration: %w", rb.ID, rb.ApprovalTimeout, err)
		}
		catalog.Runbooks = append(catalog.Runbooks, Runbook{
			ID:              rb.ID,
			Name:            rb.Name,
			Match:           rb.Match,
			Steps:           rb.Steps,
			ApprovalTimeout: timeout,
		})
	}

	if err := catalog.Validate(); err != nil {
		return Catalog{}, err
	}
	return catalog, nil
}

//go:embed runbooks.json
var defaultCatalogJSON []byte

// DefaultCatalog returns the catalog embedded in the binary. A deployment
// overrides it by supplying its own JSON.
func DefaultCatalog() (Catalog, error) {
	return ParseCatalog(defaultCatalogJSON)
}
