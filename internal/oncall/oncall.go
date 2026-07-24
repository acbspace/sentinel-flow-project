// Package oncall defines the escalation policy and on-call schedule the alerting
// workflow follows: an ordered list of levels, each with a responder rotation and
// the time an incident may go unacknowledged before it escalates to the next.
//
// Like the event and incident packages, this is pure domain: it knows nothing
// about Temporal, HTTP or the database, so the workflow and its tests share one
// definition of "who is on call and when do we escalate".
package oncall

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Contact is a responder that can be notified.
type Contact struct {
	Name string `json:"name"`
	// Address is a delivery hint (a webhook URL, a channel, an email). It is
	// optional: notifications are always recorded by contact name, and webhook
	// delivery falls back to a globally configured URL when this is empty.
	Address string `json:"address,omitempty"`
}

// Rotation resolves who is on call at a given instant by cycling its contacts on
// a fixed shift length. A rotation of a single contact — or one with no shift —
// is a permanent assignment.
type Rotation struct {
	Contacts []Contact
	Start    time.Time
	Shift    time.Duration
}

// OnCallAt returns the contact on call at t. Before Start, or for a
// single-contact or shiftless rotation, it returns the first contact.
func (r Rotation) OnCallAt(t time.Time) Contact {
	switch {
	case len(r.Contacts) == 0:
		return Contact{}
	case len(r.Contacts) == 1 || r.Shift <= 0:
		return r.Contacts[0]
	}

	elapsed := t.Sub(r.Start)
	if elapsed < 0 {
		return r.Contacts[0]
	}
	shifts := int64(elapsed / r.Shift)
	return r.Contacts[shifts%int64(len(r.Contacts))]
}

// Level is one rung of an escalation policy: who to page, and how long to wait
// for an acknowledgement before escalating to the next level.
type Level struct {
	// Target is a human label for the rung, e.g. "primary on-call".
	Target     string
	AckTimeout time.Duration
	Rotation   Rotation
}

// EscalationPolicy is the ordered set of levels an unacknowledged incident climbs.
type EscalationPolicy struct {
	Levels []Level
}

// Validate reports whether the policy is well formed enough to drive a workflow.
func (p EscalationPolicy) Validate() error {
	if len(p.Levels) == 0 {
		return errors.New("escalation policy must have at least one level")
	}
	for i, l := range p.Levels {
		n := i + 1
		if strings.TrimSpace(l.Target) == "" {
			return fmt.Errorf("level %d: target is required", n)
		}
		if l.AckTimeout <= 0 {
			return fmt.Errorf("level %d: ack_timeout must be greater than zero", n)
		}
		if len(l.Rotation.Contacts) == 0 {
			return fmt.Errorf("level %d: at least one contact is required", n)
		}
		for j, c := range l.Rotation.Contacts {
			if strings.TrimSpace(c.Name) == "" {
				return fmt.Errorf("level %d, contact %d: name is required", n, j+1)
			}
		}
		if l.Rotation.Shift < 0 {
			return fmt.Errorf("level %d: shift must not be negative", n)
		}
	}
	return nil
}

// policyWire is the JSON shape of a policy. Durations and the rotation start are
// strings on the wire (e.g. "2m", "168h", RFC3339) so a policy file is readable
// and hand-editable, rather than raw nanoseconds.
type policyWire struct {
	Levels []struct {
		Target     string `json:"target"`
		AckTimeout string `json:"ack_timeout"`
		Rotation   struct {
			Contacts []Contact `json:"contacts"`
			Start    string    `json:"start"`
			Shift    string    `json:"shift"`
		} `json:"rotation"`
	} `json:"levels"`
}

// ParsePolicy decodes a policy from JSON and validates it. Unknown fields are
// rejected so a misspelled key is caught rather than silently ignored.
func ParsePolicy(data []byte) (EscalationPolicy, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	var wire policyWire
	if err := dec.Decode(&wire); err != nil {
		return EscalationPolicy{}, fmt.Errorf("parse escalation policy: %w", err)
	}

	var policy EscalationPolicy
	for i, l := range wire.Levels {
		n := i + 1

		ackTimeout, err := time.ParseDuration(l.AckTimeout)
		if err != nil {
			return EscalationPolicy{}, fmt.Errorf("level %d: ack_timeout %q is not a valid duration: %w", n, l.AckTimeout, err)
		}

		var shift time.Duration
		if l.Rotation.Shift != "" {
			shift, err = time.ParseDuration(l.Rotation.Shift)
			if err != nil {
				return EscalationPolicy{}, fmt.Errorf("level %d: shift %q is not a valid duration: %w", n, l.Rotation.Shift, err)
			}
		}

		var start time.Time
		if l.Rotation.Start != "" {
			start, err = time.Parse(time.RFC3339, l.Rotation.Start)
			if err != nil {
				return EscalationPolicy{}, fmt.Errorf("level %d: start %q is not an RFC3339 timestamp: %w", n, l.Rotation.Start, err)
			}
		}

		policy.Levels = append(policy.Levels, Level{
			Target:     l.Target,
			AckTimeout: ackTimeout,
			Rotation:   Rotation{Contacts: l.Rotation.Contacts, Start: start, Shift: shift},
		})
	}

	if err := policy.Validate(); err != nil {
		return EscalationPolicy{}, err
	}
	return policy, nil
}

//go:embed policy.json
var defaultPolicyJSON []byte

// DefaultPolicy returns the escalation policy embedded in the binary. A
// deployment overrides it by supplying its own JSON; this is the fallback.
func DefaultPolicy() (EscalationPolicy, error) {
	return ParsePolicy(defaultPolicyJSON)
}
