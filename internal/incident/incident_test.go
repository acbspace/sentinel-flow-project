package incident_test

import (
	"errors"
	"testing"

	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/incident"
)

func TestStatusCanTransitionTo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from incident.Status
		to   incident.Status
		want bool
	}{
		{"open can be acknowledged", incident.StatusOpen, incident.StatusAcknowledged, true},
		{"open can be resolved directly", incident.StatusOpen, incident.StatusResolved, true},
		{"open cannot transition to itself", incident.StatusOpen, incident.StatusOpen, false},
		{"acknowledged can be resolved", incident.StatusAcknowledged, incident.StatusResolved, true},
		{"acknowledged cannot be reopened", incident.StatusAcknowledged, incident.StatusOpen, false},
		{"acknowledged cannot re-acknowledge", incident.StatusAcknowledged, incident.StatusAcknowledged, false},
		{"resolved is terminal to open", incident.StatusResolved, incident.StatusOpen, false},
		{"resolved is terminal to acknowledged", incident.StatusResolved, incident.StatusAcknowledged, false},
		{"resolved cannot re-resolve", incident.StatusResolved, incident.StatusResolved, false},
		{"unknown source rejects everything", incident.Status("bogus"), incident.StatusResolved, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.from.CanTransitionTo(tc.to); got != tc.want {
				t.Errorf("Status(%q).CanTransitionTo(%q) = %v, want %v", tc.from, tc.to, got, tc.want)
			}
		})
	}
}

func TestStatusActive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status incident.Status
		want   bool
	}{
		{incident.StatusOpen, true},
		{incident.StatusAcknowledged, true},
		{incident.StatusResolved, false},
		{incident.Status("bogus"), false},
	}

	for _, tc := range tests {
		t.Run(string(tc.status), func(t *testing.T) {
			t.Parallel()

			if got := tc.status.Active(); got != tc.want {
				t.Errorf("Status(%q).Active() = %v, want %v", tc.status, got, tc.want)
			}
		})
	}
}

func TestStatusValid(t *testing.T) {
	t.Parallel()

	for _, s := range []incident.Status{incident.StatusOpen, incident.StatusAcknowledged, incident.StatusResolved} {
		if !s.Valid() {
			t.Errorf("Status(%q).Valid() = false, want true", s)
		}
	}
	if incident.Status("bogus").Valid() {
		t.Error(`Status("bogus").Valid() = true, want false`)
	}
}

func TestFingerprint(t *testing.T) {
	t.Parallel()

	base := incident.Fingerprint("error_rate", "tenant-a", "payment-service")

	// Deterministic: the same inputs always produce the same fingerprint.
	if again := incident.Fingerprint("error_rate", "tenant-a", "payment-service"); again != base {
		t.Errorf("Fingerprint is not deterministic: %q != %q", again, base)
	}

	// Every dimension participates: changing any one changes the fingerprint.
	others := []string{
		incident.Fingerprint("high_latency", "tenant-a", "payment-service"),
		incident.Fingerprint("error_rate", "tenant-b", "payment-service"),
		incident.Fingerprint("error_rate", "tenant-a", "order-service"),
	}
	for _, other := range others {
		if other == base {
			t.Errorf("Fingerprint collision with %q", base)
		}
	}

	// A separator that cannot appear in the inputs prevents the classic
	// ambiguity where ("ab","c") and ("a","bc") collapse to the same key.
	left := incident.Fingerprint("rule", "ab", "c")
	right := incident.Fingerprint("rule", "a", "bc")
	if left == right {
		t.Errorf("Fingerprint boundary collision: %q == %q", left, right)
	}
}

func TestIncidentValidate(t *testing.T) {
	t.Parallel()

	valid := incident.Incident{
		Fingerprint: "error_rate\x1ftenant-a\x1fpayment-service",
		TenantID:    "tenant-a",
		ServiceName: "payment-service",
		RuleID:      "error_rate",
		Title:       "elevated error rate on payment-service",
		Severity:    event.SeverityError,
		Status:      incident.StatusOpen,
	}

	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() on a well-formed incident returned %v, want nil", err)
	}

	mutate := func(f func(*incident.Incident)) incident.Incident {
		clone := valid
		f(&clone)
		return clone
	}

	tests := []struct {
		name string
		inc  incident.Incident
	}{
		{"missing fingerprint", mutate(func(i *incident.Incident) { i.Fingerprint = "  " })},
		{"missing tenant", mutate(func(i *incident.Incident) { i.TenantID = "" })},
		{"missing service", mutate(func(i *incident.Incident) { i.ServiceName = "" })},
		{"missing rule", mutate(func(i *incident.Incident) { i.RuleID = "" })},
		{"missing title", mutate(func(i *incident.Incident) { i.Title = "" })},
		{"unknown status", mutate(func(i *incident.Incident) { i.Status = "bogus" })},
		{"unknown severity", mutate(func(i *incident.Incident) { i.Severity = "nuclear" })},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.inc.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil, want an error for %s", tc.name)
			}
			var ve *incident.ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("Validate() returned %T, want *incident.ValidationError", err)
			}
		})
	}
}
