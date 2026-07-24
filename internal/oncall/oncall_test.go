package oncall_test

import (
	"strings"
	"testing"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/oncall"
)

func TestRotationOnCallAt(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	week := 168 * time.Hour
	a := oncall.Contact{Name: "a"}
	b := oncall.Contact{Name: "b"}
	c := oncall.Contact{Name: "c"}

	weekly := oncall.Rotation{Contacts: []oncall.Contact{a, b, c}, Start: start, Shift: week}

	tests := []struct {
		name string
		rot  oncall.Rotation
		at   time.Time
		want oncall.Contact
	}{
		{"empty rotation yields zero contact", oncall.Rotation{}, start, oncall.Contact{}},
		{"single contact is permanent", oncall.Rotation{Contacts: []oncall.Contact{a}}, start.Add(9999 * time.Hour), a},
		{"no shift pins the first contact", oncall.Rotation{Contacts: []oncall.Contact{a, b}}, start.Add(week), a},
		{"before start uses the first contact", weekly, start.Add(-time.Hour), a},
		{"first week is a", weekly, start.Add(3 * time.Hour), a},
		{"second week is b", weekly, start.Add(week + time.Hour), b},
		{"third week is c", weekly, start.Add(2*week + time.Hour), c},
		{"fourth week wraps to a", weekly, start.Add(3*week + time.Hour), a},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.rot.OnCallAt(tc.at); got != tc.want {
				t.Errorf("OnCallAt(%v) = %+v, want %+v", tc.at, got, tc.want)
			}
		})
	}
}

func TestDefaultPolicyIsValid(t *testing.T) {
	t.Parallel()

	// The embedded policy is a build-time artifact; this pins that it parses,
	// validates, and carries the escalation shape the workflow relies on.
	p, err := oncall.DefaultPolicy()
	if err != nil {
		t.Fatalf("DefaultPolicy() = %v", err)
	}
	if len(p.Levels) != 3 {
		t.Fatalf("default policy has %d levels, want 3", len(p.Levels))
	}
	wantTimeouts := []time.Duration{2 * time.Minute, 5 * time.Minute, 10 * time.Minute}
	for i, want := range wantTimeouts {
		if p.Levels[i].AckTimeout != want {
			t.Errorf("level %d ack_timeout = %v, want %v", i+1, p.Levels[i].AckTimeout, want)
		}
	}
	// The primary level rotates two contacts weekly.
	if got := len(p.Levels[0].Rotation.Contacts); got != 2 {
		t.Errorf("primary level has %d contacts, want 2", got)
	}
}

func TestParsePolicyValid(t *testing.T) {
	t.Parallel()

	const doc = `{
	  "levels": [
	    {"target": "primary", "ack_timeout": "90s",
	     "rotation": {"contacts": [{"name": "alice"}, {"name": "bob"}], "start": "2026-01-01T00:00:00Z", "shift": "24h"}},
	    {"target": "manager", "ack_timeout": "5m",
	     "rotation": {"contacts": [{"name": "carol"}]}}
	  ]
	}`

	p, err := oncall.ParsePolicy([]byte(doc))
	if err != nil {
		t.Fatalf("ParsePolicy() = %v", err)
	}
	if len(p.Levels) != 2 {
		t.Fatalf("parsed %d levels, want 2", len(p.Levels))
	}
	if p.Levels[0].AckTimeout != 90*time.Second {
		t.Errorf("level 1 ack_timeout = %v, want 90s", p.Levels[0].AckTimeout)
	}
	if p.Levels[0].Rotation.Shift != 24*time.Hour {
		t.Errorf("level 1 shift = %v, want 24h", p.Levels[0].Rotation.Shift)
	}
}

func TestParsePolicyRejectsBadInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		doc     string
		wantSub string
	}{
		{"no levels", `{"levels": []}`, "at least one level"},
		{"missing target", `{"levels": [{"ack_timeout": "1m", "rotation": {"contacts": [{"name": "a"}]}}]}`, "target is required"},
		{"bad ack_timeout", `{"levels": [{"target": "t", "ack_timeout": "soon", "rotation": {"contacts": [{"name": "a"}]}}]}`, "ack_timeout"},
		{"no contacts", `{"levels": [{"target": "t", "ack_timeout": "1m", "rotation": {"contacts": []}}]}`, "at least one contact"},
		{"nameless contact", `{"levels": [{"target": "t", "ack_timeout": "1m", "rotation": {"contacts": [{"name": ""}]}}]}`, "name is required"},
		{"unknown field", `{"levels": [], "extra": true}`, "parse escalation policy"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := oncall.ParsePolicy([]byte(tc.doc))
			if err == nil {
				t.Fatalf("ParsePolicy(%s) = nil, want an error", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not mention %q", err, tc.wantSub)
			}
		})
	}
}
