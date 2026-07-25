package event

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// reference is a fixed "now" so every bound in this file is exact.
var reference = time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)

// freshEvent is a structurally valid event at the reference time, so a test that
// fails is failing on the time bound and nothing else.
func freshEvent(ts time.Time) Event {
	return Event{
		EventID:       "0921316d-4496-4568-8638-2b0ef226f850",
		SchemaVersion: SchemaVersion10,
		TenantID:      "t",
		ServiceName:   "s",
		Environment:   "test",
		EventType:     "request.completed",
		Severity:      SeverityInfo,
		Timestamp:     NewTimestamp(ts),
	}
}

func TestTimeBoundsFutureLimit(t *testing.T) {
	bounds := TimeBounds{MaxFuture: 5 * time.Minute}

	tests := []struct {
		name    string
		ts      time.Time
		wantErr bool
	}{
		{"well within", reference.Add(time.Minute), false},
		{"exactly at the bound", reference.Add(5 * time.Minute), false},
		{"one nanosecond past", reference.Add(5*time.Minute + time.Nanosecond), true},
		{"far future", reference.AddDate(4, 0, 0), true},
		{"in the past is not a future breach", reference.AddDate(-1, 0, 0), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := freshEvent(tt.ts).ValidateWithin(bounds, reference)
			if tt.wantErr && err == nil {
				t.Fatal("ValidateWithin() = nil, want a future-bound violation")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ValidateWithin() = %v, want nil", err)
			}
		})
	}
}

func TestTimeBoundsAgeLimit(t *testing.T) {
	bounds := TimeBounds{MaxAge: 7 * 24 * time.Hour}

	tests := []struct {
		name    string
		ts      time.Time
		wantErr bool
	}{
		{"recent", reference.Add(-time.Hour), false},
		{"exactly at the bound", reference.Add(-7 * 24 * time.Hour), false},
		{"one nanosecond past", reference.Add(-7*24*time.Hour - time.Nanosecond), true},
		{"ancient", reference.AddDate(-5, 0, 0), true},
		{"in the future is not an age breach", reference.AddDate(1, 0, 0), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := freshEvent(tt.ts).ValidateWithin(bounds, reference)
			if tt.wantErr && err == nil {
				t.Fatal("ValidateWithin() = nil, want an age-bound violation")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ValidateWithin() = %v, want nil", err)
			}
		})
	}
}

func TestTimeBoundsZeroValueDisablesBothChecks(t *testing.T) {
	// The zero TimeBounds must be inert, so that a caller who has not opted in
	// keeps exactly the behaviour Validate always had.
	for _, ts := range []time.Time{reference.AddDate(100, 0, 0), reference.AddDate(-100, 0, 0)} {
		if err := freshEvent(ts).ValidateWithin(TimeBounds{}, reference); err != nil {
			t.Errorf("ValidateWithin(zero bounds) on %s = %v, want nil", ts, err)
		}
	}
}

func TestValidateIgnoresTimeEntirely(t *testing.T) {
	// Validate is the structural check and must stay a pure function of the
	// event: a wildly future-dated event is structurally fine.
	if err := freshEvent(reference.AddDate(50, 0, 0)).Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil — Validate must not police time", err)
	}
}

func TestValidateWithinReportsStructuralAndTimeProblemsTogether(t *testing.T) {
	// The API promises a 400 that lists everything wrong at once; a clock
	// problem must join that list rather than pre-empt it.
	ev := freshEvent(reference.AddDate(1, 0, 0))
	ev.EventID = "not-a-uuid"
	ev.Severity = "catastrophe"

	err := ev.ValidateWithin(TimeBounds{MaxFuture: time.Minute}, reference)
	if err == nil {
		t.Fatal("ValidateWithin() = nil, want errors")
	}

	var errs ValidationErrors
	if !errors.As(err, &errs) {
		t.Fatalf("error is %T, want ValidationErrors", err)
	}

	fields := make(map[string]bool)
	for _, fe := range errs {
		fields[fe.Field] = true
	}
	for _, want := range []string{"event_id", "severity", "timestamp"} {
		if !fields[want] {
			t.Errorf("missing a problem for %q; got %v", want, errs)
		}
	}
}

func TestTimeBoundsSkipsAMissingTimestamp(t *testing.T) {
	ev := freshEvent(time.Time{})

	err := ev.ValidateWithin(TimeBounds{MaxFuture: time.Minute, MaxAge: time.Minute}, reference)
	if err == nil {
		t.Fatal("ValidateWithin() = nil, want the required-field error")
	}

	var errs ValidationErrors
	if !errors.As(err, &errs) {
		t.Fatalf("error is %T, want ValidationErrors", err)
	}

	// One complaint about the timestamp, not three: a zero value is a missing
	// field, and also calling it two thousand years old is noise.
	n := 0
	for _, fe := range errs {
		if fe.Field == "timestamp" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("got %d timestamp errors, want exactly 1: %v", n, errs)
	}
}

func TestTimeBoundsMessageNamesTheClock(t *testing.T) {
	// The whole class of bug here is an unsynchronised producer, so the message
	// has to be actionable by whoever runs that host.
	err := freshEvent(reference.Add(time.Hour)).ValidateWithin(TimeBounds{MaxFuture: time.Minute}, reference)
	if err == nil {
		t.Fatal("ValidateWithin() = nil, want an error")
	}

	msg := err.Error()
	for _, want := range []string{"future", "clock", "server time"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q should mention %q", msg, want)
		}
	}
}

func TestFormatDriftStaysReadable(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want string
	}{
		{1500 * time.Millisecond, "1.5s"},
		{90 * time.Second, "1m30s"},
		{2 * time.Hour, "2h0m0s"},
		{47 * time.Hour, "47h0m0s"},
		// Past two days, hours stop conveying anything.
		{48 * time.Hour, "2.0 days"},
		{30140*time.Hour + 20*time.Minute, "1255.8 days"},
	}

	for _, tt := range tests {
		if got := formatDrift(tt.in); got != tt.want {
			t.Errorf("formatDrift(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
