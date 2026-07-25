package event

import (
	"fmt"
	"time"
)

// TimeBounds constrains how far an event's timestamp may sit either side of the
// moment it is examined.
//
// The two bounds are separate fields rather than one symmetric window because
// they are enforced in different places, for different reasons, and applying
// both everywhere would destroy data. See Event.ValidateWithin for the full
// argument.
type TimeBounds struct {
	// MaxFuture is how far ahead of now a timestamp may be. Zero or less
	// disables the check.
	MaxFuture time.Duration

	// MaxAge is how far behind now a timestamp may be. Zero or less disables the
	// check.
	MaxAge time.Duration
}

// ValidateWithin reports every contract violation Validate finds, plus any
// breach of the supplied time bounds, as one combined set — the API answers a
// single 400 listing everything wrong with an event, and a clock problem is no
// exception.
//
// # Why the bounds are asymmetric
//
// This runs at two trust boundaries and they must not be configured alike.
//
// The ingestion API enforces both bounds. It is the only place that can tell a
// producer its clock is wrong while the producer is still listening, and a
// future-dated event is not merely untidy: the correlation engine's window
// query is `event_timestamp >= now() - window`, so a row dated next year
// matches every window forever. It holds an incident open permanently and
// auto-resolution never fires, because the incident never goes quiet.
//
// The incident engine enforces only MaxFuture, and this is the important part.
// An event's timestamp is producer-supplied and fixed at creation; sitting in
// Kafka does not change it, but it does make the event *older*. If the engine
// re-applied MaxAge, then any backlog older than that bound — an outage, a slow
// consumer, a paused partition, exactly the situations where losing telemetry
// hurts most — would be classified as permanently invalid, logged, and have its
// offset committed. That is silent data loss dressed up as validation.
//
// MaxFuture is safe to re-apply precisely because it cannot have that effect:
// now only moves forward, so an event that satisfied the future bound at
// ingestion still satisfies it at every later moment. Re-checking it costs
// nothing and closes the gap that the topic is not private to the ingestion API
// — a rogue producer writing straight to Kafka is still caught.
func (e Event) ValidateWithin(bounds TimeBounds, now time.Time) error {
	errs := e.validate()
	errs = append(errs, bounds.check(e.Timestamp.Time, now)...)

	if len(errs) > 0 {
		return errs
	}
	return nil
}

// check reports the bounds that ts breaches.
//
// A zero timestamp yields nothing: it is already reported as a missing required
// field by the structural validation, and adding "and it is 2025 years old"
// would be noise rather than help.
func (b TimeBounds) check(ts time.Time, now time.Time) []FieldError {
	if ts.IsZero() {
		return nil
	}

	var errs []FieldError

	if b.MaxFuture > 0 {
		if ahead := ts.Sub(now); ahead > b.MaxFuture {
			errs = append(errs, FieldError{
				"timestamp",
				fmt.Sprintf("is %s in the future, which exceeds the %s limit (server time is %s); check the sending host's clock",
					formatDrift(ahead), b.MaxFuture, now.UTC().Format(time.RFC3339)),
			})
		}
	}

	if b.MaxAge > 0 {
		if age := now.Sub(ts); age > b.MaxAge {
			errs = append(errs, FieldError{
				"timestamp",
				fmt.Sprintf("is %s old, which exceeds the %s limit (server time is %s)",
					formatDrift(age), b.MaxAge, now.UTC().Format(time.RFC3339)),
			})
		}
	}

	return errs
}

// formatDrift renders how far out a timestamp is, at a precision worth reading.
//
// The exact nanosecond an event is out by has never helped anyone find a clock
// bug, and past a couple of days hours stop being comprehensible at all:
// "30140h20m0s in the future" is the correct answer to the question and no help
// whatsoever in answering "by how much?".
func formatDrift(d time.Duration) string {
	switch {
	case d >= 48*time.Hour:
		return fmt.Sprintf("%.1f days", d.Hours()/24)
	case d >= time.Hour:
		return d.Round(time.Minute).String()
	case d >= time.Minute:
		return d.Round(time.Second).String()
	default:
		return d.Round(time.Millisecond).String()
	}
}
