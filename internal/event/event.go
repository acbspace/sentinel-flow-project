// Package event defines the versioned telemetry event contract that every
// SentinelFlow service produces, transports and stores, together with the
// normalization and validation rules applied at each trust boundary.
//
// The contract is deliberately transport agnostic: it knows nothing about HTTP,
// Kafka or PostgreSQL so that the ingestion API and the incident engine can
// apply exactly the same rules to the same bytes.
package event

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// SchemaVersion10 is the only event schema version currently accepted.
// Breaking changes to the contract require a new version string here plus an
// explicit decision about how long the old version stays supported.
const SchemaVersion10 = "1.0"

// Field length ceilings. These protect the database and the log pipeline from
// unbounded strings; they are generous enough that no legitimate producer will
// ever hit them.
const (
	MaxIdentifierLen = 128
	MaxTraceIDLen    = 128
)

// Severity classifies how much attention an event deserves. The set is closed
// so that downstream alerting rules can rely on a small, ordered vocabulary.
type Severity string

// Supported severities, ordered from least to most urgent.
const (
	SeverityDebug    Severity = "debug"
	SeverityInfo     Severity = "info"
	SeverityWarn     Severity = "warn"
	SeverityError    Severity = "error"
	SeverityCritical Severity = "critical"
)

var supportedSchemaVersions = map[string]struct{}{
	SchemaVersion10: {},
}

var supportedSeverities = map[Severity]struct{}{
	SeverityDebug:    {},
	SeverityInfo:     {},
	SeverityWarn:     {},
	SeverityError:    {},
	SeverityCritical: {},
}

// DefaultEnvironment is applied when a producer omits the environment field.
const DefaultEnvironment = "unknown"

// Attributes carries producer-defined key/value context. It is stored verbatim
// as JSONB so that new attribute keys never require a schema migration.
type Attributes map[string]any

// Timestamp is a time.Time that accepts only RFC3339 on the wire. The custom
// decoder exists purely to turn Go's low level parse failure into a message
// that names the offending value, which the ingestion API returns to callers.
type Timestamp struct {
	time.Time
}

// NewTimestamp wraps t for transport.
func NewTimestamp(t time.Time) Timestamp {
	return Timestamp{Time: t}
}

// UnmarshalJSON decodes an RFC3339 string.
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("timestamp must be an RFC3339 string")
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return fmt.Errorf("timestamp %q is not a valid RFC3339 timestamp", raw)
	}
	t.Time = parsed
	return nil
}

// MarshalJSON encodes the timestamp as RFC3339 with nanosecond precision.
func (t Timestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time.Format(time.RFC3339Nano))
}

// Event is a single unit of telemetry emitted by an instrumented service.
type Event struct {
	EventID       string     `json:"event_id"`
	SchemaVersion string     `json:"schema_version"`
	TenantID      string     `json:"tenant_id"`
	ServiceName   string     `json:"service_name"`
	Environment   string     `json:"environment"`
	EventType     string     `json:"event_type"`
	Severity      Severity   `json:"severity"`
	Timestamp     Timestamp  `json:"timestamp"`
	TraceID       string     `json:"trace_id"`
	Attributes    Attributes `json:"attributes,omitempty"`
}

// PartitionKey is the logical identity used to order events. Every event from
// one service of one tenant lands on the same partition, which keeps that
// stream in order without forcing a single global partition.
func (e Event) PartitionKey() string {
	return e.TenantID + ":" + e.ServiceName
}

// Normalize trims incidental whitespace and fills in defaults. It runs before
// Validate so that a field containing only spaces is reported as empty, and
// before persistence so that the database never sees two spellings of one
// tenant. Normalize is idempotent.
func (e *Event) Normalize() {
	e.EventID = strings.ToLower(strings.TrimSpace(e.EventID))
	e.SchemaVersion = strings.TrimSpace(e.SchemaVersion)
	e.TenantID = strings.TrimSpace(e.TenantID)
	e.ServiceName = strings.TrimSpace(e.ServiceName)
	e.Environment = strings.TrimSpace(e.Environment)
	e.EventType = strings.TrimSpace(e.EventType)
	e.Severity = Severity(strings.ToLower(strings.TrimSpace(string(e.Severity))))
	e.TraceID = strings.TrimSpace(e.TraceID)

	if e.Environment == "" {
		e.Environment = DefaultEnvironment
	}
	if e.Attributes == nil {
		e.Attributes = Attributes{}
	}
	if !e.Timestamp.IsZero() {
		e.Timestamp = Timestamp{Time: e.Timestamp.UTC()}
	}
}

// FieldError describes one rejected field.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationErrors is the full set of problems found in a single event. All
// problems are reported together so a producer can fix them in one pass.
type ValidationErrors []FieldError

func (v ValidationErrors) Error() string {
	if len(v) == 0 {
		return "event is valid"
	}
	parts := make([]string, 0, len(v))
	for _, fe := range v {
		parts = append(parts, fe.Field+": "+fe.Message)
	}
	return "invalid event: " + strings.Join(parts, "; ")
}

// Validate reports every structural contract violation in the event. It is
// called at both trust boundaries: once when the ingestion API accepts an event
// over HTTP, and again when the incident engine reads it back off Kafka,
// because the topic can also be written to by producers the API never saw.
//
// It deliberately takes no clock and applies no time bounds, so that it stays a
// pure function of the event. Callers that must also police the timestamp
// against the current time use ValidateWithin.
func (e Event) Validate() error {
	if errs := e.validate(); len(errs) > 0 {
		return errs
	}
	return nil
}

// validate collects the structural problems without deciding how to report
// them, so that Validate and ValidateWithin build one combined error set rather
// than two competing ones.
func (e Event) validate() ValidationErrors {
	var errs ValidationErrors

	if e.EventID == "" {
		errs = append(errs, FieldError{"event_id", "is required"})
	} else if _, err := uuid.Parse(e.EventID); err != nil {
		errs = append(errs, FieldError{"event_id", "must be a valid UUID"})
	}

	switch {
	case e.SchemaVersion == "":
		errs = append(errs, FieldError{"schema_version", "is required"})
	default:
		if _, ok := supportedSchemaVersions[e.SchemaVersion]; !ok {
			errs = append(errs, FieldError{
				"schema_version",
				fmt.Sprintf("%q is not a supported schema version (supported: %s)",
					e.SchemaVersion, strings.Join(SupportedSchemaVersions(), ", ")),
			})
		}
	}

	errs = appendStringErrors(errs, "tenant_id", e.TenantID, MaxIdentifierLen)
	errs = appendStringErrors(errs, "service_name", e.ServiceName, MaxIdentifierLen)
	errs = appendStringErrors(errs, "event_type", e.EventType, MaxIdentifierLen)

	if len(e.Environment) > MaxIdentifierLen {
		errs = append(errs, FieldError{"environment", fmt.Sprintf("must be at most %d characters", MaxIdentifierLen)})
	}

	if e.Severity == "" {
		errs = append(errs, FieldError{"severity", "is required"})
	} else if _, ok := supportedSeverities[e.Severity]; !ok {
		errs = append(errs, FieldError{
			"severity",
			fmt.Sprintf("%q is not a supported severity (supported: %s)",
				e.Severity, strings.Join(SupportedSeverities(), ", ")),
		})
	}

	if e.Timestamp.IsZero() {
		errs = append(errs, FieldError{"timestamp", "is required and must be an RFC3339 timestamp"})
	}

	if len(e.TraceID) > MaxTraceIDLen {
		errs = append(errs, FieldError{"trace_id", fmt.Sprintf("must be at most %d characters", MaxTraceIDLen)})
	}

	return errs
}

func appendStringErrors(errs ValidationErrors, field, value string, maxLen int) ValidationErrors {
	switch {
	case value == "":
		return append(errs, FieldError{field, "is required"})
	case len(value) > maxLen:
		return append(errs, FieldError{field, fmt.Sprintf("must be at most %d characters", maxLen)})
	}
	return errs
}

// SupportedSchemaVersions lists the accepted schema versions in sorted order.
func SupportedSchemaVersions() []string {
	out := make([]string, 0, len(supportedSchemaVersions))
	for v := range supportedSchemaVersions {
		out = append(out, v)
	}
	sortStrings(out)
	return out
}

// SupportedSeverities lists the accepted severities from least to most urgent.
func SupportedSeverities() []string {
	return []string{
		string(SeverityDebug),
		string(SeverityInfo),
		string(SeverityWarn),
		string(SeverityError),
		string(SeverityCritical),
	}
}

// sortStrings is a tiny insertion sort; the slices involved have a handful of
// entries and this keeps the package free of extra imports.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
