package event_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/event"
)

const validUUID = "6f1c4d5e-8b3a-4c2d-9e7f-1a2b3c4d5e6f"

// newValidEvent returns an event that passes validation, so each test case only
// has to describe the one thing it changes.
func newValidEvent() event.Event {
	return event.Event{
		EventID:       validUUID,
		SchemaVersion: event.SchemaVersion10,
		TenantID:      "demo-tenant",
		ServiceName:   "payment-service",
		Environment:   "local",
		EventType:     "request.completed",
		Severity:      event.SeverityInfo,
		Timestamp:     event.NewTimestamp(time.Date(2026, 7, 22, 10, 30, 0, 0, time.UTC)),
		TraceID:       "0af7651916cd43dd8448eb211c80319c",
		Attributes: event.Attributes{
			"http_method":      "POST",
			"http_status_code": 200,
		},
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// mutate applies the single deviation under test.
		mutate     func(*event.Event)
		wantValid  bool
		wantFields []string
	}{
		{
			name:      "fully populated event is valid",
			mutate:    func(*event.Event) {},
			wantValid: true,
		},
		{
			name:      "attributes may be empty",
			mutate:    func(e *event.Event) { e.Attributes = nil },
			wantValid: true,
		},
		{
			name:      "trace id may be empty",
			mutate:    func(e *event.Event) { e.TraceID = "" },
			wantValid: true,
		},
		{
			name:       "event id must be present",
			mutate:     func(e *event.Event) { e.EventID = "" },
			wantFields: []string{"event_id"},
		},
		{
			name:       "event id must be a uuid",
			mutate:     func(e *event.Event) { e.EventID = "not-a-uuid" },
			wantFields: []string{"event_id"},
		},
		{
			name:       "schema version must be supported",
			mutate:     func(e *event.Event) { e.SchemaVersion = "2.0" },
			wantFields: []string{"schema_version"},
		},
		{
			name:       "schema version must be present",
			mutate:     func(e *event.Event) { e.SchemaVersion = "" },
			wantFields: []string{"schema_version"},
		},
		{
			name:       "tenant id must be present",
			mutate:     func(e *event.Event) { e.TenantID = "" },
			wantFields: []string{"tenant_id"},
		},
		{
			name:       "tenant id has a length ceiling",
			mutate:     func(e *event.Event) { e.TenantID = strings.Repeat("t", event.MaxIdentifierLen+1) },
			wantFields: []string{"tenant_id"},
		},
		{
			name:       "service name must be present",
			mutate:     func(e *event.Event) { e.ServiceName = "" },
			wantFields: []string{"service_name"},
		},
		{
			name:       "event type must be present",
			mutate:     func(e *event.Event) { e.EventType = "" },
			wantFields: []string{"event_type"},
		},
		{
			name:       "severity must be present",
			mutate:     func(e *event.Event) { e.Severity = "" },
			wantFields: []string{"severity"},
		},
		{
			name:       "severity must be from the supported set",
			mutate:     func(e *event.Event) { e.Severity = "catastrophic" },
			wantFields: []string{"severity"},
		},
		{
			name:       "timestamp must be set",
			mutate:     func(e *event.Event) { e.Timestamp = event.Timestamp{} },
			wantFields: []string{"timestamp"},
		},
		{
			name:       "trace id has a length ceiling",
			mutate:     func(e *event.Event) { e.TraceID = strings.Repeat("a", event.MaxTraceIDLen+1) },
			wantFields: []string{"trace_id"},
		},
		{
			name: "every problem is reported at once",
			mutate: func(e *event.Event) {
				e.EventID = "nope"
				e.TenantID = ""
				e.Severity = "loud"
			},
			wantFields: []string{"event_id", "tenant_id", "severity"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ev := newValidEvent()
			tc.mutate(&ev)

			err := ev.Validate()

			if tc.wantValid {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("Validate() = nil, want errors on fields %v", tc.wantFields)
			}

			var validationErrs event.ValidationErrors
			if !errors.As(err, &validationErrs) {
				t.Fatalf("Validate() returned %T, want event.ValidationErrors", err)
			}

			got := make(map[string]bool, len(validationErrs))
			for _, fe := range validationErrs {
				got[fe.Field] = true
				if fe.Message == "" {
					t.Errorf("field %q reported with an empty message", fe.Field)
				}
			}

			for _, field := range tc.wantFields {
				if !got[field] {
					t.Errorf("missing error for field %q; got %v", field, err)
				}
			}
			if len(got) != len(tc.wantFields) {
				t.Errorf("got errors for %d fields, want %d: %v", len(got), len(tc.wantFields), err)
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input event.Event
		check func(*testing.T, event.Event)
	}{
		{
			name: "trims surrounding whitespace",
			input: event.Event{
				EventID:     "  " + validUUID + "  ",
				TenantID:    "  demo-tenant\t",
				ServiceName: " payment-service ",
				EventType:   " request.completed ",
			},
			check: func(t *testing.T, e event.Event) {
				if e.EventID != validUUID {
					t.Errorf("EventID = %q, want %q", e.EventID, validUUID)
				}
				if e.TenantID != "demo-tenant" {
					t.Errorf("TenantID = %q, want %q", e.TenantID, "demo-tenant")
				}
				if e.ServiceName != "payment-service" {
					t.Errorf("ServiceName = %q, want %q", e.ServiceName, "payment-service")
				}
				if e.EventType != "request.completed" {
					t.Errorf("EventType = %q, want %q", e.EventType, "request.completed")
				}
			},
		},
		{
			name:  "whitespace-only fields become empty so validation rejects them",
			input: event.Event{TenantID: "   "},
			check: func(t *testing.T, e event.Event) {
				if e.TenantID != "" {
					t.Errorf("TenantID = %q, want empty", e.TenantID)
				}
			},
		},
		{
			name:  "severity is lowercased",
			input: event.Event{Severity: "  ERROR "},
			check: func(t *testing.T, e event.Event) {
				if e.Severity != event.SeverityError {
					t.Errorf("Severity = %q, want %q", e.Severity, event.SeverityError)
				}
			},
		},
		{
			name:  "uuid case is canonicalised",
			input: event.Event{EventID: strings.ToUpper(validUUID)},
			check: func(t *testing.T, e event.Event) {
				if e.EventID != validUUID {
					t.Errorf("EventID = %q, want %q", e.EventID, validUUID)
				}
			},
		},
		{
			name:  "missing environment gets a default",
			input: event.Event{},
			check: func(t *testing.T, e event.Event) {
				if e.Environment != event.DefaultEnvironment {
					t.Errorf("Environment = %q, want %q", e.Environment, event.DefaultEnvironment)
				}
			},
		},
		{
			name:  "nil attributes become an empty map so JSONB never sees null",
			input: event.Event{},
			check: func(t *testing.T, e event.Event) {
				if e.Attributes == nil {
					t.Error("Attributes = nil, want an empty map")
				}
			},
		},
		{
			name: "timestamps are converted to UTC",
			input: event.Event{
				Timestamp: event.NewTimestamp(time.Date(2026, 7, 22, 12, 0, 0, 0, time.FixedZone("CEST", 2*60*60))),
			},
			check: func(t *testing.T, e event.Event) {
				if e.Timestamp.Location() != time.UTC {
					t.Errorf("Timestamp location = %v, want UTC", e.Timestamp.Location())
				}
				if want := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC); !e.Timestamp.Equal(want) {
					t.Errorf("Timestamp = %v, want %v", e.Timestamp.Time, want)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ev := tc.input
			ev.Normalize()
			tc.check(t, ev)
		})
	}
}

func TestNormalizeIsIdempotent(t *testing.T) {
	t.Parallel()

	ev := newValidEvent()
	ev.Severity = "  ERROR "
	ev.TenantID = "  demo-tenant "

	ev.Normalize()
	once := ev

	ev.Normalize()

	if ev.EventID != once.EventID || ev.TenantID != once.TenantID || ev.Severity != once.Severity {
		t.Errorf("second Normalize() changed the event: %+v then %+v", once, ev)
	}
	if !ev.Timestamp.Equal(once.Timestamp.Time) {
		t.Errorf("second Normalize() changed the timestamp: %v then %v", once.Timestamp.Time, ev.Timestamp.Time)
	}
}

func TestPartitionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tenant  string
		service string
		wantKey string
	}{
		{"tenant and service are joined by a colon", "demo-tenant", "payment-service", "demo-tenant:payment-service"},
		{"different services of one tenant get different keys", "demo-tenant", "order-service", "demo-tenant:order-service"},
		{"different tenants get different keys", "other-tenant", "payment-service", "other-tenant:payment-service"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ev := event.Event{TenantID: tc.tenant, ServiceName: tc.service}
			if got := ev.PartitionKey(); got != tc.wantKey {
				t.Errorf("PartitionKey() = %q, want %q", got, tc.wantKey)
			}
		})
	}
}

func TestTimestampUnmarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr bool
		want    time.Time
	}{
		{
			name:  "RFC3339 with Z offset",
			input: `"2026-07-22T10:30:00Z"`,
			want:  time.Date(2026, 7, 22, 10, 30, 0, 0, time.UTC),
		},
		{
			name:  "RFC3339 with numeric offset",
			input: `"2026-07-22T12:30:00+02:00"`,
			want:  time.Date(2026, 7, 22, 10, 30, 0, 0, time.UTC),
		},
		{
			name:  "RFC3339 with fractional seconds",
			input: `"2026-07-22T10:30:00.123456Z"`,
			want:  time.Date(2026, 7, 22, 10, 30, 0, 123456000, time.UTC),
		},
		{name: "a date alone is not RFC3339", input: `"2026-07-22"`, wantErr: true},
		{name: "free text is rejected", input: `"yesterday"`, wantErr: true},
		{name: "a number is rejected", input: `1753180200`, wantErr: true},
		{name: "an empty string is rejected", input: `""`, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var ts event.Timestamp
			err := json.Unmarshal([]byte(tc.input), &ts)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("Unmarshal(%s) = nil, want an error", tc.input)
				}
				// The message must name the offending value so the API can hand
				// it straight back to the caller.
				if !strings.Contains(err.Error(), "timestamp") {
					t.Errorf("error %q does not mention the field name", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unmarshal(%s) = %v, want nil", tc.input, err)
			}
			if !ts.Equal(tc.want) {
				t.Errorf("Unmarshal(%s) = %v, want %v", tc.input, ts.Time, tc.want)
			}
		})
	}
}

func TestEventJSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := newValidEvent()

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal() = %v", err)
	}

	var decoded event.Event
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal() = %v", err)
	}

	if decoded.EventID != original.EventID {
		t.Errorf("EventID = %q, want %q", decoded.EventID, original.EventID)
	}
	if decoded.Severity != original.Severity {
		t.Errorf("Severity = %q, want %q", decoded.Severity, original.Severity)
	}
	if !decoded.Timestamp.Equal(original.Timestamp.Time) {
		t.Errorf("Timestamp = %v, want %v", decoded.Timestamp.Time, original.Timestamp.Time)
	}
	if got := decoded.Attributes["http_method"]; got != "POST" {
		t.Errorf("Attributes[http_method] = %v, want POST", got)
	}
	// JSON numbers decode as float64; the value must survive even so.
	if got := decoded.Attributes["http_status_code"]; got != float64(200) {
		t.Errorf("Attributes[http_status_code] = %v (%T), want 200", got, got)
	}
	if err := decoded.Validate(); err != nil {
		t.Errorf("round-tripped event failed validation: %v", err)
	}
}

func TestTimestampMarshalsAsRFC3339(t *testing.T) {
	t.Parallel()

	ts := event.NewTimestamp(time.Date(2026, 7, 22, 10, 30, 0, 0, time.UTC))

	encoded, err := json.Marshal(ts)
	if err != nil {
		t.Fatalf("Marshal() = %v", err)
	}

	if want := `"2026-07-22T10:30:00Z"`; string(encoded) != want {
		t.Errorf("Marshal() = %s, want %s", encoded, want)
	}
}
