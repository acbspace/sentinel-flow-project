package correlate_test

import (
	"testing"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/correlate"
	"github.com/acbspace/sentinel-flow-project/internal/event"
	"github.com/acbspace/sentinel-flow-project/internal/store"
)

func errorRateRule() correlate.Rule {
	return correlate.Rule{
		ID:               "error_rate",
		Name:             "elevated error rate",
		Kind:             correlate.RuleKindErrorRate,
		Window:           time.Minute,
		Threshold:        0.5,
		MinEvents:        5,
		IncidentSeverity: event.SeverityError,
	}
}

// window is a freshly filled window: every error in it is new, which is what a
// first cycle sees.
func window(total, errs int64) store.ServiceWindow {
	return windowWithNew(total, errs, errs)
}

// windowWithNew is an overlapping window: errs events are inside it but only
// newErrs of them arrived since the previous cycle.
func windowWithNew(total, errs, newErrs int64) store.ServiceWindow {
	return store.ServiceWindow{
		TenantID:    "tenant-a",
		ServiceName: "payment-service",
		Total:       total,
		Errors:      errs,
		NewErrors:   newErrs,
	}
}

func TestRuleEvaluateErrorRate(t *testing.T) {
	t.Parallel()

	rule := errorRateRule()

	tests := []struct {
		name       string
		window     store.ServiceWindow
		wantFires  bool
		wantErrors int64
	}{
		{"well above threshold fires", window(20, 15), true, 15},
		{"exactly at threshold fires", window(10, 5), true, 5},
		{"just below threshold does not fire", window(10, 4), false, 0},
		{"clean traffic does not fire", window(50, 0), false, 0},
		{"below min events never fires despite 100%", window(4, 4), false, 0},
		{"exactly min events can fire", window(5, 5), true, 5},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			det := rule.Evaluate(tc.window)

			if det.Fires != tc.wantFires {
				t.Fatalf("Evaluate(%+v).Fires = %v, want %v", tc.window, det.Fires, tc.wantFires)
			}
			if !tc.wantFires {
				return
			}
			if det.EventCount != tc.wantErrors {
				t.Errorf("EventCount = %d, want %d", det.EventCount, tc.wantErrors)
			}
			if det.Title == "" {
				t.Error("a firing detection must carry a non-empty title")
			}
			if got, ok := det.Details["error_count"]; !ok || got != tc.wantErrors {
				t.Errorf("Details[error_count] = %v (present=%v), want %d", got, ok, tc.wantErrors)
			}
			if _, ok := det.Details["error_rate"]; !ok {
				t.Error("Details must include the observed error_rate")
			}
		})
	}
}

func TestRuleEvaluateUnknownKindNeverFires(t *testing.T) {
	t.Parallel()

	// A kind with no case in Evaluate must be inert rather than panic: this keeps
	// a half-added future rule from taking the engine down.
	rule := correlate.Rule{Kind: "not_a_real_kind", Threshold: 0, MinEvents: 1}
	if det := rule.Evaluate(window(100, 100)); det.Fires {
		t.Errorf("unknown rule kind fired: %+v", det)
	}
}
