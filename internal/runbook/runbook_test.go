package runbook_test

import (
	"strings"
	"testing"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/runbook"
)

func TestMatcherMatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		matcher runbook.Matcher
		rule    string
		service string
		sev     string
		want    bool
	}{
		{"rule only matches any service", runbook.Matcher{RuleID: "error_rate"}, "error_rate", "payment-service", "error", true},
		{"different rule does not match", runbook.Matcher{RuleID: "error_rate"}, "high_latency", "payment-service", "error", false},
		{"service narrows the match", runbook.Matcher{RuleID: "error_rate", ServiceName: "payment-service"}, "error_rate", "payment-service", "error", true},
		{"wrong service does not match", runbook.Matcher{RuleID: "error_rate", ServiceName: "order-service"}, "error_rate", "payment-service", "error", false},
		{"severity narrows the match", runbook.Matcher{RuleID: "error_rate", Severity: "critical"}, "error_rate", "payment-service", "error", false},
		{"matching severity passes", runbook.Matcher{RuleID: "error_rate", Severity: "error"}, "error_rate", "payment-service", "error", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.matcher.Matches(tc.rule, tc.service, tc.sev); got != tc.want {
				t.Errorf("Matches(%q,%q,%q) = %v, want %v", tc.rule, tc.service, tc.sev, got, tc.want)
			}
		})
	}
}

func TestCatalogFindReturnsFirstMatch(t *testing.T) {
	t.Parallel()

	specific := runbook.Runbook{
		ID: "specific", Match: runbook.Matcher{RuleID: "error_rate", ServiceName: "payment-service"},
		Steps: []runbook.Step{{Name: "s", Kind: runbook.ActionNoop, Mode: runbook.ModeAuto}}, ApprovalTimeout: time.Minute,
	}
	general := runbook.Runbook{
		ID: "general", Match: runbook.Matcher{RuleID: "error_rate"},
		Steps: []runbook.Step{{Name: "s", Kind: runbook.ActionNoop, Mode: runbook.ModeAuto}}, ApprovalTimeout: time.Minute,
	}
	catalog := runbook.Catalog{Runbooks: []runbook.Runbook{specific, general}}

	got, ok := catalog.Find("error_rate", "payment-service", "error")
	if !ok || got.ID != "specific" {
		t.Errorf("Find returned (%q, %v), want the first (most specific) match", got.ID, ok)
	}

	got, ok = catalog.Find("error_rate", "order-service", "error")
	if !ok || got.ID != "general" {
		t.Errorf("Find returned (%q, %v), want the general runbook", got.ID, ok)
	}

	if _, ok := catalog.Find("unknown_rule", "order-service", "error"); ok {
		t.Error("Find matched a rule with no runbook, want no match")
	}
}

func TestDefaultCatalogIsValid(t *testing.T) {
	t.Parallel()

	c, err := runbook.DefaultCatalog()
	if err != nil {
		t.Fatalf("DefaultCatalog() = %v", err)
	}

	rb, ok := c.Find("error_rate", "payment-service", "error")
	if !ok {
		t.Fatal("the embedded catalog does not cover the error_rate rule")
	}
	if len(rb.Steps) < 2 {
		t.Fatalf("embedded runbook has %d steps, want at least 2", len(rb.Steps))
	}
	// The dangerous step must be gated: this is the safety property the whole
	// milestone rests on, so it is asserted rather than assumed.
	var sawApproval bool
	for _, s := range rb.Steps {
		if s.Kind == runbook.ActionWebhook && s.Mode != runbook.ModeApproval {
			t.Errorf("step %q performs a webhook action unattended; it must require approval", s.Name)
		}
		if s.Mode == runbook.ModeApproval {
			sawApproval = true
		}
	}
	if !sawApproval {
		t.Error("the embedded runbook has no approval-gated step")
	}
}

func TestParseCatalogRejectsBadInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		doc     string
		wantSub string
	}{
		{"no runbooks", `{"runbooks": []}`, "at least one runbook"},
		{
			"missing rule id",
			`{"runbooks":[{"id":"a","approval_timeout":"1m","match":{},"steps":[{"name":"s","kind":"noop","mode":"auto"}]}]}`,
			"match.rule_id is required",
		},
		{
			"no steps",
			`{"runbooks":[{"id":"a","approval_timeout":"1m","match":{"rule_id":"r"},"steps":[]}]}`,
			"at least one step",
		},
		{
			"unknown action kind",
			`{"runbooks":[{"id":"a","approval_timeout":"1m","match":{"rule_id":"r"},"steps":[{"name":"s","kind":"launch-missiles","mode":"auto"}]}]}`,
			"unknown action kind",
		},
		{
			"webhook without a target",
			`{"runbooks":[{"id":"a","approval_timeout":"1m","match":{"rule_id":"r"},"steps":[{"name":"s","kind":"webhook","mode":"approval"}]}]}`,
			"needs a target",
		},
		{
			"invalid mode",
			`{"runbooks":[{"id":"a","approval_timeout":"1m","match":{"rule_id":"r"},"steps":[{"name":"s","kind":"noop","mode":"whenever"}]}]}`,
			"mode must be",
		},
		{
			"duplicate ids",
			`{"runbooks":[
			   {"id":"a","approval_timeout":"1m","match":{"rule_id":"r"},"steps":[{"name":"s","kind":"noop","mode":"auto"}]},
			   {"id":"a","approval_timeout":"1m","match":{"rule_id":"r2"},"steps":[{"name":"s","kind":"noop","mode":"auto"}]}]}`,
			"defined more than once",
		},
		{
			"bad approval timeout",
			`{"runbooks":[{"id":"a","approval_timeout":"soon","match":{"rule_id":"r"},"steps":[{"name":"s","kind":"noop","mode":"auto"}]}]}`,
			"approval_timeout",
		},
		{"unknown field", `{"runbooks": [], "extra": 1}`, "parse runbook catalog"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := runbook.ParseCatalog([]byte(tc.doc))
			if err == nil {
				t.Fatalf("ParseCatalog(%s) = nil, want an error", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not mention %q", err, tc.wantSub)
			}
		})
	}
}
