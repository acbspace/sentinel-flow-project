package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/acbspace/sentinel-flow-project/internal/config"
)

func TestLoadIngestionAPIDefaults(t *testing.T) {
	// Not parallel: these tests manipulate the process environment.
	t.Setenv("KAFKA_BROKERS", "kafka:9092")

	cfg, err := config.LoadIngestionAPI()
	if err != nil {
		t.Fatalf("LoadIngestionAPI() = %v", err)
	}

	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8080")
	}
	if cfg.Kafka.Topic != "telemetry.events.v1" {
		t.Errorf("Kafka.Topic = %q, want %q", cfg.Kafka.Topic, "telemetry.events.v1")
	}
	if cfg.MaxBodyBytes != 64*1024 {
		t.Errorf("MaxBodyBytes = %d, want %d", cfg.MaxBodyBytes, 64*1024)
	}
}

func TestLoadIngestionAPIParsesOverrides(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":9000")
	t.Setenv("KAFKA_BROKERS", " broker-1:9092 , broker-2:9092 ")
	t.Setenv("KAFKA_TOPIC", "custom.topic")
	t.Setenv("MAX_BODY_BYTES", "1024")
	t.Setenv("KAFKA_PRODUCE_TIMEOUT", "3s")

	cfg, err := config.LoadIngestionAPI()
	if err != nil {
		t.Fatalf("LoadIngestionAPI() = %v", err)
	}

	if cfg.HTTPAddr != ":9000" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":9000")
	}
	// A comma-separated broker list must survive stray whitespace.
	want := []string{"broker-1:9092", "broker-2:9092"}
	if len(cfg.Kafka.Brokers) != len(want) {
		t.Fatalf("Brokers = %v, want %v", cfg.Kafka.Brokers, want)
	}
	for i := range want {
		if cfg.Kafka.Brokers[i] != want[i] {
			t.Errorf("Brokers[%d] = %q, want %q", i, cfg.Kafka.Brokers[i], want[i])
		}
	}
	if cfg.Kafka.Topic != "custom.topic" {
		t.Errorf("Kafka.Topic = %q, want %q", cfg.Kafka.Topic, "custom.topic")
	}
	if cfg.MaxBodyBytes != 1024 {
		t.Errorf("MaxBodyBytes = %d, want 1024", cfg.MaxBodyBytes)
	}
	if cfg.ProduceTimeout != 3*time.Second {
		t.Errorf("ProduceTimeout = %v, want 3s", cfg.ProduceTimeout)
	}
}

func TestLoadIngestionAPIRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantSub string
	}{
		{
			name:    "non-numeric body size",
			env:     map[string]string{"MAX_BODY_BYTES": "big"},
			wantSub: "MAX_BODY_BYTES",
		},
		{
			name:    "non-positive body size",
			env:     map[string]string{"MAX_BODY_BYTES": "0"},
			wantSub: "MAX_BODY_BYTES",
		},
		{
			name:    "unparseable duration",
			env:     map[string]string{"KAFKA_PRODUCE_TIMEOUT": "soon"},
			wantSub: "KAFKA_PRODUCE_TIMEOUT",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			_, err := config.LoadIngestionAPI()
			if err == nil {
				t.Fatal("LoadIngestionAPI() = nil, want a configuration error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not mention %q", err, tc.wantSub)
			}
		})
	}
}

func TestLoadIncidentEngineRequiresDSN(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "")

	_, err := config.LoadIncidentEngine()
	if err == nil {
		t.Fatal("LoadIncidentEngine() = nil, want an error when POSTGRES_DSN is unset")
	}
	if !strings.Contains(err.Error(), "POSTGRES_DSN") {
		t.Errorf("error %q does not mention POSTGRES_DSN", err)
	}
}

func TestLoadIncidentEngineDefaults(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://user:pass@localhost:5432/db")

	cfg, err := config.LoadIncidentEngine()
	if err != nil {
		t.Fatalf("LoadIncidentEngine() = %v", err)
	}

	// The consumer group name is part of the deployment contract: changing it
	// silently replays the whole topic, so the default is asserted here.
	if cfg.ConsumerGroup != "incident-engine-v1" {
		t.Errorf("ConsumerGroup = %q, want %q", cfg.ConsumerGroup, "incident-engine-v1")
	}
	if cfg.RetryAttempts != 5 {
		t.Errorf("RetryAttempts = %d, want 5", cfg.RetryAttempts)
	}
	if cfg.HTTPAddr != ":8081" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8081")
	}
}

func TestLoadIncidentEngineCorrelationDefaults(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://user:pass@localhost:5432/db")

	cfg, err := config.LoadIncidentEngine()
	if err != nil {
		t.Fatalf("LoadIncidentEngine() = %v", err)
	}

	c := cfg.Correlation
	if !c.Enabled {
		t.Error("Correlation.Enabled = false, want true by default")
	}
	if c.Interval != 15*time.Second {
		t.Errorf("Correlation.Interval = %v, want 15s", c.Interval)
	}
	if c.Window != 60*time.Second {
		t.Errorf("Correlation.Window = %v, want 60s", c.Window)
	}
	if c.ErrorRateThreshold != 0.5 {
		t.Errorf("Correlation.ErrorRateThreshold = %v, want 0.5", c.ErrorRateThreshold)
	}
	if c.ErrorRateMinEvents != 5 {
		t.Errorf("Correlation.ErrorRateMinEvents = %d, want 5", c.ErrorRateMinEvents)
	}
	if c.ResolveAfter != 5*time.Minute {
		t.Errorf("Correlation.ResolveAfter = %v, want 5m", c.ResolveAfter)
	}
}

func TestLoadIncidentEngineRejectsInvalidCorrelation(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantSub string
	}{
		{
			name:    "threshold above one",
			env:     map[string]string{"CORRELATION_ERROR_RATE_THRESHOLD": "1.5"},
			wantSub: "CORRELATION_ERROR_RATE_THRESHOLD",
		},
		{
			name:    "zero minimum events",
			env:     map[string]string{"CORRELATION_ERROR_RATE_MIN_EVENTS": "0"},
			wantSub: "CORRELATION_ERROR_RATE_MIN_EVENTS",
		},
		{
			name:    "non-positive interval",
			env:     map[string]string{"CORRELATION_INTERVAL": "0s"},
			wantSub: "CORRELATION_INTERVAL",
		},
		{
			name:    "unparseable window",
			env:     map[string]string{"CORRELATION_WINDOW": "soon"},
			wantSub: "CORRELATION_WINDOW",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("POSTGRES_DSN", "postgres://user:pass@localhost:5432/db")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			_, err := config.LoadIncidentEngine()
			if err == nil {
				t.Fatal("LoadIncidentEngine() = nil, want a configuration error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not mention %q", err, tc.wantSub)
			}
		})
	}
}

func TestLoadAlertingDefaults(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://user:pass@localhost:5432/db")

	cfg, err := config.LoadAlerting()
	if err != nil {
		t.Fatalf("LoadAlerting() = %v", err)
	}

	if cfg.HTTPAddr != ":8085" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8085")
	}
	if cfg.TemporalAddress != "localhost:7233" {
		t.Errorf("TemporalAddress = %q, want localhost:7233", cfg.TemporalAddress)
	}
	if cfg.TaskQueue != "incident-alerts" {
		t.Errorf("TaskQueue = %q, want incident-alerts", cfg.TaskQueue)
	}
	if cfg.PollInterval != 10*time.Second {
		t.Errorf("PollInterval = %v, want 10s", cfg.PollInterval)
	}
	if cfg.BatchSize != 50 {
		t.Errorf("BatchSize = %d, want 50", cfg.BatchSize)
	}
}

func TestLoadAlertingRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantSub string
	}{
		{
			name:    "missing DSN",
			env:     map[string]string{"POSTGRES_DSN": ""},
			wantSub: "POSTGRES_DSN",
		},
		{
			name:    "non-positive poll interval",
			env:     map[string]string{"POSTGRES_DSN": "postgres://u:p@h:5432/d", "ALERT_POLL_INTERVAL": "0s"},
			wantSub: "ALERT_POLL_INTERVAL",
		},
		{
			name:    "zero batch size",
			env:     map[string]string{"POSTGRES_DSN": "postgres://u:p@h:5432/d", "ALERT_BATCH_SIZE": "0"},
			wantSub: "ALERT_BATCH_SIZE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			_, err := config.LoadAlerting()
			if err == nil {
				t.Fatal("LoadAlerting() = nil, want a configuration error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not mention %q", err, tc.wantSub)
			}
		})
	}
}

func TestLoadRemediationDefaults(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://user:pass@localhost:5432/db")

	cfg, err := config.LoadRemediation()
	if err != nil {
		t.Fatalf("LoadRemediation() = %v", err)
	}

	if cfg.HTTPAddr != ":8086" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8086")
	}
	if cfg.TaskQueue != "incident-remediation" {
		t.Errorf("TaskQueue = %q, want incident-remediation", cfg.TaskQueue)
	}
	if cfg.PollInterval != 10*time.Second {
		t.Errorf("PollInterval = %v, want 10s", cfg.PollInterval)
	}
	// An empty catalog path means "use the runbooks embedded in the binary".
	if cfg.CatalogPath != "" {
		t.Errorf("CatalogPath = %q, want empty (embedded default)", cfg.CatalogPath)
	}
}

func TestLoadRemediationRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantSub string
	}{
		{
			name:    "missing DSN",
			env:     map[string]string{"POSTGRES_DSN": ""},
			wantSub: "POSTGRES_DSN",
		},
		{
			name:    "non-positive poll interval",
			env:     map[string]string{"POSTGRES_DSN": "postgres://u:p@h:5432/d", "REMEDIATION_POLL_INTERVAL": "0s"},
			wantSub: "REMEDIATION_POLL_INTERVAL",
		},
		{
			name:    "zero batch size",
			env:     map[string]string{"POSTGRES_DSN": "postgres://u:p@h:5432/d", "REMEDIATION_BATCH_SIZE": "0"},
			wantSub: "REMEDIATION_BATCH_SIZE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			_, err := config.LoadRemediation()
			if err == nil {
				t.Fatal("LoadRemediation() = nil, want a configuration error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not mention %q", err, tc.wantSub)
			}
		})
	}
}

func TestLoadDemoServiceValidatesFailureRate(t *testing.T) {
	tests := []struct {
		name    string
		rate    string
		wantErr bool
	}{
		{name: "zero is valid", rate: "0"},
		{name: "one is valid", rate: "1"},
		{name: "a fraction is valid", rate: "0.25"},
		{name: "negative is rejected", rate: "-0.1", wantErr: true},
		{name: "above one is rejected", rate: "1.5", wantErr: true},
		{name: "non-numeric is rejected", rate: "often", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FAILURE_RATE", tc.rate)

			_, err := config.LoadDemoService("payment-service", ":8083", 0.2)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("LoadDemoService() with FAILURE_RATE=%q = nil, want an error", tc.rate)
				}
				if !strings.Contains(err.Error(), "FAILURE_RATE") {
					t.Errorf("error %q does not mention FAILURE_RATE", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadDemoService() with FAILURE_RATE=%q = %v", tc.rate, err)
			}
		})
	}
}

func TestLoadDemoServiceRejectsInvertedLatencyRange(t *testing.T) {
	t.Setenv("MIN_LATENCY", "500ms")
	t.Setenv("MAX_LATENCY", "100ms")

	_, err := config.LoadDemoService("order-service", ":8082", 0.1)
	if err == nil {
		t.Fatal("LoadDemoService() = nil, want an error when MAX_LATENCY < MIN_LATENCY")
	}
}

func TestLoadDemoServiceUsesSuppliedDefaults(t *testing.T) {
	cfg, err := config.LoadDemoService("order-service", ":8082", 0.35)
	if err != nil {
		t.Fatalf("LoadDemoService() = %v", err)
	}

	if cfg.HTTPAddr != ":8082" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8082")
	}
	if cfg.FailureRate != 0.35 {
		t.Errorf("FailureRate = %v, want 0.35", cfg.FailureRate)
	}
	if cfg.Observability.ServiceName != "order-service" {
		t.Errorf("ServiceName = %q, want %q", cfg.Observability.ServiceName, "order-service")
	}
}

func TestLoadIngestionAPIEventBoundDefaults(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "kafka:9092")

	cfg, err := config.LoadIngestionAPI()
	if err != nil {
		t.Fatalf("LoadIngestionAPI() = %v", err)
	}

	if cfg.MaxEventFutureSkew != 5*time.Minute {
		t.Errorf("MaxEventFutureSkew = %v, want %v", cfg.MaxEventFutureSkew, 5*time.Minute)
	}
	if cfg.MaxEventBackdate != 7*24*time.Hour {
		t.Errorf("MaxEventBackdate = %v, want %v", cfg.MaxEventBackdate, 7*24*time.Hour)
	}
}

func TestLoadIngestionAPIRejectsNegativeEventBounds(t *testing.T) {
	tests := map[string]string{
		"MAX_EVENT_FUTURE_SKEW": "-1m",
		"MAX_EVENT_BACKDATE":    "-1h",
	}

	for name, value := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("KAFKA_BROKERS", "kafka:9092")
			t.Setenv(name, value)

			_, err := config.LoadIngestionAPI()
			if err == nil {
				t.Fatalf("LoadIngestionAPI() = nil, want an error for %s=%s", name, value)
			}
			if !strings.Contains(err.Error(), name) {
				t.Errorf("error %q should name %s", err, name)
			}
		})
	}
}

func TestLoadIngestionAPIAllowsDisablingEventBounds(t *testing.T) {
	// Zero is a documented opt-out rather than an error, so an operator can turn
	// a bound off deliberately during a migration.
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("MAX_EVENT_FUTURE_SKEW", "0")
	t.Setenv("MAX_EVENT_BACKDATE", "0")

	cfg, err := config.LoadIngestionAPI()
	if err != nil {
		t.Fatalf("LoadIngestionAPI() = %v", err)
	}
	if cfg.MaxEventFutureSkew != 0 || cfg.MaxEventBackdate != 0 {
		t.Errorf("bounds = %v/%v, want 0/0", cfg.MaxEventFutureSkew, cfg.MaxEventBackdate)
	}
}

func TestLoadIncidentEngineEventBoundDefault(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("POSTGRES_DSN", "postgres://localhost/db")

	cfg, err := config.LoadIncidentEngine()
	if err != nil {
		t.Fatalf("LoadIncidentEngine() = %v", err)
	}

	// Must match the ingestion API's default, or a record accepted at the front
	// door could be discarded by the engine that receives it.
	if cfg.MaxEventFutureSkew != 5*time.Minute {
		t.Errorf("MaxEventFutureSkew = %v, want %v", cfg.MaxEventFutureSkew, 5*time.Minute)
	}
}
