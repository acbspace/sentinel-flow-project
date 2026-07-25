-- Telemetry events are the single normalized record of everything the pipeline
-- has ingested. This table stores them; the incidents table correlates them into
-- episodes.

CREATE TABLE IF NOT EXISTS telemetry_events (
    -- The producer-supplied event ID is the primary key rather than a serial
    -- surrogate. That is the whole idempotency strategy: because Kafka delivery
    -- is at-least-once, the same event can arrive more than once, and a unique
    -- constraint on the producer's ID is the only thing that can reject the
    -- redelivery atomically under concurrent consumers.
    event_id        UUID        PRIMARY KEY,

    schema_version  TEXT        NOT NULL,
    tenant_id       TEXT        NOT NULL,
    service_name    TEXT        NOT NULL,
    environment     TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    severity        TEXT        NOT NULL,

    -- When the event happened, as reported by the producer.
    event_timestamp TIMESTAMPTZ NOT NULL,

    trace_id        TEXT        NOT NULL DEFAULT '',

    -- Free-form producer context. JSONB rather than a wide table so that adding
    -- an attribute never requires a migration, and so attributes stay queryable.
    attributes      JSONB       NOT NULL DEFAULT '{}'::jsonb,

    -- When the ingestion API accepted the event (the Kafka record timestamp).
    received_at     TIMESTAMPTZ NOT NULL,

    -- When this row was written. received_at to processed_at is the pipeline's
    -- end-to-end latency, which is the number an on-call engineer cares about.
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT telemetry_events_severity_check
        CHECK (severity IN ('debug', 'info', 'warn', 'error', 'critical')),
    CONSTRAINT telemetry_events_schema_version_check
        CHECK (schema_version <> ''),
    CONSTRAINT telemetry_events_tenant_id_check
        CHECK (tenant_id <> ''),
    CONSTRAINT telemetry_events_service_name_check
        CHECK (service_name <> ''),
    CONSTRAINT telemetry_events_event_type_check
        CHECK (event_type <> '')
);

-- Every read path in this system is "some slice of events, most recent first".
-- Each index below is a composite of the filter column plus a descending
-- timestamp, so a query can seek straight to the newest matching rows and stop,
-- instead of scanning the slice and sorting it.

-- "Show me this tenant's recent activity" — also the natural isolation boundary
-- for a multi-tenant product.
CREATE INDEX IF NOT EXISTS telemetry_events_tenant_ts_idx
    ON telemetry_events (tenant_id, event_timestamp DESC);

-- "Show me what this service has been doing" — the per-service dashboard.
CREATE INDEX IF NOT EXISTS telemetry_events_service_ts_idx
    ON telemetry_events (service_name, event_timestamp DESC);

-- "Show me every request.completed" — feeds rate and throughput calculations.
CREATE INDEX IF NOT EXISTS telemetry_events_type_ts_idx
    ON telemetry_events (event_type, event_timestamp DESC);

-- "Show me recent errors" — the query the future correlation engine runs most.
CREATE INDEX IF NOT EXISTS telemetry_events_severity_ts_idx
    ON telemetry_events (severity, event_timestamp DESC);

-- Pivot from a trace to every event recorded under it. Partial, because events
-- submitted without trace context would otherwise bloat the index with a single
-- worthless key.
CREATE INDEX IF NOT EXISTS telemetry_events_trace_idx
    ON telemetry_events (trace_id)
    WHERE trace_id <> '';
