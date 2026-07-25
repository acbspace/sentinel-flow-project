-- Collapse the partitioned telemetry_events back into a single table.
--
-- DATA LOSS WARNING, and it is inherent rather than sloppy. The partitioned
-- schema's primary key is (event_timestamp, event_id); a plain table's is
-- event_id alone. Reverting therefore narrows the constraint, and any rows the
-- wider key permitted — the same event_id stored twice with different
-- timestamps — cannot all survive it. The copy below keeps the first of each
-- event_id and drops the rest.
--
-- In practice that set is empty: a Kafka redelivery replays an identical record,
-- so duplicates share a timestamp and were already collapsed. It is non-empty
-- only if a producer sent one event_id under two different timestamps, which the
-- pre-partition schema silently discarded anyway. Check before rolling back if
-- it matters:
--
--   SELECT event_id, count(*) FROM telemetry_events
--   GROUP BY event_id HAVING count(*) > 1;

SET LOCAL TimeZone = 'UTC';

CREATE TABLE telemetry_events_plain (
    event_id        UUID        PRIMARY KEY,
    schema_version  TEXT        NOT NULL,
    tenant_id       TEXT        NOT NULL,
    service_name    TEXT        NOT NULL,
    environment     TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    severity        TEXT        NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    trace_id        TEXT        NOT NULL DEFAULT '',
    attributes      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    received_at     TIMESTAMPTZ NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT telemetry_events_severity_check
        CHECK (severity IN ('debug', 'info', 'warn', 'error', 'critical')),
    CONSTRAINT telemetry_events_schema_version_check CHECK (schema_version <> ''),
    CONSTRAINT telemetry_events_tenant_id_check      CHECK (tenant_id <> ''),
    CONSTRAINT telemetry_events_service_name_check   CHECK (service_name <> ''),
    CONSTRAINT telemetry_events_event_type_check     CHECK (event_type <> '')
);

INSERT INTO telemetry_events_plain (
    event_id, schema_version, tenant_id, service_name, environment,
    event_type, severity, event_timestamp, trace_id, attributes,
    received_at, processed_at
)
SELECT event_id, schema_version, tenant_id, service_name, environment,
       event_type, severity, event_timestamp, trace_id, attributes,
       received_at, processed_at
FROM telemetry_events
ON CONFLICT (event_id) DO NOTHING;

-- Drops every partition, including the default one, with it.
DROP TABLE telemetry_events;

ALTER TABLE telemetry_events_plain RENAME TO telemetry_events;
ALTER TABLE telemetry_events
    RENAME CONSTRAINT telemetry_events_plain_pkey TO telemetry_events_pkey;

CREATE INDEX telemetry_events_tenant_ts_idx
    ON telemetry_events (tenant_id, event_timestamp DESC);
CREATE INDEX telemetry_events_service_ts_idx
    ON telemetry_events (service_name, event_timestamp DESC);
CREATE INDEX telemetry_events_type_ts_idx
    ON telemetry_events (event_type, event_timestamp DESC);
CREATE INDEX telemetry_events_severity_ts_idx
    ON telemetry_events (severity, event_timestamp DESC);
CREATE INDEX telemetry_events_trace_idx
    ON telemetry_events (trace_id) WHERE trace_id <> '';
CREATE INDEX telemetry_events_ts_idx
    ON telemetry_events (event_timestamp DESC);
