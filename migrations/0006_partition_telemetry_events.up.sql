-- Range-partition telemetry_events by event_timestamp, one partition per day.
--
-- Why: the table has no retention policy and grows without bound. Deleting old
-- rows from a single heap is slow and leaves bloat that only VACUUM FULL
-- reclaims; dropping a partition is a catalogue operation that returns the disk
-- immediately. Partition pruning also keeps every time-bounded query reading
-- only the days it asks about, which is most of the queries this system runs.
--
-- THE COST, STATED PLAINLY: PostgreSQL requires the partition key to appear in
-- every unique constraint, so the primary key becomes (event_timestamp,
-- event_id) rather than event_id alone. The deduplication guarantee narrows
-- from "the same event_id yields exactly one row" to "the same (event_id,
-- event_timestamp) yields exactly one row".
--
-- That is still sound for the delivery semantics this system actually has. A
-- Kafka redelivery replays a byte-identical record, so its event_timestamp is
-- identical too, it routes to the same partition, and the conflict still fires.
-- What is no longer caught is a producer that sends the same event_id twice
-- with *different* timestamps — which was never a redelivery, it was always a
-- bug or an attack, and the old schema silently accepted the first and hid the
-- second. Neither behaviour is good; the new one is at least visible in the data.
--
-- Daily rather than monthly: retention granularity equals partition size, so
-- monthly partitions with a 30-day policy would keep up to 60 days of data.
-- Daily also matches how the table is written (append-mostly, time-ordered) and
-- how it is read (recent windows).
--
-- DEPLOYMENT ORDER MATTERS. This migration is not backward compatible with an
-- incident-engine built before it. The old binary inserts with ON CONFLICT
-- (event_id), and once event_id alone is no longer unique there is no index for
-- that specification to match, so every insert fails with SQLSTATE 42P10.
-- Migrate and deploy the engine together; do not migrate ahead of a rollout.
--
-- The failure is at least a safe one, and was confirmed rather than assumed: the
-- engine treats it as a permanent store failure, exits loudly, and leaves the
-- Kafka offset uncommitted, so the records are redelivered to the new binary and
-- nothing is lost. A targetless ON CONFLICT DO NOTHING would have worked against
-- both schemas and made the ordering irrelevant, and was rejected: it would
-- silently swallow a violation of any unique constraint added later, which is
-- exactly the kind of quiet failure the rest of this system refuses to have.

SET LOCAL TimeZone = 'UTC';

-- The new table. Column definitions are repeated rather than copied with LIKE so
-- that this file is the readable definition of the table's final shape.
CREATE TABLE telemetry_events_partitioned (
    event_id        UUID        NOT NULL,
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
    CONSTRAINT telemetry_events_event_type_check     CHECK (event_type <> ''),

    -- Temporary name: the index behind it must not collide with the existing
    -- table's primary key index while both tables exist. Renamed below.
    CONSTRAINT telemetry_events_partitioned_pkey
        PRIMARY KEY (event_timestamp, event_id)
) PARTITION BY RANGE (event_timestamp);

-- The safety net. Without it, an insert whose timestamp falls outside every
-- defined partition fails outright, which would turn "the janitor stopped" into
-- "ingestion stopped". Rows here are a signal that something is wrong, not a
-- normal resting place, which is why the janitor reports on its occupancy.
CREATE TABLE telemetry_events_default
    PARTITION OF telemetry_events_partitioned DEFAULT;

-- Daily partitions covering everything already stored, plus a week ahead so the
-- pipeline keeps writing to real partitions even before the janitor first runs.
DO $$
DECLARE
    lo DATE;
    hi DATE;
    d  DATE;
BEGIN
    SELECT COALESCE(min(event_timestamp)::date, CURRENT_DATE),
           COALESCE(max(event_timestamp)::date, CURRENT_DATE)
      INTO lo, hi
      FROM telemetry_events;

    hi := GREATEST(hi, CURRENT_DATE) + 7;

    d := lo;
    WHILE d <= hi LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF telemetry_events_partitioned '
            'FOR VALUES FROM (%L) TO (%L)',
            'telemetry_events_' || to_char(d, 'YYYYMMDD'),
            d::timestamptz,
            (d + 1)::timestamptz
        );
        d := d + 1;
    END LOOP;
END $$;

INSERT INTO telemetry_events_partitioned (
    event_id, schema_version, tenant_id, service_name, environment,
    event_type, severity, event_timestamp, trace_id, attributes,
    received_at, processed_at
)
SELECT event_id, schema_version, tenant_id, service_name, environment,
       event_type, severity, event_timestamp, trace_id, attributes,
       received_at, processed_at
FROM telemetry_events;

DROP TABLE telemetry_events;

ALTER TABLE telemetry_events_partitioned RENAME TO telemetry_events;
ALTER TABLE telemetry_events
    RENAME CONSTRAINT telemetry_events_partitioned_pkey TO telemetry_events_pkey;

-- The indexes from 0001 and 0005, rebuilt on the partitioned table. Declaring
-- them on the parent makes them partitioned indexes: every partition the janitor
-- creates later inherits them automatically, so a new day is never unindexed.
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

-- Still worth having after partitioning: pruning narrows a query to one day,
-- but a 60-second correlation window is a thousandth of that day.
CREATE INDEX telemetry_events_ts_idx
    ON telemetry_events (event_timestamp DESC);
