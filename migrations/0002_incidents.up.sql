-- Incidents are the correlation engine's output: one row per detected episode
-- of unhealthy behaviour, opened when a rule trips over the telemetry_events
-- stream and closed manually or after a quiet period.

CREATE TABLE IF NOT EXISTS incidents (
    -- A surrogate UUID, not the fingerprint, is the primary key: the fingerprint
    -- identifies the active incident, but the same fingerprint recurs over time
    -- as one episode resolves and another opens later.
    id              UUID        PRIMARY KEY,

    -- The deterministic grouping key: rule + tenant + service. See the partial
    -- unique index below for the invariant it enforces.
    fingerprint     TEXT        NOT NULL,

    tenant_id       TEXT        NOT NULL,
    service_name    TEXT        NOT NULL,
    rule_id         TEXT        NOT NULL,
    title           TEXT        NOT NULL,
    severity        TEXT        NOT NULL,
    status          TEXT        NOT NULL,

    -- How many offending events have folded into this incident across every
    -- detection that grouped into it.
    event_count     BIGINT      NOT NULL DEFAULT 0,

    -- first_seen_at to last_seen_at is the episode's span; last_seen_at drives
    -- both the triage ordering and the auto-resolution quiet-period check.
    first_seen_at   TIMESTAMPTZ NOT NULL,
    last_seen_at    TIMESTAMPTZ NOT NULL,

    opened_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at TIMESTAMPTZ,
    resolved_at     TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Rule-specific evidence (observed rate, threshold, sample size, window).
    -- JSONB so a new rule kind can attach new evidence without a migration.
    details         JSONB       NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT incidents_severity_check
        CHECK (severity IN ('debug', 'info', 'warn', 'error', 'critical')),
    CONSTRAINT incidents_status_check
        CHECK (status IN ('open', 'acknowledged', 'resolved')),
    CONSTRAINT incidents_fingerprint_check CHECK (fingerprint <> ''),
    CONSTRAINT incidents_tenant_id_check   CHECK (tenant_id <> ''),
    CONSTRAINT incidents_service_name_check CHECK (service_name <> ''),
    CONSTRAINT incidents_rule_id_check     CHECK (rule_id <> ''),

    -- A resolved incident must record when it resolved, and a non-resolved one
    -- must not. This keeps resolved_at from drifting out of sync with status.
    CONSTRAINT incidents_resolved_at_check
        CHECK ((status = 'resolved') = (resolved_at IS NOT NULL))
);

-- The core deduplication guarantee: at most one non-resolved incident may exist
-- per fingerprint. Because the index is partial (resolved rows are exempt), the
-- same condition can recur as a brand-new incident after the previous one is
-- resolved, while an ongoing condition can only ever have a single open incident
-- that repeat detections update in place. UPSERT relies on this index as its
-- conflict arbiter.
CREATE UNIQUE INDEX IF NOT EXISTS incidents_active_fingerprint_idx
    ON incidents (fingerprint)
    WHERE status <> 'resolved';

-- "Show me this tenant's incidents, most recent activity first" — the per-tenant
-- incident list, and the natural isolation boundary for a multi-tenant product.
CREATE INDEX IF NOT EXISTS incidents_tenant_status_idx
    ON incidents (tenant_id, status, last_seen_at DESC);

-- "Show me every open critical" — the cross-tenant triage view.
CREATE INDEX IF NOT EXISTS incidents_status_severity_idx
    ON incidents (status, severity, last_seen_at DESC);
