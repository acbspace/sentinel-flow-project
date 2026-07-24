-- Remediation actions are the audit trail of everything SentinelFlow did, or was
-- asked to do, about an incident automatically. Every step of a runbook gets a
-- row -- including the ones a human rejected and the ones that timed out waiting
-- for approval -- because "what did the robot do to production, and who let it?"
-- must be answerable from data.

CREATE TABLE IF NOT EXISTS remediation_actions (
    id          UUID        PRIMARY KEY,

    incident_id UUID        NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,

    runbook_id  TEXT        NOT NULL,
    -- 1-based position of the step within its runbook.
    step_index  INT         NOT NULL,
    step_name   TEXT        NOT NULL,
    action_kind TEXT        NOT NULL,
    mode        TEXT        NOT NULL,
    status      TEXT        NOT NULL,
    -- Who approved or rejected, when a human was involved. Empty for auto steps.
    actor       TEXT        NOT NULL DEFAULT '',
    -- Parameters sent, responses received, failure reasons.
    detail      JSONB       NOT NULL DEFAULT '{}'::jsonb,

    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT remediation_actions_step_check   CHECK (step_index >= 1),
    CONSTRAINT remediation_actions_name_check   CHECK (step_name <> ''),
    CONSTRAINT remediation_actions_runbook_check CHECK (runbook_id <> ''),
    CONSTRAINT remediation_actions_mode_check   CHECK (mode IN ('auto', 'approval')),
    CONSTRAINT remediation_actions_status_check
        CHECK (status IN ('pending', 'approved', 'rejected', 'timed_out', 'succeeded', 'failed', 'skipped')),

    -- One row per step of an incident's runbook. The workflow derives the id
    -- deterministically and upserts as the step moves pending -> approved ->
    -- succeeded, so a retry or replay updates rather than duplicating.
    CONSTRAINT remediation_actions_step_unique UNIQUE (incident_id, step_index)
);

-- The one read this table serves: one incident's actions, in order.
CREATE INDEX IF NOT EXISTS remediation_actions_incident_idx
    ON remediation_actions (incident_id, step_index);

-- Finding the step currently awaiting a human, for the approve/reject endpoints.
CREATE INDEX IF NOT EXISTS remediation_actions_pending_idx
    ON remediation_actions (incident_id)
    WHERE status = 'pending';

-- The remediation starter's "already kicked off" marker, mirroring alerted_at.
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS remediated_at TIMESTAMPTZ;
