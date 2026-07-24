-- Notifications are the audit trail of alerting: one row per dispatch the alert
-- workflow makes as it climbs an escalation policy. They are the timeline an
-- operator reads to answer "who was paged, when, and did it escalate?".

CREATE TABLE IF NOT EXISTS notifications (
    id          UUID        PRIMARY KEY,

    -- Deleting an incident removes its notifications; they have no meaning
    -- without the incident they describe.
    incident_id UUID        NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,

    -- The 1-based escalation level this dispatch was sent at.
    level       INT         NOT NULL,
    -- The policy rung's label (e.g. "primary on-call") and the resolved contact.
    target      TEXT        NOT NULL,
    contact     TEXT        NOT NULL,
    -- How it was delivered: "log" (recorded only) or "webhook".
    channel     TEXT        NOT NULL,
    -- Delivery outcome.
    status      TEXT        NOT NULL,
    -- Free-form context: the incident title, the webhook response, an error.
    detail      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    sent_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT notifications_level_check   CHECK (level >= 1),
    CONSTRAINT notifications_target_check  CHECK (target <> ''),
    CONSTRAINT notifications_contact_check CHECK (contact <> ''),
    CONSTRAINT notifications_channel_check CHECK (channel <> ''),
    CONSTRAINT notifications_status_check  CHECK (status IN ('sent', 'failed'))
);

-- The one read this table serves: one incident's dispatches, in order.
CREATE INDEX IF NOT EXISTS notifications_incident_idx
    ON notifications (incident_id, sent_at);

-- alerted_at is the workflow starter's "already kicked off" marker: the poller
-- starts an alert workflow only for open incidents where this is still NULL, so
-- it never starts a second workflow for the same incident.
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS alerted_at TIMESTAMPTZ;
