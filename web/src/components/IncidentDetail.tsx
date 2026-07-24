import { useState } from "react";
import {
  acknowledgeIncident,
  ApiError,
  decideRemediation,
  listNotifications,
  listRemediation,
  resolveIncident,
} from "../api";
import type { Incident, RemediationAction } from "../types";
import { usePolling } from "../usePolling";
import { relativeTime } from "./IncidentList";

interface Props {
  incident: Incident;
  onChanged: () => void;
}

/** Who the dashboard claims to be when approving. A real deployment takes this
 *  from the authenticated session; there is no auth yet, so it is explicit. */
const ACTOR = "dashboard";

export function IncidentDetail({ incident, onChanged }: Props) {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [isError, setIsError] = useState(false);

  const notifications = usePolling(() => listNotifications(incident.id), 5000, [incident.id]);
  const remediation = usePolling(() => listRemediation(incident.id), 5000, [incident.id]);

  const actions = remediation.data?.actions ?? [];
  const pending = actions.find((a) => a.status === "pending");

  // Remediation is optional: a deployment without the service answers 503, which
  // is a fact about the deployment rather than an error worth shouting about.
  const remediationUnavailable = remediation.error?.includes("not configured") ?? false;

  const run = async (label: string, fn: () => Promise<unknown>) => {
    setBusy(true);
    setMessage(null);
    try {
      await fn();
      setIsError(false);
      setMessage(`${label} succeeded.`);
      notifications.refresh();
      remediation.refresh();
      onChanged();
    } catch (err) {
      setIsError(true);
      // The API's 409s are informative ("already resolved", "nothing pending"),
      // so surface the server's own message rather than a generic failure.
      setMessage(err instanceof ApiError ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="detail">
      <header className="detail-header">
        <div>
          <h2>{incident.title}</h2>
          <p className="detail-sub">
            <span className={`badge badge-${incident.severity}`}>{incident.severity}</span>
            <span className={`status status-${incident.status}`}>{incident.status}</span>
            <span>{incident.service_name}</span>
            <span>{incident.tenant_id}</span>
            <span>rule: {incident.rule_id}</span>
          </p>
        </div>
        <div className="detail-actions">
          <button
            type="button"
            className="btn"
            disabled={busy || incident.status !== "open"}
            onClick={() => run("Acknowledge", () => acknowledgeIncident(incident.id))}
          >
            Acknowledge
          </button>
          <button
            type="button"
            className="btn btn-primary"
            disabled={busy || incident.status === "resolved"}
            onClick={() => run("Resolve", () => resolveIncident(incident.id))}
          >
            Resolve
          </button>
        </div>
      </header>

      {message && (
        <p className={isError ? "banner banner-error" : "banner banner-ok"} role="status">
          {message}
        </p>
      )}

      <dl className="facts">
        <div>
          <dt>Events</dt>
          <dd>{incident.event_count}</dd>
        </div>
        <div>
          <dt>Opened</dt>
          <dd>{relativeTime(incident.opened_at)}</dd>
        </div>
        <div>
          <dt>Last seen</dt>
          <dd>{relativeTime(incident.last_seen_at)}</dd>
        </div>
        <div>
          <dt>Acknowledged</dt>
          <dd>{incident.acknowledged_at ? relativeTime(incident.acknowledged_at) : "—"}</dd>
        </div>
        <div>
          <dt>Resolved</dt>
          <dd>{incident.resolved_at ? relativeTime(incident.resolved_at) : "—"}</dd>
        </div>
      </dl>

      <h3>Alert timeline</h3>
      {notifications.error ? (
        <p className="empty">Could not load notifications: {notifications.error}</p>
      ) : (notifications.data?.notifications.length ?? 0) === 0 ? (
        <p className="empty">Nobody has been paged for this incident yet.</p>
      ) : (
        <ol className="timeline">
          {notifications.data?.notifications.map((n) => (
            <li key={n.id}>
              <span className="timeline-level">L{n.level}</span>
              <span className="timeline-main">
                <strong>{n.target}</strong> → {n.contact}
              </span>
              <span className={`pill pill-${n.status}`}>{n.status}</span>
              <span className="timeline-time">{relativeTime(n.sent_at)}</span>
            </li>
          ))}
        </ol>
      )}

      <h3>Remediation</h3>
      {remediationUnavailable ? (
        <p className="empty">Remediation is not configured on this deployment.</p>
      ) : remediation.error ? (
        <p className="empty">Could not load remediation: {remediation.error}</p>
      ) : actions.length === 0 ? (
        <p className="empty">No runbook has run for this incident.</p>
      ) : (
        <ol className="timeline">
          {actions.map((a) => (
            <RemediationRow key={a.id} action={a} />
          ))}
        </ol>
      )}

      {pending && (
        <div className="approval" role="group" aria-label="Approve or reject the pending step">
          <p>
            <strong>“{pending.step_name}”</strong> is waiting for a decision before it runs.
          </p>
          <div className="detail-actions">
            <button
              type="button"
              className="btn btn-danger"
              disabled={busy}
              onClick={() => run("Reject", () => decideRemediation(incident.id, "reject", ACTOR))}
            >
              Reject
            </button>
            <button
              type="button"
              className="btn btn-primary"
              disabled={busy}
              onClick={() => run("Approve", () => decideRemediation(incident.id, "approve", ACTOR))}
            >
              Approve
            </button>
          </div>
          <p className="approval-note">
            Approving is applied by the workflow asynchronously — the step's status updates here
            within a few seconds.
          </p>
        </div>
      )}
    </section>
  );
}

function RemediationRow({ action }: { action: RemediationAction }) {
  return (
    <li>
      <span className="timeline-level">{action.step_index}</span>
      <span className="timeline-main">
        <strong>{action.step_name}</strong>
        <span className="muted">
          {" "}
          {action.action_kind} · {action.mode}
          {action.actor ? ` · ${action.actor}` : ""}
        </span>
      </span>
      <span className={`pill pill-${action.status}`}>{action.status.replace("_", " ")}</span>
      <span className="timeline-time">{relativeTime(action.updated_at)}</span>
    </li>
  );
}
