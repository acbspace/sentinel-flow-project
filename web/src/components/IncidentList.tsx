import type { Incident, IncidentStatus } from "../types";

interface Props {
  incidents: Incident[];
  selectedId: string | null;
  statusFilter: IncidentStatus | "";
  onSelect: (id: string) => void;
  onFilterChange: (status: IncidentStatus | "") => void;
}

const FILTERS: Array<{ label: string; value: IncidentStatus | "" }> = [
  { label: "Open", value: "open" },
  { label: "Acknowledged", value: "acknowledged" },
  { label: "Resolved", value: "resolved" },
  { label: "All", value: "" },
];

/** Relative time, because "3m ago" is what an on-call engineer actually reads. */
export function relativeTime(iso: string): string {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "—";

  const seconds = Math.round((Date.now() - then) / 1000);
  if (seconds < 0) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

export function IncidentList({
  incidents,
  selectedId,
  statusFilter,
  onSelect,
  onFilterChange,
}: Props) {
  return (
    <aside className="list">
      <div className="list-filters" role="group" aria-label="Filter incidents by status">
        {FILTERS.map((f) => (
          <button
            key={f.label}
            type="button"
            className={statusFilter === f.value ? "chip chip-active" : "chip"}
            aria-pressed={statusFilter === f.value}
            onClick={() => onFilterChange(f.value)}
          >
            {f.label}
          </button>
        ))}
      </div>

      {incidents.length === 0 ? (
        <p className="empty">No incidents match this filter.</p>
      ) : (
        <ul className="incident-list">
          {incidents.map((inc) => (
            <li key={inc.id}>
              <button
                type="button"
                className={inc.id === selectedId ? "incident-row incident-row-active" : "incident-row"}
                onClick={() => onSelect(inc.id)}
                aria-current={inc.id === selectedId}
              >
                <span className="incident-row-top">
                  <span className={`badge badge-${inc.severity}`}>{inc.severity}</span>
                  <span className={`status status-${inc.status}`}>{inc.status}</span>
                </span>
                <span className="incident-row-service">{inc.service_name}</span>
                <span className="incident-row-title">{inc.title}</span>
                <span className="incident-row-meta">
                  {inc.event_count} events · {relativeTime(inc.last_seen_at)}
                </span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </aside>
  );
}
