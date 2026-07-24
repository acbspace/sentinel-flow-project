import { useEffect, useState } from "react";
import { listIncidents } from "./api";
import { IncidentDetail } from "./components/IncidentDetail";
import { IncidentList } from "./components/IncidentList";
import type { IncidentStatus } from "./types";
import { usePolling } from "./usePolling";

const REFRESH_MS = 5000;

export default function App() {
  const [statusFilter, setStatusFilter] = useState<IncidentStatus | "">("open");
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const incidents = usePolling(() => listIncidents(statusFilter), REFRESH_MS, [statusFilter]);
  const rows = incidents.data?.incidents ?? [];

  // Keep a selection that still exists in the current filter; otherwise fall
  // back to the first row so the detail pane is never pointing at nothing.
  useEffect(() => {
    if (rows.length === 0) {
      setSelectedId(null);
      return;
    }
    if (!selectedId || !rows.some((r) => r.id === selectedId)) {
      setSelectedId(rows[0]?.id ?? null);
    }
  }, [rows, selectedId]);

  const selected = rows.find((r) => r.id === selectedId) ?? null;

  return (
    <div className="app">
      <header className="app-header">
        <h1>
          Sentinel<span>Flow</span>
        </h1>
        <p className="app-sub">
          {incidents.error ? (
            <span className="app-error">API unreachable — {incidents.error}</span>
          ) : (
            <>
              {incidents.data?.count ?? 0} incident{(incidents.data?.count ?? 0) === 1 ? "" : "s"} ·
              refreshing every {REFRESH_MS / 1000}s
            </>
          )}
        </p>
      </header>

      <main className="app-body">
        <IncidentList
          incidents={rows}
          selectedId={selectedId}
          statusFilter={statusFilter}
          onSelect={setSelectedId}
          onFilterChange={setStatusFilter}
        />

        {incidents.loading && rows.length === 0 ? (
          <section className="detail detail-empty">Loading incidents…</section>
        ) : selected ? (
          <IncidentDetail incident={selected} onChanged={incidents.refresh} />
        ) : (
          <section className="detail detail-empty">
            <p>Nothing to show.</p>
            <p className="muted">
              Drive some traffic and force a spike: <code>make demo</code> then <code>make burst</code>.
            </p>
          </section>
        )}
      </main>
    </div>
  );
}
