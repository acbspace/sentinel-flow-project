# ADR 0002: Windowed correlation and the incident lifecycle

- **Status:** Accepted
- **Date:** 2026-07-24
- **Deciders:** SentinelFlow engineering
- **Supersedes:** none
- **Builds on:** [0001](0001-event-driven-architecture.md)

## Context

Ingestion ends at normalized storage: events land in `telemetry_events` and
nothing draws a conclusion from them. Correlation is the first reaction — it
watches the stream, decides when a service is unhealthy, and records that as an
**incident** with a lifecycle an operator can act on.

The forces shaping this:

1. **The interesting signals are properties of a window, not of an event.** "This
   one request failed" is not actionable; "half of this service's requests failed
   in the last minute" is. Detection has to reason over an aggregate across time,
   which a single event in isolation cannot express.

2. **A spike is one incident, not a thousand.** When a service starts failing it
   produces a flood of bad events. The output must be *one* incident that grows,
   not a new incident per detection or per event. Duplicate incidents are as
   useless as duplicate pages.

3. **Correlation must not endanger ingestion.** Ingestion is the job the platform
   cannot fail (ADR 0001). Correlation is secondary: a bug or a slow query in it
   must never stall the consume loop or drop an event.

4. **Detections repeat and state must survive restarts.** The engine restarts,
   and may run more than one replica. Two evaluations of the same condition must
   converge on the same single incident, not race to create two.

5. **An incident has a life, and part of it is human.** It opens automatically,
   but a person acknowledges it and a person (or a quiet period) resolves it. The
   model has to represent that progression honestly.

6. **The stored data must become queryable.** Ingestion provides no read path at all.
   Incidents are worthless if the only way to see them is `psql`.

## Decision

**Correlation is a windowed evaluator that runs on a fixed cadence, queries
PostgreSQL for per-service tallies, and opens or groups incidents keyed by a
deterministic fingerprint. It runs in-process inside the incident engine. A
separate read service exposes incidents and events over HTTP.**

Concretely:

- **Windowed, database-backed evaluation.** On each tick the evaluator asks the
  database for per-`(tenant, service)` event counts over the rule's window
  (total, and error/critical), and applies each rule to the result. All state
  lives in the database; the evaluator holds none between ticks. This satisfies
  forces 1 and 4 directly: restarts and replicas are safe because the source of
  truth is the shared table, and evaluation is idempotent.

- **Rules are data.** A rule is `{kind, window, threshold, min-events, severity}`.
  The first and only kind is `error_rate`. `min-events` prevents a lone failure
  in an idle service from reporting a 100% error rate over a sample of one.

- **Fingerprint dedup via a partial unique index.** An incident's identity is
  `rule : tenant : service`. A partial unique index — `UNIQUE (fingerprint)
  WHERE status <> 'resolved'` — enforces at most one active incident per
  fingerprint, and `INSERT … ON CONFLICT … DO UPDATE` opens-or-groups in a single
  atomic statement. This is force 2 made a database invariant rather than
  application logic that races under concurrency.

- **One-directional lifecycle.** `open → acknowledged → resolved`, never
  backwards. A recurrence of a resolved condition opens a *new* incident (the
  partial index permits it, since resolved rows are exempt), so each incident
  describes one contiguous episode with an honest start and end.

- **Auto-resolution decoupled from detection.** An incident whose most recent
  detection is older than a quiet period is auto-resolved on the next tick,
  independent of whether any rule is currently firing. Crucially, a failed window
  read aborts the cycle *before* auto-resolution, so a transient database blip
  cannot masquerade as "everything went quiet" and close every incident.

- **In-process, not a new service.** The evaluator runs as a third goroutine in
  the incident engine's errgroup, beside the consume loop and probe server, over
  the same pool and shutdown lifetime. Unlike the consume loop it logs-and-
  continues on failure rather than propagating, honouring force 3.

- **A separate read service.** `incidents-api` serves incident queries, the
  acknowledge/resolve transitions, and a read API over stored events. It is
  distinct from the write-only ingestion API, mirroring read/write separation and
  letting the two scale independently (force 6).

## Alternatives considered

### A. Per-event streaming correlation in the consume loop

Maintain in-memory sliding-window counters and evaluate as each event is stored.

*Rejected.* It couples correlation to the ingest hot path (against force 3), and
its state is in memory, so a restart loses every window and a second replica
double-counts (against force 4). Windowed aggregates are exactly what SQL over the
already-durable table computes cheaply and correctly, without a second source of
truth to keep consistent.

### B. A standalone correlator service

Give correlation its own binary and deployment.

*Rejected for now, but the seams are drawn for it.* At this scale a separate
service buys operational cost — another image, another deployment, another thing
to monitor — for scaling independence nothing yet needs. The evaluator depends on
narrow `WindowSource`/`IncidentSink` interfaces, so promoting it to its own
process later is a wiring change, not a rewrite. When correlation's cadence and
the consumer's throughput want to scale independently, revisit this.

### C. Fingerprint as the incident primary key

Use the fingerprint itself as the key instead of a surrogate id with a partial
unique index.

*Rejected.* It makes force 5 impossible: a fingerprint would map to exactly one
row for all time, so a resolved incident could never coexist with a fresh
recurrence. The surrogate key plus partial index gives both properties — one
*active* incident per fingerprint, but a full history of past episodes.

### D. Reopening resolved incidents

On recurrence, flip the most recent resolved incident back to open.

*Rejected.* It conflates two separate outages into one record with a misleading
duration and a dishonest history. A new incident per episode is easier to reason
about and to report on ("how many times did this break this week?").

### E. Rules stored in the database, editable at runtime

*Deferred.* The rule model is already data, but it is constructed in code and
configured by environment variable. A rules table with a management API is real
value once there is more than one rule kind and someone other than a deployer
needs to change thresholds — but it is its own feature, not a prerequisite for
the first correlation.

## Consequences

### Positive

- **Restart- and replica-safe.** No in-memory correlation state means the engine
  can restart or scale out without losing or double-counting incidents.
- **Dedup is a database guarantee.** The partial unique index makes "one active
  incident per fingerprint" impossible to violate, even under concurrent
  evaluators — it is not defended by application-level locking.
- **Correlation cannot take down ingestion.** It shares a lifetime with the
  consume loop but not a failure mode; a bad cycle is logged and retried.
- **Honest incident history.** Each incident is one episode. Recurrences are
  distinct rows, so counting and trend analysis are meaningful.
- **The platform is finally queryable.** `incidents-api` turns stored state into
  something an operator or a UI can read and act on.

### Negative

- **Detection latency is bounded below by the interval.** An incident opens up to
  one `CORRELATION_INTERVAL` after the condition crosses the threshold. Shortening
  the interval trades database load for faster reaction.
- **The window query scans recent rows.** `WHERE event_timestamp >= since`
  aggregated per service originally leaned on the existing time-ordered indexes,
  none of which lead with `event_timestamp` and so none of which could seek on a
  bare time range; migration 0005 adds a dedicated one. A rollup is the next step
  if volume outgrows that.
- **A permanently failing service re-opens forever.** Once resolved, a still-bad
  service opens a fresh incident on the next cycle. That is correct (the problem
  really is still happening) but can be noisy without a suppression/snooze
  mechanism, which is future work.
- **Thresholds are global per deployment.** One env-configured error-rate rule
  applies to every service; a service with a legitimately high baseline error
  rate has no per-service override yet.

### Neutral

- Severity is fixed by the rule, and because the rule id is part of the
  fingerprint, an incident's severity cannot change within its lifetime. If
  escalation ("warn that became critical") is wanted later, it needs an explicit
  model, not an implicit one.
- The read API is intentionally unauthenticated, consistent
  with the rest of the stack living on a private Compose network.

## Validation

Acceptance criteria:

- An error spike opens exactly one incident, and a repeat detection groups into
  it rather than duplicating. *Verified by
  `TestCorrelationOpensGroupsAndReopensAfterResolve`, which asserts one incident
  after two cycles and an accumulated event count.*
- A resolved condition that persists opens a new, distinct incident. *Same test:
  after manual resolve, a further cycle yields two incidents (one resolved, one
  open) with different ids.*
- A quiet incident auto-resolves, and a failed read does not. *Verified by
  `TestCorrelationAutoResolvesQuietIncident` and the unit test
  `TestEvaluateOnceDoesNotResolveWhenReadFails`.*
- A healthy service opens nothing. *Verified by
  `TestCorrelationLeavesHealthyServiceAlone`.*
- Lifecycle transitions answer the right status codes. *Verified by the
  `incidentapi` handler tests: 200/404/409 across acknowledge and resolve.*

## Revisit this decision if

- Correlation's load or cadence needs to scale independently of the consumer, at
  which point alternative B (a standalone correlator) is the natural next step.
- Rule count or the need for runtime edits grows, making alternative E (a rules
  table and management API) worth its cost.
- Detection latency needs to drop below what a polling interval can offer, which
  would push toward a streaming design and reopen alternative A's trade-offs under
  new constraints.

## References

- `docs/architecture.md` — the correlation section: window query, fingerprinting,
  and the lifecycle state machine.
- ADR 0001 — the event-driven foundation this builds on.
