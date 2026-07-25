# ADR 0003: Temporal for alert escalation

- **Status:** Accepted
- **Date:** 2026-07-24
- **Deciders:** SentinelFlow engineering
- **Supersedes:** none
- **Builds on:** [0001](0001-event-driven-architecture.md), [0002](0002-correlation-and-incident-lifecycle.md)

## Context

Correlation detects incidents, but an incident nobody is told about is worthless.
Alerting pages the on-call responder, waits for an acknowledgement, and escalates
to the next responder if none comes — the loop a real incident platform lives or
dies by.

The forces:

1. **Escalation is a long-lived, timer-driven process.** "Page primary, wait two
   minutes, page secondary, wait five, page the manager" is a workflow measured in
   minutes to hours. It must survive process restarts and deploys; an escalation
   that evaporates because a pod was rescheduled is worse than no alerting,
   because it is silently trusted.

2. **It is human-in-the-loop.** The workflow blocks on an event that arrives
   out-of-band, from a different service, at an unpredictable time — someone
   acknowledging. That is not a computation; it is a rendezvous.

3. **Paging the wrong number of times is a real failure.** Double-paging erodes
   trust as fast as not paging at all. Each escalation level must fire exactly
   once per incident, including across retries and restarts.

4. **Alerting must not endanger ingestion or detection.** As with correlation
   (ADR 0002), the pipeline's core job outranks this one.

5. **The escalation must be auditable.** "Who was paged, when, and did it
   escalate?" has to be answerable after the fact, from data, not from logs.

## Decision

**Escalation runs as a Temporal workflow, one per incident, hosted by a new
`alerting` service. The workflow id is the incident id; acknowledgement arrives as
a Temporal signal; the database remains the source of truth.**

Concretely:

- **Temporal, not a hand-rolled scheduler.** Durable timers, automatic retries,
  restart-survivability and signal-based rendezvous are exactly Temporal's
  purpose, and they are precisely forces 1 and 2. Writing a correct durable timer
  wheel with at-most-once side effects is a project in itself; the design
  spends a dependency instead of re-deriving one.

- **Workflow id = incident id** (`incident-alert-<uuid>`) with a
  `REJECT_DUPLICATE` reuse policy, and an "already started" error treated as
  success. This makes "exactly one alert workflow per incident" a server-side
  guarantee (force 3), and makes the starter poller idempotent and restart-safe.
  Because a recurrence is a *new* incident with a new id (ADR 0002), it correctly
  gets its own workflow.

- **Signals are the fast path; the database is the authority.** The incidents-api
  signals `acknowledge`/`resolve` after committing the transition, so escalation
  stops within milliseconds. But the workflow *also* re-reads incident status via
  an activity before every escalation, so a lost or failed signal still converges
  to the correct outcome. The signal is an optimisation, never the source of
  truth — which is why a signal failure is logged rather than failing the API call.

- **Notifications are a durable audit table, not just log lines** (force 5). Each
  dispatch is a row: level, target, resolved contact, channel, status, evidence.
  The workflow supplies a deterministic notification id (a UUIDv5 of
  incident:level:reason), so an activity retry or a workflow replay records the
  dispatch exactly once via `ON CONFLICT DO NOTHING`.

- **A separate `alerting` service** hosts the worker and the starter poller. The
  incident engine is untouched and never learns Temporal exists (force 4); the
  poller simply reads the incidents the engine already writes, marking each with
  `alerted_at` so it starts one workflow apiece.

- **On-call schedules and escalation policies are data**: an ordered list of
  levels, each with a responder rotation resolved by a deterministic shift
  calculation. Defined as JSON with a default embedded in the binary.

## Alternatives considered

### A. A database-backed escalation poller, like the correlation loop

Store escalation state in a table and advance it from a ticker, exactly as
the correlation engine does.

*Rejected, though it was close.* It would fit the project's minimal-dependency
ethos and reuse a proven pattern. But it means hand-building durable timers,
at-most-once side-effect semantics across restarts, retry/backoff per
notification, and a bespoke rendezvous for acknowledgement — the exact set of
problems Temporal exists to solve, and each an opportunity to double-page someone
at 3am. Correlation already demonstrates the DB-polling pattern; spending it again
here would have bought consistency at the cost of correctness risk and would have
taught the codebase nothing new.

### B. Sleep-and-loop inside the incident engine

*Rejected* against force 4 and force 1: it couples alerting to the consume loop
and loses every in-flight escalation on restart.

### C. Delegate to a paging SaaS (PagerDuty, Opsgenie)

*Rejected here, and it is the honest production answer.* A real
deployment should hand escalation to a service that owns phone trees, mobile push
and quiet hours. But this project exists to build the mechanism, not to integrate
one; and an integration would be untestable here without credentials and a live
external account. The webhook sink is the seam where such an integration lands.

### D. Cron-style re-evaluation of every open incident

*Rejected.* It computes "should we page now?" from scratch every tick, which
makes exactly-once paging a deduplication problem on top of a scheduling problem —
strictly harder than modelling the escalation as the sequential process it is.

## Consequences

### Positive

- **Escalations survive restarts and deploys.** Workflow state lives in Temporal,
  not in the worker's memory.
- **Exactly one alert workflow per incident**, enforced by the server, not by
  application locking.
- **Acknowledgement is immediate but not fragile** — a signal for latency, a
  database re-check for correctness.
- **The alert timeline is queryable data**, exposed at
  `GET /v1/incidents/{id}/notifications`.
- **Alerting cannot take down ingestion**; it is a separate service and a separate
  failure domain.
- **Escalation logic is unit-testable without infrastructure**, via Temporal's
  test framework with virtual time — the whole escalate/ack/resolve matrix runs in
  milliseconds with no server.

### Negative

- **A large new dependency and a new piece of infrastructure.** Temporal is a
  server (plus its own schema, here reusing PostgreSQL) that must be run,
  upgraded and understood. This is by far the heaviest thing the project depends
  on, and it is only worth it because escalation is genuinely a durable-execution
  problem.
- **Workflow code is constrained.** Workflow bodies must be deterministic and
  replay-safe: no clocks, randomness or I/O outside activities. That discipline is
  invisible in the diff and easy for a future contributor to violate.
- **Policy changes do not affect in-flight workflows.** The policy travels in the
  workflow input so replays stay deterministic, which means an escalation already
  running keeps the policy it started with.
- **Two places can stop an escalation** (signal, DB re-check). That redundancy is
  deliberate but is more surface than a single mechanism.

### Neutral

- Delivery is a durable audit row plus an optional webhook; there is no real
  Slack/email/PagerDuty integration, so "paged" means "recorded and webhooked".
- The alert worker is stateless and horizontally scalable — Temporal distributes
  workflow and activity tasks across however many workers poll the task queue.

## Validation

Acceptance criteria:

- An open incident pages level 1 promptly. *Verified by
  `TestAlertingAcknowledgeStopsEscalation`'s first phase against a live Temporal,
  and by the `testsuite` workflow tests.*
- An unacknowledged incident escalates to the next level. *Verified by
  `TestAlertingEscalatesWhenUnacknowledged` and
  `TestWorkflowEscalatesThroughEveryLevelWhenUnacknowledged`.*
- Acknowledging stops escalation. *Verified by
  `TestAlertingAcknowledgeStopsEscalation` (both the signal and the database
  path) and `TestWorkflowStopsOnAcknowledgeSignal`.*
- An already-closed incident pages no one. *Verified by
  `TestWorkflowSendsNothingWhenIncidentAlreadyClosed`.*
- A failing webhook does not stall escalation. *Verified by
  `TestNotifierRecordsFailedWebhookWithoutErroring`.*

## Revisit this decision if

- Escalation stops being the only durable workflow in the system and Temporal is
  carrying a whole platform — at which point the dependency pays for itself many
  times over, and remediation should run on it too.
- Conversely, if escalation stays this small and the operational cost of running
  Temporal outweighs it, alternative A becomes reasonable again.
- The deployment gains a real paging provider, making alternative C the right
  place for escalation and reducing this workflow to a dispatcher.

## References

- `docs/architecture.md` §13 — the workflow, the starter, and the signal path.
- ADR 0002 — the incident lifecycle these alerts hang off.
