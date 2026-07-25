# ADR 0004: Runbook remediation with approval gates

- **Status:** Accepted
- **Date:** 2026-07-24
- **Deciders:** SentinelFlow engineering
- **Supersedes:** none
- **Builds on:** [0002](0002-correlation-and-incident-lifecycle.md), [0003](0003-temporal-for-alert-escalation.md)

## Context

Alerting pages a human. Remediation asks the harder question: what can the
platform do about an incident *itself*, and how do we make that safe?

Automated remediation is the highest-risk feature in this project. Everything
before it observes and reports; this one acts on production. The forces:

1. **A wrong automated action can be far worse than the incident.** Restarting
   the wrong service during a partial outage turns a degradation into a full one.
   The blast radius of a bug here is unbounded in a way that a missed page is not.

2. **The safe actions and the dangerous ones are not the same.** Collecting
   diagnostics is always fine. Restarting instances is not. Treating them
   identically means either blocking the harmless work behind a human, or letting
   the harmful work run unattended.

3. **Automation must stop when a human says stop** — and stay stopped. A system
   that continues to the next step after a rejection has not implemented approval;
   it has implemented a delay.

4. **Every action needs an audit trail, including the ones that did not happen.**
   "Why did the robot restart production at 3am?" and "why didn't it?" are both
   post-incident questions, and both need answers from data.

5. **Remediation runs a multi-step process with human pauses in the middle** —
   the same durable, timer-driven, signal-driven shape as escalation.

## Decision

**Remediation is a runbook engine on Temporal: an ordered list of steps per
incident, each either unattended or gated behind a human approval, with every
outcome recorded and any refusal halting the run.**

Concretely:

- **Runbooks are data, matched to incidents.** A runbook has a matcher (rule id
  required, service and severity optional) and ordered steps. The catalog is JSON
  with a default embedded in the binary; the first match wins. Requiring a rule id
  means a runbook can never apply to everything by accident.

- **Per-step modes, not per-runbook.** Each step is `auto` or `approval`,
  answering force 2 directly: the diagnostic step runs immediately, the
  restart step stops and waits. A test asserts the shipped catalog never performs
  a webhook action unattended.

- **Refusal halts the run — in every form.** A rejection, an approval timeout, a
  failed step, or the incident no longer being open all stop the runbook where it
  stands. The workflow never proceeds to step *n+1* after step *n* did not
  cleanly succeed (force 3). Continuing to automate after something already went
  wrong is how a small incident becomes a large one.

- **The incident is re-checked before every step.** Somebody resolving the
  incident is the clearest possible signal that automation should stand down, and
  it is honoured even mid-runbook.

- **A second Temporal workflow, not a second job in `alerting`.** The shape is the
  same as escalation (force 5), so it reuses the same machinery: workflow id =
  incident id for exactly-once, deterministic UUIDv5 action ids so retries and
  replays advance one audit row rather than duplicating it. But it runs in its own
  `remediation` service, because paging a human and acting on production are
  different blast radii and deserve separate failure domains.

- **Only two action kinds ship: `noop` and `webhook`.** Restarting deployments,
  draining nodes and rolling back releases belong behind that webhook, in a system
  that already owns those credentials and permissions. An incident platform that
  can itself restart your fleet is a much larger security and blast-radius
  proposition than one that asks something else to.

- **Every step is a row** in `remediation_actions` — including `rejected`,
  `timed_out` and `skipped` (force 4). The unique constraint on
  `(incident_id, step_index)` makes a step exactly one row whose status advances.

## Alternatives considered

### A. Fully automatic remediation, no approvals

*Rejected* against force 1. The value is real — mean-time-to-recovery drops when
nobody has to wake up — but it requires confidence in the detection rules that
this system has not earned. Correlation ships a single global error-rate
threshold; letting that alone trigger production changes would be reckless. When
detection is mature and per-service, individual steps can graduate to `auto`
without any code change, because the mode is data.

### B. Approval per runbook rather than per step

*Rejected* against force 2. It forces a choice between gating harmless
diagnostics behind a human (slow, and trains people to approve reflexively) or
letting a restart through unattended (dangerous). Per-step is barely more
complex and strictly more useful.

### C. Continue the runbook after a rejected step

*Rejected outright* against force 3. If a human declines a step, the remaining
steps were designed on the assumption that step happened. Proceeding is both
unsafe and incoherent.

### D. Built-in Kubernetes / cloud actions

*Rejected.* It would make the demo more impressive and the
project far more dangerous: the platform would need cluster credentials, and a
bug in a correlation rule could reach them. The webhook indirection keeps the
permission boundary where it belongs. A deployment that wants real actions puts
a small, tightly-scoped service behind that URL.

### E. A generic "workflow" service hosting both alerting and remediation

*Rejected*, though it would be fewer moving parts. Paging is safe and should keep
running even if remediation is broken or deliberately disabled; sharing a process
couples those fates and muddies what the service is for.

## Consequences

### Positive

- **The dangerous path is gated by construction**, and the gate is data, so
  changing what is automatic is a config change with an audit trail, not a deploy.
- **Refusal is total.** Rejected, timed out, failed and stood-down all halt, and
  all are recorded.
- **The audit trail answers both post-incident questions** — what ran, and what
  was declined, by whom.
- **Exactly-once actions**, inherited from the same Temporal properties as
  alerting: no step is executed twice by a retry or a replay.
- **Blast radius is bounded by design.** The platform holds no cluster
  credentials; the most it can do unattended is POST to a URL somebody configured.

### Negative

- **Approval gates mean remediation is not fast.** With a human in the loop, the
  time-to-recovery benefit is limited to whatever the `auto` steps achieve. That
  is the correct trade at this maturity, but it is a trade.
- **A second Temporal worker and service** to run, monitor and deploy.
- **The runbook catalog is global config**, so changing it is a deployment
  concern; there is no per-team ownership or review workflow around it.
- **`noop` steps do nothing but tell the truth about it.** The demo's automation
  is therefore mostly ceremonial until somebody wires a real endpoint to the
  webhook — honest, but less impressive than it sounds.

### Neutral

- Steps run strictly in order with no parallelism or rollback. Both are plausible
  future needs; neither is required to demonstrate a gated runbook.
- An incident with no matching runbook is marked as handled so the poller stops
  re-examining it. "Nothing to automate" is a normal outcome, not an error.

## Validation

Acceptance criteria:

- An unattended step runs on its own; a gated step waits. *Verified by
  `TestRemediationRunsAutoStepThenGatesOnApproval` against a live Temporal, and
  `TestRemediationRunsAutoStepThenWaitsForApproval` in the workflow test suite.*
- A rejected step is never executed and the run halts. *Verified by
  `TestRemediationHaltsWhenRejected` (integration, including that the status does
  not later flip) and `TestRemediationHaltsWhenStepIsRejected` (unit).*
- Nobody approving halts the run. *Verified by
  `TestRemediationHaltsWhenApprovalTimesOut`.*
- A resolved incident stops automation dead, even before the first step.
  *Verified by `TestRemediationSkipsWhenIncidentAlreadyResolved`.*
- The shipped catalog never runs a real action unattended. *Verified by
  `TestDefaultCatalogIsValid`.*
- Deciding when nothing is pending is a conflict, not a silent success. *Verified
  by the `incidentapi` handler tests.*

## Revisit this decision if

- Detection matures to per-service, well-tuned rules with a track record, at which
  point specific steps can move from `approval` to `auto` — the model already
  supports it.
- Runbooks need per-team ownership, review or versioning, which points at a
  runbook store and management API rather than a config file.
- Rollback or compensating actions become necessary, which is a genuine workflow
  design change rather than a new step kind.

## References

- `docs/architecture.md` §14 — the runbook engine and the approval path.
- ADR 0003 — the Temporal foundation this reuses.
