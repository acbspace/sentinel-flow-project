# SentinelFlow architecture

This document explains how the pipeline works and, more importantly, why it works
the way it does. Sections 1–11 cover the ingestion pipeline: the event flow, the
Kafka partitioning and offset strategy, the idempotency guarantee, what happens
when each component fails, and why the database is indexed the way it is.
Section 12 covers correlation and the incident lifecycle: how the stored stream
becomes incidents, how duplicates are collapsed, and how an incident moves from
open to resolved. Section 13 covers alerting: how an open incident pages the
on-call responder, escalates when unacknowledged, and stops the moment someone
takes ownership. Section 14 covers remediation: what the platform may do about an
incident itself, and the approval gates that bound it. Section 15 covers the
dashboard, the telemetry collector, the Kubernetes manifests, and an honest
analysis of the multi-region question.

What is deliberately not here is named rather than implied: no auth anywhere, no
real paging or remediation providers (both sit behind webhooks), no frontend
tests, and no multi-region deployment — see §15.4 for why that last one is
analysis rather than code.

---

## 1. Event flow

```
order-service ──┐
                ├── POST /v1/events ──▶ ingestion-api ──▶ Kafka ──▶ incident-engine ──▶ PostgreSQL
payment-service ┘                       (validate)      (durable)    (validate,          (telemetry_events)
                                                                      persist)
```

A single event makes six hops:

1. **A demo service simulates work.** `order-service` handles `POST /demo/orders`,
   sleeps for a simulated latency, and (when `DOWNSTREAM_URL` is set) calls
   `payment-service`, which independently simulates its own work and failure
   rate. Both services build a telemetry event describing what happened: method,
   route, status code, latency, outcome and trace ID.

2. **The event is submitted over HTTP.** The demo service POSTs the event to the
   ingestion API using an `otelhttp`-instrumented client, so the request carries
   a `traceparent` header.

3. **The ingestion API validates it.** The body is size-capped, decoded strictly
   (unknown fields are rejected), normalized, and validated against the event
   contract. A rejected event never reaches Kafka; the caller gets a 400 listing
   every offending field at once.

4. **The event is published to Kafka.** The API serialises the event to JSON,
   keys the record `<tenant_id>:<service_name>`, injects the W3C trace context
   into the record headers, and produces **synchronously**. Only after the broker
   acknowledges the write does the API answer `202 Accepted`.

5. **The incident engine consumes it.** The engine polls the
   `telemetry.events.v1` topic as a member of the `incident-engine-v1` consumer
   group, extracts the trace context from the record headers so its span
   continues the original trace, and revalidates the event.

6. **The event is persisted.** The engine inserts a row into `telemetry_events`
   with `ON CONFLICT (event_id) DO NOTHING`, then commits the Kafka offset.

The `received_at` column comes from the Kafka record timestamp — the moment the
ingestion API produced it — and `processed_at` from the database clock at insert
time. The difference between them is the pipeline's true end-to-end latency.

---

## 2. Why an HTTP boundary in front of Kafka

The demo services could publish to Kafka directly. They don't, for three reasons:

- **Validation belongs at a trust boundary.** One service owning the schema check
  is far easier to reason about than every producer embedding a Kafka client and
  its own idea of what a valid event is.
- **Producers should not know about the transport.** Swapping Kafka for something
  else later becomes an ingestion API change, not a fleet-wide change.
- **Brokers are not a public interface.** Exposing Kafka credentials to every
  application service is a security and operations problem the HTTP hop avoids.

The cost is one extra network hop and one more service to run. For a telemetry
pipeline where producers are numerous and the schema must hold, that trade is
worth it.

---

## 3. Kafka partitioning strategy

**Topic:** `telemetry.events.v1` (3 partitions locally, replication factor 1).

**Message key:** `<tenant_id>:<service_name>`, for example
`demo-tenant:payment-service`.

Kafka guarantees ordering *within a partition*, and a keyed record always lands
on the same partition (`hash(key) % partitions`). That gives exactly the ordering
guarantee this system needs:

- Every event from one service of one tenant is processed in the order it was
  produced.
- Events from different services, or different tenants, are free to be processed
  in parallel across partitions.

Alternative keys, and why they were rejected:

| Key choice | Consequence |
|---|---|
| `event_id` | Perfectly uniform spread, but no ordering guarantee whatsoever. Ordering is the reason to key at all. |
| `tenant_id` alone | A single busy tenant becomes one hot partition, and all of its services serialise behind each other. |
| No key (round-robin) | Best balance, zero ordering. Correlating "this service degraded, then this one did" becomes impossible. |
| `trace_id` | Groups one request's events together, but produces unbounded key cardinality and no per-service ordering. |

`<tenant_id>:<service_name>` is the smallest unit that preserves the ordering the
correlation engine will need, while still spreading load across partitions.

**The known weakness:** a tenant whose traffic is dominated by one service still
concentrates on one partition, because that key is indivisible by design. The
mitigation when it bites is a composite key with a bucket suffix
(`tenant:service:<bucket>`) and correlation logic that tolerates it — a change
worth making only when measurements show the imbalance, not before.

**Partition count.** Three locally, because partition count is the ceiling on
consumer parallelism: a consumer group can never have more *working* members than
partitions. Three lets us demonstrate a real rebalance. Increasing partitions
later is possible but changes the key-to-partition mapping, so historical
ordering is only preserved from that point forward.

---

## 4. Consumer group design

**Group ID:** `incident-engine-v1`.

The group name carries a version suffix deliberately. Consumer offsets are stored
per group, so changing the group name means "start over from the beginning of the
topic". Baking `v1` into the name makes a full reprocess an explicit, deliberate
act (deploy as `incident-engine-v2`) rather than an accident.

Scaling behaviour:

- Run *n* engine replicas and Kafka distributes the 3 partitions among them.
- Each partition is owned by exactly one member at a time, so per-partition
  ordering survives horizontal scaling.
- Replicas beyond the partition count idle as hot standbys. That is wasted
  capacity but not a correctness problem.

The engine uses `BlockRebalanceOnPoll`, which prevents a rebalance from landing
between processing a batch and committing its offsets. Without it, a rebalance at
that exact moment would hand the partition to another member that would reprocess
records the first member had already stored — still correct thanks to the
idempotency constraint, but it would burn work needlessly.

`OnPartitionsLost` is logged separately from `OnPartitionsRevoked`: a *revoke* is
a clean handover, whereas *lost* means the session expired and uncommitted work
will definitely be redelivered.

---

## 5. Offset commit strategy

Automatic offset commits are **disabled**. The engine commits explicitly, and
only after PostgreSQL has confirmed the write:

```
poll records
  └── for each record:
        decode → validate → INSERT ... ON CONFLICT DO NOTHING (with retries)
  └── all records durable?
        yes → CommitRecords()  → AllowRebalance()
        no  → return without committing → records are redelivered
```

This ordering is the entire delivery guarantee. Consider the alternative:

- **Commit before writing (at-most-once):** the engine commits, crashes before
  the insert, and the event is gone forever. Unacceptable for telemetry that will
  drive incident detection.
- **Commit after writing (at-least-once):** the engine writes, crashes before
  committing, and the event is redelivered and reprocessed. Safe, *provided*
  reprocessing is harmless — which is what the idempotency constraint ensures.

The commit itself uses a context derived with `context.WithoutCancel`, so a
shutdown signal arriving mid-batch still gets the chance to record work that is
already durable, rather than throwing away a correct commit and forcing a replay.

---

## 6. Idempotency strategy

**The mechanism is one line of SQL:**

```sql
INSERT INTO telemetry_events (...) VALUES (...)
ON CONFLICT (event_id) DO NOTHING
```

`event_id` is the primary key, and it is supplied by the *producer* — not
generated by the database. That is what makes it usable as a deduplication key:
the producer decides the identity of the event once, and every subsequent copy of
that event, however it arrives, carries the same identity.

`RowsAffected() == 0` means the row already existed. The engine reports that as
`OutcomeDuplicate`, logs it, counts it in the `sentinelflow.kafka.consumed`
metric with `outcome=duplicate`, and commits the offset. A duplicate is a normal
event, not an error.

### Where duplicates actually come from

At-least-once delivery is not a theoretical concern; here are the concrete paths:

1. **Engine crashes after the insert, before the commit.** The offset still points
   at the record. On restart it is redelivered and the insert is a no-op.
2. **Rebalance during processing.** A member loses its partition assignment before
   committing; the new owner replays from the last committed offset.
3. **Offset commit itself fails.** Network failure between a successful insert and
   a successful commit produces the same replay.
4. **A retried database insert that actually succeeded.** The connection dropped
   after PostgreSQL applied the write but before the driver saw the response. The
   retry lands on `ON CONFLICT DO NOTHING`. This is precisely why the insert must
   be idempotent before it is safe to retry it at all.
5. **A producer resubmitting the same event.** A demo service retrying a failed
   `POST /v1/events` that had actually succeeded.

Cases 1–4 are handled at the database. Case 5 is handled at the same place, which
is the point: **there is exactly one deduplication authority, and it is the
`event_id` primary key.** Nothing else in the pipeline attempts to deduplicate,
because a second mechanism would only create a second thing to be wrong.

### Why the database and not an in-memory cache

A cache in the engine would be faster and would be wrong. It cannot see what
other replicas have written, it is empty after a restart, and it would need its
own eviction policy. The unique index is authoritative under concurrency across
every replica, survives restarts, and costs one index probe.

### What idempotency does *not* cover

Two *different* `event_id`s describing the same real-world occurrence are two
distinct events as far as this system is concerned. If a demo service generated a
fresh UUID on every retry of the same logical request, deduplication would not
help. Producers own the identity of their events; that contract is documented in
the README and enforced only by convention.

---

## 7. Failure scenarios

| # | Failure | Behaviour | Data outcome |
|---|---|---|---|
| 1 | **Kafka unreachable at publish** | `ProduceSync` returns a delivery error. The API logs it and answers `503` with `Retry-After`. | No event lost, no silent drop. The producer is told to retry. |
| 2 | **Kafka slow** | The produce is bounded by `KAFKA_PRODUCE_TIMEOUT` (default 10s). The HTTP write timeout is deliberately larger, so the produce times out first and yields a clean 503. | Same as 1. |
| 3 | **Ingestion API crashes after publishing, before responding** | The record is already durable in Kafka; the client sees a connection error and may retry. | Possible duplicate, collapsed by `event_id`. |
| 4 | **PostgreSQL unreachable** | The insert fails with a retryable error. The engine retries with exponential backoff (5 attempts, 100ms → 5s). If all attempts fail, it returns an error **without committing**, the consume loop exits, and the process exits non-zero. | Nothing lost. The container restarts and reprocesses from the last committed offset. |
| 5 | **PostgreSQL rejects the row permanently** (constraint violation, type error) | Classified as non-retryable by `store.IsRetryable`. Logged at error level, wrapped in `ErrPermanent`, offset not committed. | Loud failure. This indicates a bug, not an outage, and it should not be quietly skipped. |
| 6 | **Poison message on the topic** (undecodable, or fails validation) | Logged at error level with topic, partition, offset and key, counted as `outcome=invalid`, **offset committed**. | The record is dropped on purpose. Retrying cannot help, and blocking the partition forever on one bad record would be a worse outage than losing it. The log line is the audit trail until a dead-letter topic exists. |
| 7 | **Engine crashes mid-batch** | Offsets were never committed. | Redelivery, deduplicated at the database. |
| 8 | **Consumer group rebalance** | `BlockRebalanceOnPoll` defers it until after the commit. | No duplicate work in the normal case. |
| 9 | **Demo service cannot reach the ingestion API** | The emit error is logged at error level; the HTTP response to the caller is unchanged. | The telemetry event is lost. This is a deliberate choice: an observability failure must not convert a successful business operation into a failed one. A production system would buffer locally. |
| 10 | **Downstream demo service unavailable** | `order-service` answers `502`; a *declined* payment answers `402`. Both still emit telemetry, with severity derived from the status. | Nothing lost. |
| 11 | **SIGTERM during processing** | The context cancels, the loop stops, in-flight commits use a non-cancellable context, servers drain within the grace period, the consumer leaves the group cleanly. | At worst, redelivery of an uncommitted batch. |

The consistent rule across all of these: **an error is either retried, or reported
loudly, or explicitly and visibly dropped with a documented reason.** Nothing is
swallowed.

### Failure mode this design does not handle well

Scenario 5 (a permanent database rejection) currently stalls the partition: the
offset is never committed, so the engine will restart and hit the same record
forever. That is the correct default — silently skipping a row that *should* have
been storable would hide a real bug — but it means a single malformed-but-valid
event can halt a partition until an operator intervenes. A dead-letter topic is
the fix, and it is on the roadmap rather than built.

---

## 8. Database indexing choices

Every index is a composite of a filter column plus `event_timestamp DESC`,
because every query this system will run has the same shape: *some slice of
events, most recent first*.

```sql
(tenant_id,    event_timestamp DESC)
(service_name, event_timestamp DESC)
(event_type,   event_timestamp DESC)
(severity,     event_timestamp DESC)
(trace_id) WHERE trace_id <> ''
```

The composite ordering matters. With `(tenant_id, event_timestamp DESC)`, a query
for one tenant's 100 most recent events seeks directly to the first matching
index entry and reads 100 rows. With a bare index on `tenant_id`, the same query
reads *every* row for that tenant and then sorts them — fine at a thousand rows,
ruinous at ten million.

`DESC` is explicit because PostgreSQL can scan an index backwards efficiently
anyway, but matching the physical order to the query's `ORDER BY` avoids a sort
node and makes the intent obvious to the next reader.

The `trace_id` index is **partial** (`WHERE trace_id <> ''`). Events submitted
without trace context all share the empty string, and indexing thousands of
identical keys costs storage and write throughput while helping no query.

`attributes` is `JSONB` rather than a wide column set, so a producer adding a new
attribute never requires a migration. It is deliberately **not** indexed: a GIN
index on JSONB is expensive to maintain on write, and no query here filters on
attributes yet. When one does, `CREATE INDEX ... USING GIN
(attributes jsonb_path_ops)` is the answer.

The `severity` `CHECK` constraint duplicates the application-level validation.
That redundancy is intentional: the application check gives callers a good error
message, and the database check guarantees the invariant even if a future code
path forgets to validate.

---

## 9. Observability design

**Traces.** `otelhttp` instruments both the servers and the clients, so trace
context flows from `order-service` → `payment-service` → `ingestion-api`
automatically. The Kafka boundary is where it would normally break, so the
producer injects the W3C `traceparent` into the record headers and the consumer
extracts it, making the engine's processing span a child of the HTTP request that
created the event. One trace covers the whole journey.

**Logs.** Structured JSON via `log/slog`, one object per line. A custom handler
pulls `trace_id` and `span_id` off the context automatically, so every log line
emitted during a traced operation can be joined to its trace without the caller
remembering to add anything.

**Metrics.**

| Instrument | Purpose |
|---|---|
| `sentinelflow.http.server.requests` / `.duration` | Request rate and latency, labelled by *route pattern* rather than raw path so high-cardinality URLs cannot explode the label set. |
| `sentinelflow.kafka.published` / `.publish.duration` | Produce throughput and broker acknowledgement latency, labelled by outcome. |
| `sentinelflow.kafka.consumed` / `.process.duration` | Consume throughput labelled `stored` / `duplicate` / `invalid` / `failed`, which makes the duplicate rate directly observable. |
| `sentinelflow.db.operations` / `.operation.duration` | Database call volume and latency by operation and outcome. |

**Exporters** are selected by environment variable (`stdout`, `otlp`, `none`).
Adding an OpenTelemetry Collector later requires no application change: set
`OTEL_TRACES_EXPORTER=otlp` and `OTEL_EXPORTER_OTLP_ENDPOINT`. The OTLP code path
is fully implemented, not stubbed.

The Compose default is `none` so that `make logs` shows application logs rather
than span dumps. That is a demo-ergonomics choice, not a limitation.

---

## 10. Trade-offs made in the ingestion pipeline

| Decision | Rationale | When to revisit |
|---|---|---|
| **JSON on the wire, not Avro or Protobuf** | Readable with `kcat` and `curl`, no schema registry to run, no code generation. | When throughput or schema governance matters. `schema_version` and the content-type header are the seam. |
| **Synchronous produce (`ProduceSync`)** | A `202` genuinely means durable. Async batching would be faster but would make the API lie about durability. | If ingest throughput becomes the bottleneck, batch with an explicit durability contract change. |
| **Strict decoding (unknown fields rejected)** | A misspelled field at the ingestion boundary is a producer bug, and silently ignoring it hides it. | If third-party producers need forward compatibility, relax to ignoring unknown fields and lean on `schema_version`. |
| **Poison messages dropped, not dead-lettered** | A dead-letter topic is real infrastructure with its own operational story. Logging loudly is the honest interim answer. | Immediately, if this ran in production. It is the first roadmap item. |
| **Whole-batch commit, not per-record** | One commit per batch instead of per record; a mid-batch failure replays the whole batch, which is harmless because inserts are idempotent. | If batches grow large enough that replaying one is expensive. |
| **Sequential record processing** | Preserves per-partition ordering and keeps the code obvious. Parallelism belongs across partitions, not within one. | Add engine replicas first; that is what partitions are for. |
| **Storage before correlation** | Getting events durably stored came first; correlation over an unreliable pipeline would be worthless. | Already revisited — see §12. |
| **Single-node Kafka, replication factor 1** | Local development. A broker failure loses data. | Any non-local deployment needs RF ≥ 3 and `min.insync.replicas=2`. |
| **No authentication anywhere** | The whole stack is on a private Compose network with no external exposure. | Before anything is exposed. mTLS or SASL for Kafka, an auth layer on the ingestion API. |
| **Tenant isolation by convention** | `tenant_id` is a column and a partition key, not a security boundary. | When multi-tenancy is real: row-level security, or separate schemas. |
| **`ON CONFLICT DO NOTHING`, not `DO UPDATE`** | Events are immutable facts. The first version wins; a late duplicate cannot rewrite history. | Never, for this table. Mutable state belongs in a different one. |

---

## 11. Package layout and dependency direction

```
cmd/<service>/main.go     wiring only: config → telemetry → dependencies → run
  └── internal/config     environment parsing, one struct per service
  └── internal/obs        logger, tracer/meter providers, metric instruments
  └── internal/httpx      timeout-hardened servers, health/ready, middleware
  └── internal/event      the event contract: types, normalization, validation
  └── internal/kafkax     franz-go producer/consumer, trace-context carrier
  └── internal/store      pgx pool, the insert, retryable-error classification
  └── internal/ingest     the HTTP handler
  └── internal/engine     the processor and the consume loop
  └── internal/incident   the incident domain: lifecycle, fingerprint
  └── internal/correlate  rules, the windowed evaluator, the ticker loop
  └── internal/incidentapi the read/lifecycle HTTP handler
  └── internal/oncall     escalation policy and on-call rotation
  └── internal/alerting   the Temporal workflow, activities, starter
  └── internal/runbook    runbook catalog: matchers, steps, approval modes
  └── internal/remediate  the remediation workflow, executor, starter
  └── internal/bench      the load generator's harness and report
deploy/k8s/                 Kubernetes manifests
web/                        React dashboard, 2 runtime dependencies
build/otel/                 OpenTelemetry Collector config
  └── internal/demo       simulator, emitter, demo handler, service bootstrap
  └── internal/migrate    migration runner
  └── migrations          embedded SQL
```

Dependencies point one way, toward `event`, which imports nothing from this
project. There are no cycles. The incident packages slot in above `store`:
`incident` depends only on `event`, `correlate` and `incidentapi` depend on
`store` and `incident`, exactly as `engine` and `ingest` do.

The seams that exist as interfaces, and only because tests need them:

- `ingest.Publisher` — lets the handler tests assert what *would* have been
  published without running a broker.
- `engine.EventStore` — lets the processor tests script database failures and
  duplicates deterministically.
- `correlate.WindowSource` / `correlate.IncidentSink` — let the evaluator tests
  drive the whole cycle over fakes with a fixed clock, no database.
- `incidentapi.IncidentStore` / `incidentapi.EventStore` / `incidentapi.Signaler`
  — let the handler tests map store outcomes to status codes, and assert that
  transitions signal the alert workflow, without a database or a Temporal server.
- `alerting.IncidentSource` / `alerting.WorkflowStarter` — let the starter poller
  be tested against fakes; `alerting.IncidentStatusStore` /
  `alerting.NotificationRecorder` let the workflow run its real activities over
  in-memory fakes in Temporal's test environment.

Everything else is a concrete type passed explicitly. There is no dependency
injection container, no service locator, and no global mutable state. The one
concession is OpenTelemetry's process-wide globals, which are set because
third-party instrumentation falls back to them — but this project's own code
always receives its providers as explicit parameters.

---

## 12. Correlation and the incident lifecycle

Ingestion stops at stored events. Correlation is the first *reaction*: an engine
that watches the stored stream, opens an **incident** when a
service is unhealthy, groups repeat detections into that one incident, and tracks
it through a lifecycle until it is resolved. A separate read API exposes the
result. The full rationale — including the alternatives that were rejected — is
in [ADR 0002](adr/0002-correlation-and-incident-lifecycle.md); this section is the
mechanics.

### 12.1 Where correlation runs

The evaluator runs **in-process in the incident engine**, as a third goroutine in
the same errgroup as the consume loop and the probe server, over the same
connection pool and the same shutdown lifetime. It is not per-event and it is not
a separate service.

- **Not per-event**, because the signal is a windowed aggregate. "Half of this
  service's requests failed in the last minute" cannot be decided one event at a
  time; it is a property of a window.
- **Database-backed, holding no state between ticks.** Each cycle asks PostgreSQL
  for the tallies it needs and forgets them. That is what makes the engine safe to
  restart and safe to run as multiple replicas: the source of truth is the shared
  table, not memory, so two evaluators converge on the same incident instead of
  racing to create two.

Unlike the consume loop, a failed correlation cycle does **not** stop the engine.
Ingestion is the job that must not fail; correlation is best-effort over data that
is already durable. A bad cycle is logged and retried on the next tick.

### 12.2 The evaluation cycle

Every `CORRELATION_INTERVAL` (default 15s):

```
for each distinct rule window:
    windows := SELECT tenant, service,
                      count(*)                                    AS total,
                      count(*) FILTER (WHERE severity IN (error,critical)) AS errors
               FROM telemetry_events
               WHERE event_timestamp >= now - window
               GROUP BY tenant, service
    for each (service window, rule):
        if rule fires:  UpsertOpen(incident)   -- opens or groups
AutoResolveStale(now - CORRELATION_RESOLVE_AFTER)   -- close the quiet ones
```

Rules are grouped by window so each distinct lookback is queried once. The
`error_rate` rule fires when `errors / total ≥ threshold`, but only once `total ≥
min-events`: without that floor, a single failed request in an otherwise idle
service would report a 100% error rate over a sample of one.

Auto-resolution runs **after**, and only if the read succeeded. Aborting the cycle
on a failed read is deliberate: absent data would otherwise look exactly like
"every service went quiet", and the sweep would wrongly close every open incident.

### 12.3 Fingerprint dedup — one active incident per condition

An incident's identity is a deterministic **fingerprint**: `rule : tenant :
service` (joined with a control-character separator so no combination of values
can collide). The invariant "at most one *active* incident per fingerprint" is
enforced by the database, not the application:

```sql
CREATE UNIQUE INDEX incidents_active_fingerprint_idx
    ON incidents (fingerprint)
    WHERE status <> 'resolved';
```

`UpsertOpen` then opens-or-groups atomically in one statement:

```sql
INSERT INTO incidents (...) VALUES (...)
ON CONFLICT (fingerprint) WHERE status <> 'resolved'
DO UPDATE SET last_seen_at = GREATEST(...),
              event_count  = incidents.event_count + EXCLUDED.event_count,
              updated_at   = now()
RETURNING (xmax = 0) AS inserted
```

Two details carry the design:

- **The partial index is the conflict arbiter.** Because it excludes resolved
  rows, the UPSERT collides only with a still-active incident. A resolved incident
  is invisible to it, so once an incident is closed a recurrence of the same
  condition opens a genuinely new row — one incident per episode, not one forever.
- **`RETURNING (xmax = 0)`** reports whether this call *opened* (a fresh insert,
  where the row's `xmax` system column is zero) or *grouped* (an update, where it
  is not). That single boolean is what lets the engine log an open distinctly from
  a group and count them separately, without a second query.

Severity and title are not updated on conflict, and they cannot drift: the rule id
is part of the fingerprint, so every detection that groups into an incident came
from the same rule and carries the same severity.

### 12.4 The lifecycle

```
        (rule fires)
             │
             ▼
        ┌────────┐   acknowledge   ┌──────────────┐   resolve   ┌──────────┐
        │  open  │ ──────────────▶ │ acknowledged │ ──────────▶ │ resolved │
        └────────┘                 └──────────────┘             └──────────┘
             │                                                       ▲
             └───────────────────── resolve ─────────────────────────┘
```

The lifecycle is **one-directional** — an incident is never reopened. This is a
deliberate honesty choice: reopening would fuse two separate outages into one
record with a misleading duration. A recurrence opens a new incident instead, so
"how many times did this break this week?" has a truthful answer.

Transitions are guarded in SQL (`UPDATE ... WHERE id = $1 AND status = 'open'`),
so an illegal transition matches no row. The store distinguishes that from a
missing incident with one cheap status lookup, letting the API answer `409
Conflict` for an illegal transition and `404` for a genuinely absent incident.

**Auto-resolution** closes any active incident whose most recent detection is
older than `CORRELATION_RESOLVE_AFTER`. It is decoupled from detection: an
incident resolves because it went quiet, not because a rule stopped firing, which
keeps the "is it still happening?" question answered by the freshness of the data
rather than by rule bookkeeping.

### 12.5 The read side

`incidents-api` (port 8084) is a separate, read-mostly service — deliberately not
bolted onto the write-only ingestion API. It serves incident queries, the
acknowledge/resolve transitions, and a filtered read API over stored events (the
query path the write-only front door lacks entirely). It shares the database with the engine
but never touches Kafka. Query parameters outside the closed vocabularies (an
unknown status or severity, a non-RFC3339 time bound, a non-UUID id) are rejected
as `400`s rather than silently returning nothing or failing deep in the database.

### 12.6 What correlation still does not do

| Not done | Consequence | Where it goes |
|---|---|---|
| **Per-service thresholds** | One global error-rate rule; a service with a high baseline has no override. | A rules table (ADR 0002, alt. E). |
| **Suppression / snooze** | A permanently broken service re-opens an incident every cycle after each resolve. | Correct but noisy; needs a snooze model. |
| **Auth on the read API** | `incidents-api` is open on the private network. | Before any external exposure. |

Paging and remediation are covered in §13 and §14. The window scan had no index
that could serve it until migration 0005 added one — see §8.

---

## 13. Alerting and escalation

Correlation detects incidents; alerting tells someone. When an incident opens,
a **Temporal workflow** pages the on-call responder, waits for an
acknowledgement, and escalates to the next responder if none arrives. The
rationale and rejected alternatives are in
[ADR 0003](adr/0003-temporal-for-alert-escalation.md); this section is the
mechanics.

### 13.1 Why a workflow engine

Escalation is a long-lived, timer-driven, human-in-the-loop process: page, wait
minutes, page again, and stop the instant someone takes ownership. It must
survive restarts (an escalation lost to a redeploy is worse than none, because it
is silently trusted) and it must page each level exactly once. Durable timers,
replay-safe state and signal-based rendezvous are precisely what Temporal
provides, so the design spends a dependency here rather than re-deriving a
scheduler.

The trade is real and deliberate: Temporal is the heaviest thing this project
runs. It is worth it here and nowhere else so far — correlation
deliberately uses a plain database poller, because a windowed aggregate is not a
durable process.

### 13.2 The pieces

```
incident-engine ──▶ incidents ──▶ alerting (poller) ──▶ Temporal ──▶ alerting (worker)
   (opens)          (alerted_at)     starts workflow                    │
                                                                        ▼
incidents-api ──── acknowledge/resolve ──── signal ─────────────▶  notifications
                        (DB first)                                  (audit trail)
```

- **`alerting`** is a new service running two things in one errgroup: a Temporal
  **worker** (executing the workflow and its activities) and a **starter poller**.
- The **starter** ticks over `SELECT ... WHERE status = 'open' AND alerted_at IS
  NULL`, starts one workflow per incident, then stamps `alerted_at`. The incident
  engine is untouched and never learns Temporal exists.
- The **incidents-api** signals a running workflow when an incident is
  acknowledged or resolved.

### 13.3 Exactly one alert per incident

The Temporal workflow id *is* the incident id (`incident-alert-<uuid>`), started
with a `REJECT_DUPLICATE` reuse policy. Starting a workflow that already exists —
running or completed — returns an "already started" error, which the starter
treats as success. So:

- the poller is idempotent and restart-safe (a missed `alerted_at` stamp costs one
  harmlessly-rejected start, not a second page);
- "one alert workflow per incident" is a **server-side guarantee**, not
  application locking;
- a recurrence is a *new* incident with a new id (the partial unique index in §12), so
  it correctly gets its own workflow.

### 13.4 The escalation loop

For each level of the policy the workflow:

1. **re-checks incident status** via an activity — if it is already acknowledged
   or resolved, it stops without paging;
2. resolves **who is on call** for that level at the current workflow time;
3. **sends the notification** (an activity: record the audit row, optionally POST
   a webhook);
4. **awaits** an `acknowledge`/`resolve` signal *or* the level's timeout.

A signal stops the loop; a timeout escalates to the next level. When the levels
run out with no acknowledgement, a final "escalation exhausted" notification is
recorded.

**Signals are the fast path; the database is the authority.** The incidents-api
commits the transition first and then signals, so escalation stops in
milliseconds — but step 1 means a lost or failed signal still converges to the
right outcome at the next level boundary. That is why a signal failure is logged
rather than failing the API request.

### 13.5 Paging exactly once, under retries

Temporal retries failed activities, and replays workflows. Either could duplicate
a page. Two things prevent it:

- The workflow computes a **deterministic notification id** — a UUIDv5 of
  `incident:level:reason`. It is pure, so it is replay-safe, and it makes the
  record idempotent through `ON CONFLICT (id) DO NOTHING`.
- The record activity **fails only on a database error**. A webhook failure is
  captured in the row's `status`/`detail` and returns success, so a flaky endpoint
  neither stalls escalation nor triggers a retry that would page again.

### 13.6 On-call schedules

A policy is an ordered list of levels, each with a target label, an ack timeout
and a **rotation**. A rotation resolves the responder at an instant by cycling its
contacts on a fixed shift: `((now - start) / shift) % len(contacts)`. It is pure
arithmetic — deterministic, so it is safe to call inside a workflow, and trivially
testable. Policies are JSON with a default embedded in the binary, overridable
with `ESCALATION_POLICY_PATH`.

### 13.7 What alerting does not do

| Not done | Consequence | Where it goes |
|---|---|---|
| **Real Slack / email / PagerDuty delivery** | "Paged" means recorded, logged, and webhooked if configured. | The webhook sink is the seam; a real provider is ADR 0003 alternative C. |
| **Quiet hours, snooze, alert grouping** | Every open incident pages. | Needs a suppression model, alongside the snooze gap in §12.6. |
| **Calendar-based on-call** | Only the fixed-shift rotation exists. | A schedule service, or delegation to a paging provider. |
| **Automated remediation** | A human still fixes it. | Covered in §14. |
| **Auth on the alerting probe surface** | As with every other service, private network only. | Before external exposure. |

---

## 14. Remediation and approval gates

Alerting pages a human. Remediation asks what the platform may do *itself* — the
highest-risk capability here, since everything else only observes. The
design is therefore organised around refusal rather than action. Full rationale in
[ADR 0004](adr/0004-runbook-remediation-with-approval-gates.md).

### 14.1 Runbooks are data, and gating is per step

A runbook has a matcher (rule id required; service and severity optional) and an
ordered list of steps. Each step declares its own **mode**:

| Mode | Meaning |
|---|---|
| `auto` | Runs as soon as the step is reached. For work that is always safe — collecting diagnostics. |
| `approval` | The run stops and waits for a human to approve or reject. For anything that touches production. |

Per-step rather than per-runbook is the point: gating the harmless diagnostic
behind a human is slow and trains people to approve reflexively, while letting a
restart through unattended is dangerous. A unit test asserts the shipped catalog
never performs a webhook action unattended.

### 14.2 The run, and the four ways it stops

```
for each step:
    re-check the incident is still open ──── resolved? ──▶ SKIPPED, halt
    if approval-gated:
        record PENDING, wait for a decision
            rejected  ──▶ REJECTED,  halt
            timed out ──▶ TIMED_OUT, halt
            approved  ──▶ record APPROVED, continue
    execute the action
            failed    ──▶ FAILED,    halt
            ok        ──▶ SUCCEEDED, next step
```

Every one of those halts is recorded, not just the happy path. The run never
proceeds to step *n+1* after step *n* did not cleanly succeed: continuing to
automate after something already went wrong is how a small incident becomes a
large one. And because the incident is re-checked before each step, somebody
resolving it stands the automation down mid-runbook.

### 14.3 Blast radius is bounded by construction

Only two action kinds ship:

- **`noop`** — records intent, touches nothing.
- **`webhook`** — POSTs the step's context to a configured URL.

Restarting deployments, draining nodes and rolling back releases live *behind*
that webhook, in a system that already holds those credentials. An incident
platform that can restart your fleet itself is a far larger security proposition
than one that asks something else to — and it would mean a bug in a correlation
rule could reach cluster credentials. The indirection keeps the permission
boundary where it belongs.

### 14.4 Exactly-once actions and the audit trail

The same properties as alerting, for the same reasons: the workflow id is the
incident id (so an incident is never remediated twice), and each step's audit row
id is a deterministic UUIDv5 of `incident:step`. A `UNIQUE (incident_id,
step_index)` constraint makes a step exactly one row whose status advances
`pending → approved → succeeded`, so an activity retry or a workflow replay
updates rather than duplicating — no step is ever executed twice by a retry.

`remediation_actions` records every step including the refused ones, which is what
lets both post-incident questions be answered from data: "why did it restart
production at 3am?" and "why didn't it?".

### 14.5 Approving from the API

`POST /v1/incidents/{id}/remediation/approve` (or `/reject`) looks up the step
awaiting a decision, then signals the workflow. It answers **202 Accepted**, not
200: the workflow applies the decision asynchronously, so claiming the step is
already done would be a lie — the outcome appears in the audit trail. Deciding
when nothing is pending is a **409**, because the incident exists but there is
nothing to decide on.

### 14.6 What remediation does not do

| Not done | Consequence | Where it goes |
|---|---|---|
| **Fully automatic remediation** | Time-to-recovery only improves by whatever the `auto` steps achieve. | Steps graduate to `auto` — a config change — once detection is mature enough to trust. |
| **Built-in Kubernetes/cloud actions** | The demo's automation is mostly ceremonial until a real endpoint sits behind the webhook. | A small, tightly-scoped service behind that URL. |
| **Rollback / compensating actions** | A failed run halts; it does not undo. | A genuine workflow redesign, not a new step kind. |
| **Parallel steps** | Steps run strictly in order. | Plausible later; unnecessary to demonstrate gating. |
| **Per-team runbook ownership or review** | The catalog is global config changed by deploy. | A runbook store and management API. |

---

## 15. Interface and platform

Everything above is a platform with no face: what it knows is reachable only by
`curl` or `psql`, its telemetry goes to stdout, and it runs in exactly one place.
This section covers the dashboard, a real telemetry destination, and Kubernetes
manifests. Rationale and rejected alternatives in
[ADR 0005](adr/0005-interface-and-platform.md).

### 15.1 The dashboard

A React + TypeScript SPA over the incidents API: a live incident list, and a
detail pane with the alert timeline, the remediation trail, and the buttons to
acknowledge, resolve, approve and reject.

Two choices define it:

- **React and React DOM are the only runtime dependencies.** No component
  library, state manager, router or data-fetching library. The app is two panes
  and four components; the polling hook, the typed API client and the stylesheet
  are a few dozen lines each. Anything more would weigh more than the application.
- **Same-origin by design.** nginx (production) and Vite (development) both proxy
  `/v1` to the incidents API. The browser never makes a cross-origin request, so
  **the Go API carries no CORS middleware at all** — a whole class of
  configuration, and its footguns, simply does not exist.

Liveness is **polling**, every five seconds. The backend has no push channel, so
a websocket would be a fiction over the same requests; `usePolling` is the seam
where server-sent events would land if sub-second freshness were ever needed.
Only the first load shows a spinner — later polls refresh in place, so the view
never flickers back to "loading" while somebody is reading it.

There are deliberately **no frontend tests**; CI enforces `tsc --noEmit` in strict
mode instead, and the image build runs it too. This is the largest asymmetry in
the repository and it is a considered trade: the invariants worth testing in this
system live in Go and are tested exhaustively there, while the dashboard's logic
is formatting and conditional rendering.

### 15.2 Telemetry finally has somewhere to go

The OpenTelemetry Collector runs behind an optional Compose profile. Every
service has spoken OTLP from the start, so switching from stdout to the
collector is two environment variables and **no application change** — the payoff
for having built against a vendor-neutral protocol rather than a vendor SDK.

The collector config carries a `memory_limiter` deliberately: a telemetry burst
during an incident is exactly when the collector must not be OOM-killed, which is
also exactly when the burst arrives.

### 15.3 Running on a cluster

`deploy/k8s/` is plain YAML — no Helm, no Kustomize. Eight workloads and a
handful of knobs do not justify a templating toolchain; the moment for overlays
is when a second environment appears and the manifests diverge.

The manifests are a faithful translation of the Compose topology, weaknesses
included and named: single-broker Kafka, single-replica PostgreSQL, and
development credentials in a committed Secret that says so loudly. A manifest set
that quietly looked production-ready would be worse than one that is honest.

Two details encode real constraints rather than decoration:

| Choice | Why |
|---|---|
| The incident engine's HPA maxes at **3** | The topic has 3 partitions, and a consumer group can never have more *working* members than partitions. Scaling past it buys idle standbys, not throughput — raising the ceiling means raising the partition count first. |
| Liveness probes hit `/health`, never `/ready` | Restarting a healthy process because Kafka is unreachable turns a dependency outage into an outage of its own. Readiness removes it from the Service; liveness kills it. |

`remediation` is a separate workload partly so it can be **scaled to zero** to
disable automation entirely, without touching anything else: incidents still
open, page and escalate as normal.

### 15.4 Multi-region Kafka — the analysis, not a prop

Multi-region Kafka was on the roadmap. **It is deliberately not implemented**,
because it cannot be demonstrated honestly on one machine, and a Compose file
with two single-broker clusters labelled `us-east` and `eu-west` would be
theatre. What the work actually involves:

- **Replication.** MirrorMaker 2 or Cluster Linking mirrors `telemetry.events.v1`
  between regions. MM2 renames topics by default (`us-east.telemetry.events.v1`),
  which every consumer's subscription must account for.
- **Offset translation.** Offsets are per-cluster and do not transfer. MM2 emits
  a checkpoint stream so a consumer group failing over can resume at the
  translated offset — approximately. Any failover therefore reprocesses some
  events, which this system already tolerates: the `event_id` primary key makes
  reprocessing a no-op, and the incident fingerprint makes duplicate detection
  idempotent. **The idempotency built into ingestion is what would make a
  multi-region failover survivable**, and that is the genuinely interesting
  observation.
- **Active/active or active/passive.** Two incident engines consuming mirrored
  copies of the same events would both open incidents; the partial unique index
  on fingerprint prevents duplicates *within* one database, not across two. So
  either the databases are one logical store (adding cross-region write latency
  to the ingest path) or exactly one region correlates at a time.
- **What actually needs to be regional.** Ingestion should be regional, because
  losing telemetry during a regional outage is the failure this system exists to
  prevent. Correlation and alerting arguably should not be: they are cheap, and
  running them in one place sidesteps the split-brain question entirely.

The conclusion is that "multi-region Kafka" is not one feature but a set of
coupled decisions about where state lives, and the honest deliverable at this
stage is that analysis rather than a demo that implies more than it does.

### 15.5 What the interface and platform work does not do

| Not done | Consequence | Where it goes |
|---|---|---|
| **Frontend tests** | Rendering regressions are caught by a human, not CI. | A component test setup, once the UI grows past a few screens. |
| **Manifests validated against a live API server** | They parse and are faithful, but no job applies them. | A `kind` cluster in CI. |
| **Auth anywhere** | The dashboard approves as `actor=dashboard`. | Auth arrives across the whole stack at once, or not at all. |
| **Charts over the event stream** | `GET /v1/events` supports it; nothing visualises it. | Future dashboard work. |
| **Multi-region anything** | Analysis only — see §15.4. | A project of its own, if this ever needs it. |
