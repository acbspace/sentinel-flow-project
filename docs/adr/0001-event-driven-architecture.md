# ADR 0001: Use an event-driven architecture for telemetry ingestion

- **Status:** Accepted
- **Date:** 2026-07-23
- **Deciders:** SentinelFlow engineering
- **Supersedes:** none

## Context

SentinelFlow ingests telemetry from instrumented services and will eventually
detect incidents from it, notify responders, and run automated remediation.

The forces shaping the design:

1. **Ingest volume is bursty and outside our control.** Telemetry arrives fastest
   at exactly the moment the platform is least able to absorb it: during an
   incident. A design that is only correct at average load is not correct.

2. **Losing telemetry during an incident is the worst possible failure.** The
   events that matter most are the ones produced while things are breaking. Any
   design where back-pressure or a slow consumer drops events fails at the one
   job the system exists to do.

3. **Producers must not be coupled to consumer health.** An instrumented service
   must never slow down, fail a customer request, or block because the incident
   platform is degraded. Observability that can take down the thing it observes
   is worse than no observability.

4. **The number of consumers will grow.** Milestone 1 stores events. Later
   milestones add correlation, alert routing, remediation workflows, and
   analytics. Each is a different consumer of the same event stream, with
   different latency needs and different failure modes.

5. **Reprocessing is a routine requirement, not an exception.** Correlation rules
   will be wrong and will be fixed. Recovering from that means replaying history
   through new logic — not just for disaster recovery but as normal development.

## Decision

**SentinelFlow is built around a durable, replayable event log (Apache Kafka) as
the system of record for ingested telemetry.**

Concretely, for milestone 1:

- The ingestion API validates events and publishes them to `telemetry.events.v1`.
  It waits for the broker acknowledgement before returning `202 Accepted`, so
  that a 202 means *durable*, not merely *received*.
- The incident engine is an independent consumer group that reads the topic and
  persists normalized events to PostgreSQL.
- The two services share no database and no in-process channel. The topic is the
  only contract between them.
- Delivery is **at-least-once**, made safe by an idempotent write keyed on the
  producer-supplied `event_id`.

## Alternatives considered

### A. Synchronous HTTP writes straight to PostgreSQL

The ingestion API writes directly to the database and returns when the row is
committed.

*Rejected.* This couples ingest availability to database availability at exactly
the wrong moment: a database slowdown becomes an ingest outage, which becomes
lost telemetry during the incident that caused the slowdown. It also offers no
replay, and every future consumer would have to poll the table — reinventing a
worse log.

The simplicity is real, and for a system with one consumer and forgiving
durability needs it would be the right answer. It is not this system.

### B. A queue rather than a log (RabbitMQ, SQS)

*Rejected.* Traditional queues delete a message once it is acknowledged. That
makes force 5 — replay through new logic — impossible without a separate archive,
and force 4 awkward: each new consumer needs its own queue and fan-out
configuration, decided in advance.

A log keeps messages for a retention window independent of consumption, so a new
consumer added in six months can start from the beginning of the retention window
with no coordination from producers. For a system whose consumer set is expected
to grow, that is the deciding difference.

### C. Direct service-to-service calls, no intermediary

*Rejected outright* against force 3. It makes every producer's latency and error
budget depend on the incident platform.

### D. Batch ingestion (periodic file or table loads)

*Rejected.* Incident detection is a latency-sensitive workload. Minutes of
built-in delay defeat the purpose, and batch windows interact badly with bursts.

## Consequences

### Positive

- **Producers are decoupled from consumer health.** If the incident engine or
  PostgreSQL is down, events keep accumulating in Kafka and are processed on
  recovery. Ingest availability depends only on the API and the broker.
- **Bursts are absorbed rather than rejected.** The log is the buffer. A consumer
  that falls behind builds lag and catches up; it does not drop data.
- **New consumers are additive.** Correlation, alerting, and analytics each join
  as their own consumer group without touching the producer or existing
  consumers.
- **Replay is a first-class operation.** Fixing a correlation bug means deploying
  a new consumer group and reading the topic from the start.
- **Ordering where it matters.** Keying by `<tenant_id>:<service_name>` gives
  per-service ordering while still parallelising across partitions.
- **Failure isolation.** A crash in the engine cannot lose an accepted event,
  because the offset is only committed after the row is durable.

### Negative

- **More moving parts.** Kafka is another system to run, monitor, and understand.
  Locally that is a Compose service; in production it is a real operational
  commitment (or a managed service and its bill).
- **Eventual consistency is now visible to users.** A `202` means "durably
  queued", not "queryable". Anything reading the database must tolerate a lag
  window, and the API cannot promise read-your-writes.
- **At-least-once forces idempotency on every consumer.** Duplicates are
  guaranteed, not hypothetical. Every consumer must be designed for them; this
  one relies on the `event_id` primary key. A future consumer that performs a
  non-idempotent side effect (sending a page, triggering remediation) will need
  its own deduplication, and getting that wrong means paging someone twice.
- **Debugging spans more systems.** "Where is my event?" can be answered in the
  API, the topic, or the database. This is why trace context is propagated
  through Kafka headers rather than stopping at the HTTP boundary.
- **Operational failure modes are new and unintuitive.** Consumer lag, rebalance
  storms, partition skew, and poison messages are all now possible, and none of
  them exist in the synchronous design.

### Neutral

- Ordering guarantees are now explicitly partial rather than accidentally total.
  This is more honest, but it must be understood: there is no global ordering
  across services, and correlation logic must not assume one.
- Retention becomes a real decision with a real cost, trading storage against how
  far back a replay can reach.

## Validation

The milestone 1 acceptance criteria for this decision:

- An event accepted with `202` is durable in Kafka before the response is sent.
  *Verified by the synchronous produce path and the `503` behaviour when the
  broker is unreachable.*
- The same event submitted repeatedly results in exactly one stored row.
  *Verified by `TestPipelineIgnoresDuplicateEvents`, which submits an identical
  event three times and asserts a single row.*
- The engine can be stopped and restarted without losing accepted events.
  *Follows from committing offsets only after the database write; exercised by
  the retry and cancellation tests in `internal/engine`.*

## Revisit this decision if

- The consumer set stabilises at exactly one and replay stops being needed, in
  which case the log is carrying operational cost for optionality nobody uses.
- Ingest volume turns out to be low and steady enough that a database write path
  is comfortably within its capacity at peak, making alternative A viable.
- Ordering requirements strengthen to needing global ordering, which this
  partitioning scheme cannot provide and which would force a different design.

## References

- `docs/architecture.md` — partitioning, offset commit strategy, and the full
  failure matrix.
- Kafka documentation on consumer groups, offset management, and delivery
  semantics.
