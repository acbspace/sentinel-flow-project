# SentinelFlow

An event-driven incident detection and remediation platform, in the spirit of
Datadog, PagerDuty and the internal reliability platforms large engineering
organisations build for themselves.

## Overview

SentinelFlow turns a stream of telemetry into handled incidents, end to end:

1. **Ingest** — instrumented services emit telemetry; an API validates it and
   publishes to Kafka; an engine consumes and stores it, losslessly.
2. **Detect** — a windowed engine watches error rates and opens **incidents**,
   deduplicated by a database invariant and tracked through a lifecycle.
3. **Alert** — a Temporal workflow pages the on-call responder and escalates
   through the policy until someone acknowledges, auditing every page.
4. **Remediate** — a runbook engine runs safe steps unattended and **stops to
   ask a human** before anything touches production, halting on any refusal.
5. **Observe** — a React dashboard, an OpenTelemetry Collector, and Kubernetes
   manifests.

It is built as nine Go microservices plus a React dashboard, and is portfolio
quality throughout: everything compiles, nothing is stubbed, no error is
swallowed, and every design decision is recorded in an ADR. What is deliberately
*out of scope* is [named, not implied](#whats-deliberately-not-here).

## Repository Structure

```
.
├── cmd                     # service entry points (wiring only)
│   ├── ingestion-api       #   HTTP front door → Kafka
│   ├── incident-engine     #   Kafka consumer + correlation loop
│   ├── incidents-api       #   read / lifecycle / approval API
│   ├── alerting            #   Temporal worker + alert starter
│   ├── remediation         #   Temporal worker + runbook starter
│   ├── janitor             #   partition maintenance and retention
│   ├── order-service       #   demo producer
│   ├── payment-service     #   demo producer
│   └── migrate             #   schema migration runner
├── internal
│   ├── event               # the versioned telemetry contract
│   ├── ingest  · engine    # HTTP handler; consume + persist loop
│   ├── incident · correlate # incident domain; windowed evaluator
│   ├── oncall   · alerting  # escalation policy; alert workflow
│   ├── runbook  · remediate # runbook catalog; gated remediation flow
│   ├── incidentapi         # read / lifecycle / approval HTTP handler
│   ├── kafkax  · store     # franz-go client; pgx pool + queries
│   ├── obs · httpx · config # telemetry; server plumbing; env parsing
│   ├── janitor · bench     # partition maintenance; benchmark harness
│   └── demo · migrate      # traffic simulator; migration runner
├── web                     # React + TypeScript dashboard
├── deploy/k8s              # Kubernetes manifests: workloads, probes, HPAs
├── build                   # Dockerfiles, nginx config, OTel Collector config
├── migrations              # embedded SQL
├── test/integration        # end-to-end tests (build tag: integration)
└── docs                    # architecture notes and ADRs
```

## Technology Stack

| Component | Technologies |
|---|---|
| Ingestion & services | Go, chi, franz-go (Kafka), pgx (PostgreSQL) |
| Alerting & remediation | Temporal (durable workflows) |
| Dashboard | React, TypeScript, Vite, nginx |
| Telemetry | OpenTelemetry (OTLP), `log/slog` |
| Storage & transport | PostgreSQL, Apache Kafka |
| Infrastructure | Docker Compose, Kubernetes |

## Architecture

```
INGEST
  demo services ──▶ ingestion-api ──▶ Kafka ──▶ incident-engine ──▶ PostgreSQL
                    validate,          durable   consume, persist,   events +
                    publish (202)      log       correlate (ticker)  incidents

REACT   incident-engine opens an incident; two Temporal workers act on it
  alerting     ──▶ page on-call, escalate through the policy, audit  ──▶ notifications
  remediation  ──▶ run the runbook, stop at an approval gate         ──▶ actions

SERVE
  incidents-api ──▶ dashboard + on-call
                    list · acknowledge / resolve · approve / reject a step
                    (each action also signals the alerting / remediation workflow)
```

Four ideas carry the design. Each is explained in depth in
[`docs/architecture.md`](docs/architecture.md) and justified in an ADR.

- **Lossless ingestion.** The producer waits for a synchronous `acks=all` Kafka
  write before returning `202`, and the engine commits offsets only after the
  row is durable. Redelivery is harmless because the insert is idempotent on a
  producer-supplied `event_id`. — [ADR 0001](docs/adr/0001-event-driven-architecture.md)
- **Deduplicated incidents.** Correlation is a windowed database poller, and
  "one active incident per `(rule, tenant, service)`" is a **partial unique
  index**, not application logic that could race. Recurrences open a fresh
  incident, so each is one contiguous episode. — [ADR 0002](docs/adr/0002-correlation-and-incident-lifecycle.md)
- **Durable escalation.** Alerting is a **Temporal workflow**, one per incident
  (the workflow id *is* the incident id), so escalation survives restarts and
  pages each level exactly once. Acknowledgement is a signal; the database stays
  the authority. — [ADR 0003](docs/adr/0003-temporal-for-alert-escalation.md)
- **Remediation designed around refusal.** A runbook's steps are `auto` or
  `approval`; a rejection, timeout, failed step, or resolved incident all **halt
  the run**, and every outcome is audited. Actions are indirect (a webhook), so
  the platform holds no cluster credentials. — [ADR 0004](docs/adr/0004-runbook-remediation-with-approval-gates.md) · [ADR 0005](docs/adr/0005-interface-and-platform.md)

The incident lifecycle is one-directional — an incident is acknowledged, then
resolved, but never reopened:

```
open ──(acknowledge)──▶ acknowledged ──(resolve)──▶ resolved
  └──────────────(resolve / auto-resolve)────────────┘
```

Repeat detections group into the active incident (one per fingerprint), so a
spike is one incident with a rising count rather than a flood; a recurrence after
resolution opens a fresh one.

## Benchmarks

Measured from the host against the local Docker stack (single instance of each
service, single-broker Kafka at RF=1), driven by a small concurrent Go load
generator at 50 workers. These show the *shape* of each path's cost, not a
production SLA.

| Path | Work per request | req/s | avg | p50 | p95 | p99 |
|---|---|---:|---:|---:|---:|---:|
| `GET /health` | HTTP + routing only | 20,155 | 2.4 ms | 2.2 ms | 4.9 ms | 6.7 ms |
| `GET /v1/incidents` | + PostgreSQL query | 13,815 | 3.6 ms | 3.3 ms | 6.0 ms | 7.9 ms |
| `POST /v1/events` | + validation + **synchronous Kafka `acks=all`** | 8,186 | 6.1 ms | 5.8 ms | 9.4 ms | 11.5 ms |

The ingest path sustains **~8,200 durable writes/second at p99 11.5 ms** while
producing to Kafka synchronously with `acks=all` per request — that is a real
Kafka round-trip on the hot path, not a fire-and-forget buffer. The read path
adds a PostgreSQL query for ~1.5 ms over raw HTTP.

## Quick start

**Requirements:** Docker with Compose v2, and Go 1.24+ for the local test targets.

```bash
cp .env.example .env    # make up does this for you if you forget
make up                 # build images, start everything, wait for readiness
make demo               # drive traffic through the pipeline
make burst              # force an error spike, so an incident opens

make incidents                       # the open incidents
make alerts       INCIDENT=<id>      # its page / escalation timeline
make remediation  INCIDENT=<id>      # what automation did, and what it waits on
make approve      INCIDENT=<id>      # release the gated remediation step
make verify                          # events, incidents, notifications, actions
```

Then open the **dashboard at <http://localhost:3000>** — the incident list, the
escalation timeline, the remediation trail, and the approve/reject buttons are
all there. To watch the workflows run, start the Temporal Web UI with
`docker compose --profile ui up -d temporal-ui` and open <http://localhost:8233>.

> **Port conflicts.** Every host port is configurable in `.env` (gitignored). If
> 5432 or 8080 are taken, change `POSTGRES_PORT` / `INGESTION_API_PORT` — the
> `make` inspection targets read the same variables.

### Services

| Service | Port | Purpose |
|---|---|---|
| ingestion-api | 8080 | `POST /v1/events` — validate and publish |
| incident-engine | 8081 | Kafka consumer + correlation loop |
| order / payment-service | 8082 / 8083 | demo producers |
| incidents-api | 8084 | read / lifecycle / approval API |
| alerting | 8085 | Temporal worker + alert starter |
| remediation | 8086 | runbook worker + remediation starter |
| janitor | 8087 | partition maintenance and retention |
| dashboard | 3000 | React UI (nginx, proxies `/v1`) |
| PostgreSQL · Kafka · Temporal | 5432 · 29092 · 7233 | storage · log · workflows |
| OTel Collector | 4318 | optional `otel` profile (Prometheus on 8889) |

Run `make help` for every target.

## API

The ingestion API takes one event; the incidents API serves everything else.
Full request/response detail is in [`docs/`](docs/architecture.md); the surface
is:

**`POST /v1/events`** (ingestion-api) — validate and durably publish one event.

```bash
curl -X POST http://localhost:8080/v1/events -H 'Content-Type: application/json' -d '{
  "event_id": "0921316d-4496-4568-8638-2b0ef226f850", "schema_version": "1.0",
  "tenant_id": "demo-tenant", "service_name": "payment-service",
  "event_type": "request.completed", "severity": "error",
  "timestamp": "2026-07-23T11:30:00Z", "attributes": {"http_status_code": 500}
}'
```

`202` once durable · `400` lists **every** validation problem at once · `413` too
large · `415` non-JSON · `503` Kafka unavailable (retry).

**incidents-api** (port 8084) — read-mostly; every list is paginated and every
bad value (unknown status/severity, non-UUID id, non-RFC3339 time) is a `400`.

Lists page by cursor: a response carries `next_cursor` until the last page, and
you pass it back as `?cursor=`. `?offset=` still works but degrades on a large
table, because PostgreSQL walks and discards every skipped row. Combining the two
is a `400`, since a cursor already encodes a position.

| Endpoint | Purpose |
|---|---|
| `GET /v1/incidents` | list; filter by `status` / `tenant_id` / `service` / `severity` |
| `GET /v1/incidents/{id}` | one incident (`404` if absent) |
| `GET /v1/incidents/{id}/notifications` | the alert / escalation timeline |
| `GET /v1/incidents/{id}/remediation` | the runbook action audit trail |
| `POST /v1/incidents/{id}/acknowledge` · `/resolve` | drive the lifecycle (`409` on an illegal transition) |
| `POST /v1/incidents/{id}/remediation/approve` · `/reject` | decide a gated step — `202` (applied async), `409` if nothing pending |
| `GET /v1/events` | read stored telemetry; filter by service / severity / trace / time |

## Reliability

**Delivery is at-least-once, made safe by idempotent storage.** Guaranteed:

- A `202` means the event is durably in Kafka (`acks=all`, synchronous).
- The same `(event_id, event_timestamp)` yields exactly one row — the database is
  the single deduplication authority, under any number of engine replicas. The
  pair, rather than `event_id` alone, because `telemetry_events` is
  range-partitioned by day and PostgreSQL requires the partition key in every
  unique constraint. Redeliveries are still collapsed: a replayed Kafka record is
  byte-identical, so it carries the same timestamp and conflicts. What is no
  longer rejected is one `event_id` submitted under two *different* timestamps,
  which was never a redelivery. See
  [migration 0006](migrations/0006_partition_telemetry_events.up.sql).
- At most one **active incident** per fingerprint (partial unique index), and at
  most one **alert / remediation workflow** per incident (Temporal workflow id =
  incident id). Each escalation level and each runbook step happens exactly once,
  across retries, replays and restarts.
- Shutdown is graceful everywhere: servers drain, the consumer leaves its group
  cleanly, in-flight offset commits complete.

Not guaranteed: end-to-end exactly-once (duplicates occur and are collapsed at
the DB), read-your-writes (`202` is durable, not yet queryable), or global
ordering (ordering holds within a partition = one tenant's one service). Every
failure is retried, reported loudly, or **visibly** dropped with a documented
reason — nothing is swallowed. See
[`docs/architecture.md` §7](docs/architecture.md) for the full failure matrix.

## Testing

```bash
make test              # unit tests, race detector enabled
make test-integration  # end-to-end, requires make up
```

Unit tests are deterministic — randomness, clocks and id generation are
injectable throughout, and Temporal workflows are tested with the SDK's
`testsuite` under virtual time (no server). Coverage highlights:

| Area | What is pinned |
|---|---|
| Event contract & handlers | every field rule; `202`/`400`/`413`/`415`/`503`; strict decoding |
| Idempotency | redelivery, cross-partition redelivery, `ON CONFLICT` |
| Correlation | threshold/min-events boundaries; open / group-on-repeat / auto-resolve; **no auto-resolve on a failed read** |
| Incident lifecycle | transitions, fingerprint collision-resistance, `409` on illegal transitions |
| Alert workflow | escalate-through-every-level, stop-on-ack, stop-on-resolve, page-nobody-when-closed |
| Remediation | auto-then-approved runs both; **rejection / timeout / resolved-incident each halt**; the shipped runbook never acts unattended |
| Integration | HTTP→Kafka→engine→PostgreSQL; burst→correlate→open→group→resolve→reopen; and, against live Temporal, incident→page→escalate→ack-stops, and a gated runbook that waits then proceeds or halts |
| Dashboard | strict `tsc --noEmit` + production build in CI (no component tests — a [considered trade](docs/adr/0005-interface-and-platform.md)) |

## Configuration

Every service is configured by environment variable; [`.env.example`](.env.example)
has the full set with defaults. No credentials are hardcoded. The knobs worth
knowing:

| Variable | Default | Applies to |
|---|---|---|
| `POSTGRES_DSN` | *(required)* | engine, incidents-api, alerting, remediation, migrate |
| `KAFKA_BROKERS` · `KAFKA_TOPIC` | `localhost:9092` · `telemetry.events.v1` | api, engine |
| `TEMPORAL_ADDRESS` | `localhost:7233` | alerting, remediation (required); incidents-api (optional) |
| `CORRELATION_INTERVAL` · `_WINDOW` | `15s` · `60s` | engine |
| `CORRELATION_ERROR_RATE_THRESHOLD` · `_MIN_EVENTS` | `0.5` · `5` | engine |
| `ALERT_POLL_INTERVAL` · `ALERT_WEBHOOK_URL` | `10s` · *(log only)* | alerting |
| `ESCALATION_POLICY_PATH` · `RUNBOOK_CATALOG_PATH` | *(embedded defaults)* | alerting · remediation |
| `OTEL_TRACES_EXPORTER` · `OTEL_METRICS_EXPORTER` | `none` (→ `stdout` / `otlp`) | all |

Telemetry is vendor-neutral: `none` still creates spans and propagates trace
context (across HTTP *and* Kafka headers) — it only disables *export*. Switching
to the collector is two variables and no code change.

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — the deep dive: partitioning,
  offset strategy, the failure matrix, indexing, the correlation cycle,
  escalation, the runbook engine, the cluster topology, and a multi-region
  analysis.
- ADRs — every significant decision and its rejected alternatives:
  [0001 event-driven](docs/adr/0001-event-driven-architecture.md) ·
  [0002 correlation](docs/adr/0002-correlation-and-incident-lifecycle.md) ·
  [0003 Temporal](docs/adr/0003-temporal-for-alert-escalation.md) ·
  [0004 remediation](docs/adr/0004-runbook-remediation-with-approval-gates.md) ·
  [0005 interface & platform](docs/adr/0005-interface-and-platform.md)
- [`deploy/k8s/README.md`](deploy/k8s/README.md) · [`web/README.md`](web/README.md)
  — the manifests' and the dashboard's own choices and limitations.

## What's deliberately not here

Named rather than implied — each is a considered trade, most with the seam to
close it already in place:

- **No authentication.** Everything sits on a private network; `tenant_id` is a
  partition key, not a security boundary.
- **No real paging or remediation providers.** Slack/email/PagerDuty and cluster
  actions live behind webhooks, in systems that already hold those credentials.
- **Conservative automation.** Anything touching production is approval-gated;
  steps graduate to `auto` by config once detection is trusted. No rollback.
- **One global correlation rule; no alert suppression; fixed-shift on-call.**
- **No dead-letter topic.** A permanently failing *valid* message stalls its
  partition until an operator intervenes — the most significant ingestion gap.
- **Single-node Kafka (RF=1)** and JSON on the wire. PostgreSQL retention is
  handled (daily partitions, dropped by the janitor); Kafka retention is not.
- **No frontend tests**, and no multi-region deployment — the latter is
  [analysis](docs/architecture.md), not a demo that would imply more than it does.

## License

Not yet licensed. All rights reserved.
