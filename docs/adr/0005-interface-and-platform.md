# ADR 0005: Interface and platform

- **Status:** Accepted
- **Date:** 2026-07-24
- **Deciders:** SentinelFlow engineering
- **Supersedes:** none
- **Builds on:** [0002](0002-correlation-and-incident-lifecycle.md), [0003](0003-temporal-for-alert-escalation.md), [0004](0004-runbook-remediation-with-approval-gates.md)

## Context

Milestones 1–4 built a platform with no face. Everything it knows is reachable
only by `curl` or `psql`, its telemetry goes to stdout, and it runs in exactly one
place: Docker Compose on a laptop.

Milestone 5 addresses all three: a dashboard, a real telemetry destination, and
Kubernetes manifests. The forces are different from the earlier milestones —
nothing here is a distributed-systems correctness problem, and the risk is
different too. The risk is **scope**: each of these three is a plausible project
in its own right, and doing any of them badly would undo the credibility the
rest of the repository has earned.

Specifically:

1. **A dashboard attracts dependencies.** A component library, a state manager, a
   router, a data-fetching library and a CSS framework is the default modern
   stack, and it would be larger than the application it serves.
2. **The Go services are heavily tested; a sprawling untested frontend would be
   a visible inconsistency.**
3. **Kubernetes manifests are easy to over-engineer** into a templating system
   for one environment that does not exist yet.
4. **Some things in the original milestone cannot be honestly demonstrated
   locally** — multi-region Kafka in particular.

## Decision

**Build the smallest honest version of each: a two-dependency React dashboard, an
optional OpenTelemetry Collector, and plain-YAML Kubernetes manifests — and
document the piece that cannot be honestly built rather than faking it.**

### The dashboard

- **React and React DOM are the only runtime dependencies.** No UI framework, no
  state library, no router, no data-fetching library. The app is two panes and
  four components; the polling hook, typed API client and stylesheet are a few
  dozen lines each. This answers force 1 directly.
- **Polling, not websockets.** The backend exposes a REST read API and no push
  channel. A socket would be a fiction layered over the same requests, and adding
  a push channel to the API was not what this milestone was for. Five-second
  refresh is adequate for an incident list; `usePolling` is the seam where
  server-sent events would land if sub-second liveness is ever needed.
- **Same-origin by design.** nginx in production and Vite in development both
  proxy `/v1` to the incidents API, so the browser never issues a cross-origin
  request and **the Go API carries no CORS middleware at all**. A whole class of
  configuration and its associated security footguns simply does not exist.
- **Server error messages surface verbatim.** The API distinguishes 404, 409 and
  503, and each tells the user something different about what to do next.
- **No frontend tests, deliberately** (force 2). The logic is thin — formatting,
  fetch calls, conditional rendering — and a testing stack would be more setup
  than assertion. What CI *does* enforce is `tsc --noEmit` in strict mode, and the
  image build runs it too, so a type error fails the build rather than shipping.
  This is a considered trade, not an oversight: the invariants worth testing in
  this system live in Go and are tested there.

### Telemetry

- **The collector is optional, behind a Compose profile.** The services have
  always spoken OTLP; the collector is the endpoint they were built to talk to.
  Enabling it is two environment variables and no application change, which is
  the payoff for having built against a vendor-neutral protocol in milestone 1.
  It stays off by default so `make up` remains lean and `make logs` keeps showing
  application logs rather than span dumps.

### Kubernetes

- **Plain YAML. No Helm, no Kustomize** (force 3). Eight workloads and a handful
  of knobs do not justify a templating toolchain; the right moment for Kustomize
  overlays is when a second environment appears and the manifests start to
  diverge, and `deploy/k8s/README.md` says so explicitly.
- **The manifests translate the Compose topology faithfully, including its
  weaknesses**, and name them: single-broker Kafka, single-replica PostgreSQL,
  development credentials in a committed Secret. A manifest set that quietly
  looked production-ready would be worse than one that is honest about being a
  local-cluster deployment.
- **The HPAs encode real constraints, not decoration.** The incident engine's
  ceiling is the topic's partition count, because a consumer group cannot have
  more working members than partitions — scaling past it buys idle standbys.
  Ingestion scales out fast and in slowly, because bursts are the point.

### Multi-region Kafka — documented, not built

The original milestone listed multi-region Kafka. **It is not implemented, and it
should not be.** Doing it credibly means MirrorMaker 2 or Cluster Linking, a
second cluster, a story for offset translation across regions, and a decision
about whether the incident engine is active/active or active/passive. None of
that can be demonstrated on a laptop, and a `docker-compose` file with two
single-broker clusters labelled "us-east" and "eu-west" would be theatre.

The honest version is the analysis: see `docs/architecture.md` §15.4. Building a
convincing-looking fake would have been faster and would have made the repository
worse.

## Alternatives considered

### A. Next.js (or another full framework) for the dashboard

*Rejected.* Server-side rendering, file-based routing, API routes and an image
optimiser solve problems this dashboard does not have. It reads one JSON API and
renders two panes; the framework would be the largest thing in the repository.

### B. A component library (MUI, Chakra, shadcn)

*Rejected* against force 1. It would produce a more polished result faster, and
for a product with dozens of screens it would be correct. For four components it
inverts the dependency-to-value ratio the rest of this project maintains.

### C. Serve the dashboard from the Go binary with `embed`

*Tempting and genuinely close.* It would collapse the deployment to one artifact
and remove nginx entirely. Rejected because it couples the frontend's build to the
Go build (every `go build` needs a prior `npm run build`, or a checked-in bundle),
and because nginx does static serving, caching headers and gzip better than a Go
handler written for the purpose. Worth revisiting if the operational simplicity of
one binary ever outweighs that.

### D. Adding CORS to the incidents API instead of proxying

*Rejected.* Proxying achieves the same thing with zero application code and no
origin allowlist to misconfigure. CORS would be necessary if the dashboard were
hosted on a different domain; it is not.

### E. Helm chart

*Rejected for now* (force 3). A chart is the right answer when there are multiple
environments or the manifests are published for others to install. Neither is true
yet, and a chart with one `values.yaml` is a templating layer wrapped around a
single instantiation.

## Consequences

### Positive

- **The platform finally has a face**, and the approval gate from milestone 4 has
  somewhere natural to live: a button, next to the evidence for the decision.
- **The frontend cannot rot silently** — CI typechecks it in strict mode and
  builds the image.
- **Telemetry has a real destination** with no application change, validating the
  vendor-neutral choice made in milestone 1.
- **The system can run on a cluster**, with the scaling constraints written down
  where an operator will find them.
- **The dependency budget stayed honest**: two runtime dependencies for the
  dashboard, and the Go module gained nothing at all in this milestone.

### Negative

- **No frontend tests.** Deliberate, and the largest single asymmetry between the
  two halves of the repository. A regression in rendering logic would be caught by
  a human, not by CI.
- **Polling wastes requests** at idle, and is up to five seconds stale.
- **A second toolchain** (Node, npm, a lockfile) now has to be kept current, with
  its own supply-chain surface.
- **The manifests are unvalidated against a live API server** — they parse, and
  they are faithful to the Compose topology, but no CI job applies them to a
  cluster. `kind` in CI would close this.
- **Multi-region remains a document**, so the roadmap item is answered with
  analysis rather than running code.

### Neutral

- The dashboard authenticates as nobody: approvals record `actor=dashboard`. That
  is consistent with the rest of the stack having no auth, and both change
  together when auth is introduced.

## Validation

- The dashboard typechecks in strict mode and builds. *Verified by `npm run
  build` locally and the `web` CI job.*
- Every manifest parses and carries `apiVersion`, `kind` and `metadata.name`.
  *Verified during this milestone; 23 documents.*
- The Compose stack still resolves with the dashboard, collector and remediation
  services added. *Verified by `docker compose config`.*
- Enabling the collector requires no application change. *Follows from the OTLP
  exporter path implemented in milestone 1 and exercised by `obs` tests.*

## Revisit this decision if

- The dashboard grows past a handful of screens, at which point a router and
  possibly a component library start to pay for themselves — and frontend tests
  stop being optional.
- A second deployment environment appears, which is the moment for Kustomize
  overlays (alternative E).
- Sub-second liveness is genuinely needed, which is a backend change (server-sent
  events) before it is a frontend one.
- The project ever needs to be genuinely multi-region, at which point §15.4 stops
  being analysis and becomes a milestone.

## References

- `docs/architecture.md` §15 — the dashboard, the collector, the cluster topology
  and the multi-region analysis.
- `deploy/k8s/README.md` — every deliberate limitation of the manifests.
- `web/README.md` — the dashboard's own choices and non-goals.
