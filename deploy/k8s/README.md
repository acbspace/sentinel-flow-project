# Kubernetes manifests

Plain YAML, no Helm and no Kustomize. The stack is eight workloads with a handful
of knobs; a templating layer would add a toolchain and an indirection for
substitution this does not yet need. When a second environment appears and the
manifests start diverging, that is the moment to reach for Kustomize overlays —
not before.

```bash
kubectl apply -f deploy/k8s/00-namespace.yaml
kubectl apply -f deploy/k8s/            # the rest, in name order
kubectl -n sentinelflow get pods
```

## What is here

| File | Contents |
|---|---|
| `00-namespace.yaml` | The `sentinelflow` namespace |
| `10-config.yaml` | Non-secret configuration shared by every service |
| `11-secret.yaml` | **Development placeholders only** — see the warning below |
| `20-postgres.yaml` | PostgreSQL StatefulSet + headless Service |
| `21-kafka.yaml` | Single-broker Kafka StatefulSet + Service |
| `22-temporal.yaml` | Temporal server Deployment + Service |
| `30-migrate-job.yaml` | Schema migration Job, run before the applications |
| `40-ingestion-api.yaml` | Deployment + Service + HPA |
| `41-incident-engine.yaml` | Deployment + Service (HPA capped at the partition count) |
| `42-incidents-api.yaml` | Deployment + Service + HPA |
| `43-alerting.yaml` | Deployment + Service |
| `44-remediation.yaml` | Deployment + Service |

## Deliberate limitations

These manifests are a faithful translation of the Compose topology, not a
production deployment. Specifically:

- **The secret holds development placeholders.** Real credentials belong in a
  sealed secret, an external secrets operator, or your cloud's secret manager.
  Nothing here should be applied to a cluster that matters as-is.
- **PostgreSQL and Kafka run as single-replica StatefulSets.** Both are stateful
  systems that deserve an operator (CloudNativePG, Strimzi) or a managed service.
  A single Kafka broker with replication factor 1 loses data on node failure —
  the same caveat the Compose stack carries, made explicit here.
- **The incident engine's HPA is capped at the topic's partition count.** Scaling
  a consumer group beyond its partitions adds idle members, not throughput. This
  is why its `maxReplicas` is 3 and not something larger.
- **No Ingress, NetworkPolicy, PodDisruptionBudget or ServiceMonitor.** Each is
  environment-specific; adding guesses would be worse than leaving the seam
  visible.
- **No image registry.** The images are named `sentinelflow/<service>:latest`
  with `imagePullPolicy: IfNotPresent`, which works against a local cluster
  (kind/minikube) after `make build-images`. A real deployment pins a digest from
  a registry.

## Why the resource requests look the way they do

Every workload sets requests and limits, because a pod without requests is
unschedulable in any cluster with quotas and is the first thing evicted under
pressure. The values are deliberately modest and are starting points to be
replaced by measurements, not tuned guesses presented as fact.
