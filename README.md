# Portage — Portable Spark Runtime

Portage is a small, open-source control plane for running Apache Spark workloads independently of the infrastructure on which they execute. It defines a **Portable Workload Contract** — application artifact, entry point, Spark runtime requirements, logical datasets, resources, execution policy, identity, and arguments — and resolves that contract against pluggable execution providers (Kubernetes, Databricks) and storage providers (VAST, S3, ADLS).

```
                       Portable Workload
                              │
                              ▼
                     Portable Workload API
                              │
                  ┌───────────┴───────────┐
                  │                       │
          Execution Provider       Data Environment
                  │                       │
          ┌───────┴────────┐      ┌───────┼───────┐
          │                │      │       │       │
      Kubernetes      Databricks  VAST    S3     ADLS
          │                │
          ▼                ▼
      Apache Spark    Databricks Runtime
```

The same workload definition, run against different environments:

```
plane run claims-normalization --environment onprem-prod   # Kubernetes + VAST
plane run claims-normalization --environment azure-prod    # Databricks + ADLS
```

No code changes. No infrastructure objects in the workload definition.

## Product thesis

> A conforming workload MUST NOT belong to the infrastructure on which it happens to execute.

Portage is not a Spark platform and does not compete with Databricks, Cloudera, Trino, Iceberg, or Kubernetes. It composes those systems behind a stable portability boundary. See [`docs/architecture/spec.md`](docs/architecture/spec.md) for the full architecture and development specification, and [`docs/adr/`](docs/adr/) for the individual decisions that follow from it.

## Non-goals

Portage will not become a notebook environment, BI/dashboard platform, visual ETL designer, general workflow orchestrator, distributed SQL engine, distributed filesystem, object store, data warehouse, ML platform, model registry, streaming platform, Kafka distribution, secret manager, identity provider, data marketplace, enterprise catalog UI, lineage engine, vector database, or AI agent framework. Projects that solve those problems should integrate with Portage; they should not be absorbed by it.

## Status

**Phase 0 (architectural spike) — Kubernetes+S3 leg proven live.** `plane run examples/wordcount.yaml --environment k8s-remote` has actually executed a real PySpark artifact on a real Kubernetes cluster via the Apache Spark Kubernetes Operator, reading/writing through an S3-compatible backend, with verified-correct output — not a simulation. See [`docs/providers/kubernetes.md`](docs/providers/kubernetes.md) and [`docs/providers/s3.md`](docs/providers/s3.md). The Databricks+S3 leg remains a translation-layer prototype, tested against a mocked client only — no live workspace credentials were available (tracked in [issue #8](../../issues/8)).

**v0.1 ("Portable Spark Core") is complete.** The persistence layer is real (`control_plane/db.py`/`models.py`/`repositories.py`, migrated via `alembic/`), and so is the async submission/cancellation path the spec describes (§24): `POST /v1/runs` persists `ACCEPTED` and returns immediately; `reconciler/service.py` picks it up, resolves the environment from the database, submits to the real execution provider, and (on `DELETE /v1/runs/{id}`) cancels it. `plane` (the CLI) is a pure REST client — no direct provider access. Both execution providers support JVM JAR artifacts (`mainClass`/`jars` on Kubernetes, `SparkJarTask` on Databricks) alongside Python wheels, and both run real capability matching (spec §20-21) — `plane workload validate --environment` fails fast on an incompatible workload before any submission is attempted. `GET /metrics` (API) and `:9091/metrics` (reconciler) expose OpenTelemetry-instrumented Prometheus metrics (spec §29). `make dev` (spec §45, `scripts/dev-up.sh`) stands up kind + PostgreSQL + MinIO + the Spark Operator + the control plane from a blank machine, idempotently, with zero cloud accounts.

All of this has been run live end-to-end on the remote Kubernetes cluster: `plane run` → `POST /v1/runs` → reconciler → real SparkApplication → `SUCCEEDED`, with verified-correct output in S3, and (separately, with the base image's bundled `spark-examples.jar`) a real JVM main class executing via the `mainClass`/`jars` translation; `plane cancel` against a genuinely `RunningHealthy` SparkApplication → reconciler → the resource actually deleted from the cluster → `CANCELED`; a GPU-requiring workload failing `plane workload validate --environment` with a clear capability-mismatch message before submission; both `/metrics` endpoints showing real, non-zero data after a real run; and — the spec §45 acceptance test itself — `make dev` run twice in a row (proving idempotency) from a completely blank cluster, followed by `plane run examples/wordcount.yaml --environment local` reaching `SUCCEEDED` with verified output in the freshly-provisioned MinIO, then `make dev-down` cleanly tearing down only what it created. See the [v1.0 milestone](../../milestones) for what's next.

## Repository layout

| Path | Purpose |
|---|---|
| `api/` | FastAPI app — REST surface |
| `control_plane/` | Domain services: validation, resolution, run lifecycle, audit, persistence |
| `alembic/` | Database migrations (PostgreSQL, via `control_plane/models.py`) |
| `reconciler/` | Async worker that submits to and polls execution providers |
| `providers/execution/` | Kubernetes and Databricks execution providers |
| `providers/storage/` | VAST, S3, ADLS storage providers |
| `providers/identity/` | OIDC / machine identity |
| `spec/` | Portable workload, environment, and dataset schemas |
| `conformance/` | Static and dynamic cross-provider conformance runner |
| `cli/` | `plane` CLI |
| `ui/` | Minimal operational UI (React/TS/Vite) |
| `charts/` | Helm charts |
| `sdk/python/` | Optional convenience SDK (never required) |
| `examples/` | Example portable workloads |
| `tests/` | Unit, contract, integration, e2e, security, chaos suites |
| `docs/` | Architecture spec, ADRs, provider docs |

## Development

```
make dev       # stand up kind + PostgreSQL + MinIO + Spark Operator + the control plane
make dev-down  # tear it back down
make test      # run the unit test suite
make lint      # ruff + pyright
```

`make dev` (spec §45, `scripts/dev-up.sh`) requires Docker, `kind`, `helm`, and `kubectl` on `PATH`; every other dependency (PostgreSQL, MinIO, the Spark Operator) is provisioned inside a local `kind` cluster, so no cloud account is needed. It's idempotent — safe to re-run after a partial failure. Once it finishes:

```
PORTAGE_API_URL=http://127.0.0.1:8124 plane run examples/wordcount.yaml --environment local
```

should succeed. On Linux, if `kind create cluster` fails with `could not find a log line that matches ...Multi-User System...`, raise `fs.inotify.max_user_instances` (kind's own [known issue](https://kind.sigs.k8s.io/docs/user/known-issues/#pod-errors-due-to-too-many-open-files)) — `scripts/dev-up.sh` checks for this and prints the fix if it looks too low.

## License and governance

Apache License 2.0. Governance is currently a single lead maintainer plus a public GitHub repo, roadmap, issues, and PR review — no foundation, no steering committee. If multiple organizations become meaningful contributors, governance can evolve.
