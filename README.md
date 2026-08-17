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

**Now in v0.1 ("Portable Spark Core").** The persistence layer is real: `Environment`/`ExecutionProfile`/`StorageProfile`/`DatasetBinding`/`WorkloadDefinition` are backed by PostgreSQL (`control_plane/db.py`, `models.py`, `repositories.py`, migrated via `alembic/`) with CRUD REST endpoints (`api/routers/`) and read-access CLI commands (`plane environment list`, `plane dataset list`). Run submission (`POST /v1/runs`), the async reconciler, and migrating `plane run` off its current direct-provider-call path are the next backlog items — see the [v0.1 milestone](../../milestones).

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
make dev    # install the package in editable mode with dev dependencies
make test   # run the unit test suite
make lint   # ruff + pyright
```

`make dev` does not yet stand up the full local stack (kind, PostgreSQL, MinIO, Spark Operator) described in the spec — that's tracked as a Phase 0 backlog item.

## License and governance

Apache License 2.0. Governance is currently a single lead maintainer plus a public GitHub repo, roadmap, issues, and PR review — no foundation, no steering committee. If multiple organizations become meaningful contributors, governance can evolve.
