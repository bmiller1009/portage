# Portage

An open workload portability layer for Apache Spark.

Run the same Spark workload across Kubernetes and Databricks without coupling application code to compute or storage infrastructure.

```
Project:      Portage
PyPI package: portage-runtime
CLI:          plane
License:      Apache License 2.0
```

## What Portage is

Portage is a small, open-source control plane for running Apache Spark workloads independently of the infrastructure on which they execute. It defines a **Portable Workload Contract** — application artifact, entry point, Spark runtime requirements, logical datasets, resources, execution policy, identity, and arguments — and resolves that contract against pluggable execution providers (Kubernetes, Databricks) and storage providers (VAST, S3, ADLS).

Portage is a **portable workload runtime/control plane**, not a lakehouse platform. It doesn't compete with Databricks, Cloudera, Trino, Iceberg, or Kubernetes — it composes those systems behind a stable portability boundary. See [Non-goals](#non-goals) below.

## Why it exists

> A conforming workload MUST NOT belong to the infrastructure on which it happens to execute.

Spark workloads accumulate infrastructure coupling by default — cluster-specific paths, provider-specific configuration, storage-specific credentials baked into application code. That coupling is what makes moving a workload between Kubernetes and Databricks, or between S3 and ADLS, an application-rewrite project instead of a configuration change. Portage's whole job is to keep that coupling out of the application and in the control plane, where it belongs — so a workload written once can run wherever your infrastructure actually is, including "somewhere different than it started."

## Hero example

The same workload definition, run against different environments:

```bash
plane run claims-normalization --environment kubernetes-env   # Kubernetes execution
plane run claims-normalization --environment databricks-env   # Databricks execution
```

No code changes. No infrastructure objects in the workload definition. Same workload file, same application artifact, same logical dataset names — only the environment name differs.

This isn't a slide — it's live-verified. `examples/claims_app` is a real two-input-join Spark workload (raw claims joined against a provider reference table, not just a map/reduce) that has actually reached `SUCCEEDED` on a real Kubernetes cluster (execution provider `kubernetes`, storage provider `s3` against real MinIO) and a real Databricks Serverless workspace (execution provider `databricks`, output written to and confirmed in a Databricks-managed Unity Catalog volume), with semantically equivalent output on both — Portage's formal portability guarantee (see [Semantic conformance](#core-principles), ADR 0011). In this specific hero run the generated output also happened to be byte-identical, which is a nice-to-have observed result, not the contract itself: physical file layout, compression, or partitioning may legitimately differ between providers without that being a portability failure. See [`docs/providers/kubernetes.md`](docs/providers/kubernetes.md), [`docs/providers/databricks.md`](docs/providers/databricks.md), and [`docs/verification/v1.0.0.md`](docs/verification/v1.0.0.md) for exactly what was tested and how to reproduce it.

- **Reproducible locally, no credentials needed**: `make dev && plane run examples/wordcount.yaml --environment local` (see [Quick start](#quick-start)).
- **Verified live, requires real infrastructure you won't have**: the run above needs a real Kubernetes cluster and real Databricks workspace credentials — reproducing it requires standing up that infrastructure yourself, not something a reader can run from a laptop. VAST and ADLS are implemented as storage providers but were **not** part of this live-verified execution path — see [Supported environments](#supported-environments) and the [compatibility matrix](#compatibility-matrix) below for their actual, more limited verification status.

## Supported environments

| Execution | Storage |
|---|---|
| Kubernetes (Apache Spark Kubernetes Operator) — live-tested | S3 / S3-compatible (MinIO) — live-tested; VAST S3 mode — live-tested against real MinIO standing in for VAST's S3-compatible wire protocol, no real VAST hardware |
| Databricks (Serverless — live-tested; classic clusters — supported, not live-tested against this project's own workspace, which forbids them) | VAST NFS mode and ADLS — implemented and unit-tested against fakes only, not live-tested against real hardware/a real Azure subscription |

See the [compatibility matrix](#compatibility-matrix) below for exactly what's tested vs. experimental.

## Quick start

Requires Docker, `kind`, `helm`, and `kubectl` on `PATH`. Everything else (PostgreSQL, MinIO, the Spark Operator) is provisioned inside a local `kind` cluster — no cloud account needed.

```bash
git clone https://github.com/bmiller1009/portage.git
cd portage
make dev
PORTAGE_API_URL=http://127.0.0.1:8124 plane run examples/wordcount.yaml --environment local
```

`make dev` (spec §45, `scripts/dev-up.sh`) is idempotent — safe to re-run after a partial failure. `make dev-down` tears down only what it created.

```bash
make dev       # stand up kind + PostgreSQL + MinIO + Spark Operator + the control plane
make dev-down  # tear it back down
make test      # run the unit test suite
make lint      # ruff + pyright
```

On Linux, if `kind create cluster` fails with `could not find a log line that matches ...Multi-User System...`, raise `fs.inotify.max_user_instances` (kind's own [known issue](https://kind.sigs.k8s.io/docs/user/known-issues/#pod-errors-due-to-too-many-open-files)) — `scripts/dev-up.sh` checks for this and prints the fix if it looks too low.

A new contributor does not need Databricks, AWS, Azure, or VAST access to run the basic development/test suite — see [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Architecture summary

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

A REST API + reconciler control plane (`api/`, `reconciler/`) persists run state in PostgreSQL and asynchronously submits/polls/reconciles against whichever execution provider an environment names — the reconciler runs safely as multiple concurrent replicas (row-locked claiming), retries provider errors classified as retryable vs. terminal, and recovers cleanly from process restarts mid-run. `plane` (the CLI) and the operational UI (`ui/`) are both pure REST clients — neither talks to a database or a provider directly. See [`docs/architecture/spec.md`](docs/architecture/spec.md) for the full specification and [`docs/adr/`](docs/adr/) for the individual decisions that follow from it.

## Core principles

- **The portable workload contract is the product** (ADR 0002) — everything else either implements that contract or is an integration.
- **A conforming workload never hardcodes infrastructure.** Provider-specific configuration is allowed as an explicit, namespaced `providerOverrides` escape hatch, but it degrades the workload's reported portability status rather than silently passing as fully portable (ADR 0010).
- **Thin control plane, no absorbed subsystems** (ADR 0003) — Portage orchestrates; it doesn't reimplement a scheduler, a catalog, or a query engine.
- **Upstream-first** (ADR 0004) — no custom Spark scheduler; translation targets real, supported submission APIs (Spark Kubernetes Operator, Databricks Jobs API).
- **Capability-driven provider interface** (ADR 0005) — providers declare what they support; the control plane matches workload requirements against declared capabilities, not `if provider == "x"` branching.
- **Semantic conformance, never byte-for-byte** (ADR 0011) — cross-provider output comparison checks schema, row count, null counts, partition coverage, and deterministic hashes. Physical file differences (layout, partitioning, compression) are not portability failures.

### Non-goals

Portage will not become a notebook environment, BI/dashboard platform, visual ETL designer, general workflow orchestrator, distributed SQL engine, distributed filesystem, object store, data warehouse, ML platform, model registry, streaming platform, Kafka distribution, secret manager, identity provider, data marketplace, enterprise catalog UI, lineage engine, vector database, or AI agent framework. Projects that solve those problems should integrate with Portage; they should not be absorbed by it.

## Compatibility matrix

Terminology: **SUPPORTED** — the code path exists and is exercised by tests. **AVAILABLE** — confirmed to exist/be compatible via a live infrastructure query, but no full workload was actually run under it. **TESTED** — actually run against real infrastructure at least once. **CERTIFIED** — passes the full automated provider-contract suite against real infrastructure. **EXPERIMENTAL** — implemented and unit-tested against fakes only; no real infrastructure available to this project to test against.

| Component | Status |
|---|---|
| Python | 3.12+ (SUPPORTED) |
| PostgreSQL | 16 (TESTED — CI service container + real remote deployments) |
| Kubernetes | 1.34 (TESTED) |
| Apache Spark Kubernetes Operator | 1.8.0 chart / 1.0.0 app version (TESTED) |
| Apache Spark on Kubernetes | 4.1, 4.2 (CERTIFIED — provider contract suite + live runs) |
| Databricks Runtime 17.3 LTS (Spark 4.0) | AVAILABLE — compatibility mapping confirmed live via a real workspace's `clusters.spark_versions()`; no full hero-workload execution run under this exact version |
| Databricks Runtime 18.2 (Spark 4.1) | TESTED — the hero-demo workload (`examples/claims_app`) reached `SUCCEEDED` live under this version |
| Databricks Runtime 19 (Spark 4.2) | TESTED — a version-bumped copy of the hero-demo workload reached `SUCCEEDED` live under this version (v1.0.0 release-hardening pass) |
| S3 / S3-compatible (MinIO) | CERTIFIED |
| VAST, S3 mode | TESTED (live-verified against real MinIO; VAST's own hardware not available to this project) |
| VAST, NFS mode | EXPERIMENTAL (no VAST hardware available to this project) |
| ADLS | EXPERIMENTAL (no Azure subscription available to this project) |
| Iceberg REST Catalog / Trino / Kyuubi | TESTED (v0.5, fully self-hostable, live-verified end to end) |

Never inferred from vendor documentation alone where a real check was possible — see [`docs/verification/v1.0.0.md`](docs/verification/v1.0.0.md) for exactly how each row above was verified and its commit SHA.

## Known limitations

- **Batch workloads have the strongest portability guarantees.** Structured Streaming portability is more limited — streaming workloads exercise less of the cross-provider conformance suite today.
- **`providerOverrides` degrade portability by design, not by accident** (ADR 0010) — a workload using them is still valid and schedulable, but `plane workload validate` reports `PORTABLE_WITH_OVERRIDES` rather than `PORTABLE`, and the override counts are always surfaced, never hidden.
- **VAST NFS and S3-compatible object storage have different filesystem semantics** — Portage's dataset abstraction hides the URI/path difference, not the underlying consistency/locking model.
- **Data-location mapping does not move bytes.** Binding the same logical dataset name to two different physical locations (one per environment) is the reader's/operator's responsibility, not something Portage does automatically.
- **Databricks and upstream Apache Spark can produce different physical output layouts** for the same logical operation (partitioning, file counts, compression) — expected, and exactly why conformance comparison is semantic, never byte-for-byte.
- **Not all Spark configuration keys are meaningful on Databricks Serverless** — Serverless's Spark Connect session rejects non-Spark-builtin config keys outright; Portage routes portable dataset/storage config through task parameters instead where needed (see `docs/providers/databricks.md`).
- **Third-party execution/storage providers require a core-code registration today** — adding a provider means implementing the `ExecutionProvider`/`StorageProvider` Protocol and adding one branch in `control_plane/provider_factory.py`, not installing a dynamically-loaded plugin. See [`docs/architecture/PROVIDER_SDK.md`](docs/architecture/PROVIDER_SDK.md).
- **Spark Declarative Pipelines**: the spec-layer and Kubernetes-manifest translation are built and unit-tested, but running one to completion needs a persistent Spark Connect server as its own deployed resource — not built yet (see [`docs/releases/development-history.md`](docs/releases/development-history.md)'s v0.6 entry for the confirmed-live blocker).

## Performance expectations

Portage controls orchestration — submission, reconciliation, retries, state tracking — not Spark's own execution engine. It makes no claim that Kubernetes is faster than Databricks or vice versa; that depends entirely on your cluster sizing, data layout, and workload shape, not on which control plane submitted the job. If you're evaluating performance, measure Spark's own execution time separately from Portage's control-plane overhead (submission latency, reconciliation lag) — `GET /metrics` (API) and `:9091/metrics` (reconciler) expose the latter as OpenTelemetry/Prometheus metrics.

## Documentation

- [`docs/architecture/spec.md`](docs/architecture/spec.md) — the full architecture and development specification.
- [`docs/adr/`](docs/adr/) — individual architecture decision records.
- [`docs/architecture/STABILITY.md`](docs/architecture/STABILITY.md) — schema/API stability and deprecation policy.
- [`docs/architecture/PROVIDER_SDK.md`](docs/architecture/PROVIDER_SDK.md) — the stable provider extension surface and how to add a provider today.
- [`docs/providers/`](docs/providers/) — per-provider live-run results and configuration.
- [`docs/verification/v1.0.0.md`](docs/verification/v1.0.0.md) — exactly what was tested for the 1.0.0 release, and what wasn't.
- [`docs/releases/development-history.md`](docs/releases/development-history.md) — the full milestone-by-milestone development narrative (Phase 0 through v1.0.0).
- [`CHANGELOG.md`](CHANGELOG.md) — version-facing release notes.
- [`tests/README.md`](tests/README.md) — what each test suite covers, and how to run it.

## Repository layout

| Path | Purpose |
|---|---|
| `api/` | FastAPI app — REST surface |
| `control_plane/` | Domain services: validation, resolution, run lifecycle, audit, persistence |
| `alembic/` | Database migrations (PostgreSQL, via `control_plane/models.py`) |
| `reconciler/` | Async worker that submits to and polls execution providers |
| `providers/execution/` | Kubernetes and Databricks execution providers |
| `providers/storage/` | VAST, S3, ADLS storage providers |
| `providers/query/` | Kyuubi's custom application image (Trino needs none — official image, config only) |
| `providers/identity/` | Machine identity / credential documentation (see the directory's own README) |
| `spec/` | Portable workload, environment, and dataset schemas |
| `conformance/` | Static and dynamic cross-provider conformance runner |
| `cli/` | `plane` CLI |
| `ui/` | Minimal operational UI (React/TS/Vite) |
| `charts/` | Helm charts |
| `sdk/python/` | Optional convenience SDK (never required) |
| `terraform/provider-portage/` | Terraform provider (Go) wrapping the config-resource REST API |
| `integrations/airflow/` | Airflow provider/operator, self-contained REST client |
| `examples/` | Example portable workloads |
| `tests/` | Unit, contract, integration, e2e, security, chaos suites |
| `docs/` | Architecture spec, ADRs, provider docs, verification records |

## Project status

**Portage 1.0.0 is GA.** Every spec §71 exit criterion is live-verified except one documented, accepted gap: VAST NFS-mode and ADLS storage remain translation-layer-only (no VAST hardware or Azure subscription available to this project). See [`docs/verification/v1.0.0.md`](docs/verification/v1.0.0.md) for the full verification record and [`docs/releases/development-history.md`](docs/releases/development-history.md) for the complete development narrative.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for development setup, test-running expectations, and PR conventions. See [`SECURITY.md`](SECURITY.md) for how to report a security issue privately, and [`SUPPORT.md`](SUPPORT.md) for what support this project does and doesn't provide.

## License and governance

[Apache License 2.0](LICENSE). Governance is currently a single lead maintainer plus a public GitHub repo, roadmap, issues, and PR review — no foundation, no steering committee. If multiple organizations become meaningful contributors, governance can evolve.
