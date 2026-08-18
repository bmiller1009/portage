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

All of this has been run live end-to-end on the remote Kubernetes cluster: `plane run` → `POST /v1/runs` → reconciler → real SparkApplication → `SUCCEEDED`, with verified-correct output in S3, and (separately, with the base image's bundled `spark-examples.jar`) a real JVM main class executing via the `mainClass`/`jars` translation; `plane cancel` against a genuinely `RunningHealthy` SparkApplication → reconciler → the resource actually deleted from the cluster → `CANCELED`; a GPU-requiring workload failing `plane workload validate --environment` with a clear capability-mismatch message before submission; both `/metrics` endpoints showing real, non-zero data after a real run; and — the spec §45 acceptance test itself — `make dev` run twice in a row (proving idempotency) from a completely blank cluster, followed by `plane run examples/wordcount.yaml --environment local` reaching `SUCCEEDED` with verified output in the freshly-provisioned MinIO, then `make dev-down` cleanly tearing down only what it created.

**v0.2 ("Enterprise Storage") is complete.** A formal `StorageProvider` Protocol (`control_plane/storage_provider.py`) now mirrors `ExecutionProvider`'s capability model. VAST is supported in both its S3-compatible mode (a thin adapter delegating to `S3StorageProvider`) and its NFS mode — the latter required a new cross-cutting `volume_mounts` channel from storage provider through `ResolvedWorkload` to the Kubernetes execution provider's pod spec, since NFS access isn't expressible as `spark.hadoop.*` config. ADLS is supported with both static-key and workload-identity credential paths. The artifact repository abstraction (spec §51) resolves `artifact://name/version` references through a per-environment binding table before a provider ever sees the workload, closing a gap every example workload had silently relied on since v0.1. Runtime profiles (spec §18) let a workload request a named profile (e.g. `high-memory`) that each environment translates independently — a Kubernetes `nodeSelector` or a Databricks `node_type_id` override — with no profile match falling back to default behavior rather than erroring. A minimal operational UI (`ui/`, spec §32 — React/TypeScript/Vite/TanStack Query, no component library) gives read access to runs, workloads, environments, datasets, and providers, plus cancel/logs on a run's detail page — a pure client of the public REST API, no privileged backend of its own.

VAST and ADLS are translation-layer providers verified against unit tests and fakes, not live hardware — there's no self-hostable open-source VAST substitute and no Azure subscription available, so (like Databricks in v0.1) they're tracked as credentials/hardware-blocked rather than silently skipped. VAST's S3 mode was additionally live-verified against the real MinIO already running on the remote cluster, since its wire protocol is genuinely S3-compatible. Runtime profiles, the artifact abstraction, and the NFS volume-mount manifest shape were all live-verified against the real remote Kubernetes cluster; the UI was live-verified in a real browser against the real remote API.

**v0.3 ("Databricks Portability") is in progress — all scope buildable without a live Databricks workspace is done, the hero demo's live run is not.** A real bug was found and fixed along the way: `provider_factory.build_execution_provider()` used to construct a live `WorkspaceClient()` eagerly for any Databricks profile, which crashed with an unrelated SDK auth error whenever no Databricks credentials were configured — even for `plane workload validate`, which never touches a client. Client construction is now lazy, built only inside `submit()`/`status()`/`cancel()` via OAuth M2M (`host`/`client_id`/`client_secret`), with credentials resolved through the same `credential_reference` pattern the storage providers already use. The Databricks Runtime <-> Spark version compatibility matrix was corrected against current public docs (Databricks Runtime 17.3 LTS ships Spark 4.0.0, 18.x ships Spark 4.1.0 — no runtime ships 4.2 yet), so `plane workload validate` no longer falsely reports a Spark-4.2 workload as Databricks-compatible. `plane workload validate` also gained a repeatable `--environment` flag for static, offline cross-provider portability checks in one command.

All of this is live-verified on the remote box, including the hero demo's actual claim demonstrated statically: `plane workload validate examples/claims-normalization.yaml --environment k8s-vast-s3 --environment azure-dbx-v1` — the same workload, same artifact reference, no application changes — reports `PASS` against both a real Kubernetes+VAST environment and a Databricks+ADLS environment, in one command, over the real API. What's not done, and can't be from here: an actual live `plane run` against a real Databricks workspace succeeding — that needs real OAuth M2M credentials and a live workspace, the same constraint that's kept [issue #8](../../issues/8) (the Phase 0 exit demonstration) open since the start of the project.

**v0.4 ("Conformance & Reliability") is complete.** Unlike v0.1-v0.3, nothing in this milestone's actual scope was blocked on missing credentials — it's reliability, security, and testing-framework work, not a live-Databricks demo. Retry semantics classify every provider error as retryable or terminal (`RetryableProviderError`/`TerminalProviderError`), covering `submit()`, `status()`, and `cancel()` alike; the reconciler claims runs via `SELECT ... FOR UPDATE SKIP LOCKED` before submitting, so concurrent reconciler replicas are HA-safe, and a stable per-run idempotency token flows into Databricks's `jobs.submit()` while Kubernetes recovers from a 409 by reading back the resource instead of failing a run that already succeeded. OIDC authentication (real JWT/JWKS verification) and a five-role RBAC hierarchy (`api/auth.py`) gate every mutating endpoint, opt-in via `PORTAGE_AUTH_MODE` so nothing changes by default. An identity-bearing audit trail (`AuditEvent`, spec §36) records every privileged action, exportable via `GET /v1/audit`. A shared provider contract test suite runs the same behavioral checks against every execution and storage provider — the automated, always-on form of "provider certification." A semantic result comparator (never byte-for-byte, spec §22) backs a dynamic conformance runner, `plane conformance test`, and a provider certification report, `plane conformance report`, matching spec §78's own compatibility-matrix format.

Three real bugs were found and fixed via live verification against the real remote infrastructure, not caught by any mocked unit test: a duplicate-submission crash window in the reconciler; a `MissingGreenlet` response-serialization bug from recording an audit event right before returning the same ORM object; and a retry-classification gap where `status()`/`cancel()` weren't covered by the submit-path fix, so a transient network blip while polling could fail a run that was actually fine. A live fault-injection test killed a real Spark driver pod mid-run and confirmed the reconciler correctly reported `FAILED`; a separate test killed and restarted the API process while a different run was `RUNNING` and confirmed zero impact, since the reconciler operates as a fully independent process.

**v0.5 ("Table & Query Integration") is complete.** Unlike v0.2/v0.3 (VAST/Databricks/Azure — genuinely inaccessible from here), Iceberg REST Catalog, Trino, and Kyuubi are all real, self-hostable open-source systems, so every piece of this milestone was live-verified end to end rather than left as a translation-layer prototype — including Kyuubi, which spec §68 marks explicitly optional. A `Dataset` binding can now be `kind: table` (spec §11) alongside the existing `kind: path`, resolving into `spark.portable.dataset.<name>.identifier` plus per-run Iceberg REST catalog registration (`spec/dataset/v1alpha1.py::resolve_iceberg_catalog_config()`) — a no-op for path-only workloads, so v0.1-v0.4 behavior is unchanged. `deploy/dev/iceberg-rest.yaml`/`trino.yaml`/`kyuubi.yaml` stand up the real catalog and both query engines (Kyuubi via a custom image, `providers/query/kyuubi/image/` — no official prebuilt Kyuubi+Spark image exists), composed behind the same portability boundary as every other provider (spec §38: "the framework still does not own the query engine"). See [`docs/providers/query.md`](docs/providers/query.md) for the JDBC connection strings and the real Iceberg/Spark/S3 compatibility gaps found live-verifying this — none of it worked on the first attempt, and each fix is a genuine, previously-undocumented-until-tried defect, not a guess.

Live-verified on the remote cluster: a real Iceberg table written by one Portage-submitted Spark job, read back by a second, completely independent Portage-submitted Spark job purely through the logical dataset name (not a hardcoded path), then queried by both Trino and Kyuubi via `beeline`/the `trino` CLI — four independent readers, one consistent table, through the shared REST catalog. See the [v1.0 milestone](../../milestones) for what's next.

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
