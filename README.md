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

Portage is not a Spark platform and does not compete with Databricks, Cloudera, Trino, Iceberg, or Kubernetes. It composes those systems behind a stable portability boundary. See [`docs/architecture/spec.md`](docs/architecture/spec.md) for the full architecture and development specification, and [`docs/adr/`](docs/adr/) for the individual decisions that follow from it. As of v1.0, the workload/environment/dataset/artifact schemas and the provider extension surface are stable — see [`docs/architecture/STABILITY.md`](docs/architecture/STABILITY.md) and [`docs/architecture/PROVIDER_SDK.md`](docs/architecture/PROVIDER_SDK.md).

## Non-goals

Portage will not become a notebook environment, BI/dashboard platform, visual ETL designer, general workflow orchestrator, distributed SQL engine, distributed filesystem, object store, data warehouse, ML platform, model registry, streaming platform, Kafka distribution, secret manager, identity provider, data marketplace, enterprise catalog UI, lineage engine, vector database, or AI agent framework. Projects that solve those problems should integrate with Portage; they should not be absorbed by it.

## Status

**Phase 0 (architectural spike) — both legs proven live.** `plane run examples/wordcount.yaml --environment k8s-remote` has actually executed a real PySpark artifact on a real Kubernetes cluster via the Apache Spark Kubernetes Operator, reading/writing through an S3-compatible backend, with verified-correct output — not a simulation. See [`docs/providers/kubernetes.md`](docs/providers/kubernetes.md) and [`docs/providers/s3.md`](docs/providers/s3.md). The Databricks leg was a translation-layer prototype until v0.3, when real workspace credentials became available — see v0.3 below for its own live run ([issue #8](../../issues/8), closed).

**v0.1 ("Portable Spark Core") is complete.** The persistence layer is real (`control_plane/db.py`/`models.py`/`repositories.py`, migrated via `alembic/`), and so is the async submission/cancellation path the spec describes (§24): `POST /v1/runs` persists `ACCEPTED` and returns immediately; `reconciler/service.py` picks it up, resolves the environment from the database, submits to the real execution provider, and (on `DELETE /v1/runs/{id}`) cancels it. `plane` (the CLI) is a pure REST client — no direct provider access. Both execution providers support JVM JAR artifacts (`mainClass`/`jars` on Kubernetes, `SparkJarTask` on Databricks) alongside Python wheels, and both run real capability matching (spec §20-21) — `plane workload validate --environment` fails fast on an incompatible workload before any submission is attempted. `GET /metrics` (API) and `:9091/metrics` (reconciler) expose OpenTelemetry-instrumented Prometheus metrics (spec §29). `make dev` (spec §45, `scripts/dev-up.sh`) stands up kind + PostgreSQL + MinIO + the Spark Operator + the control plane from a blank machine, idempotently, with zero cloud accounts.

All of this has been run live end-to-end on the remote Kubernetes cluster: `plane run` → `POST /v1/runs` → reconciler → real SparkApplication → `SUCCEEDED`, with verified-correct output in S3, and (separately, with the base image's bundled `spark-examples.jar`) a real JVM main class executing via the `mainClass`/`jars` translation; `plane cancel` against a genuinely `RunningHealthy` SparkApplication → reconciler → the resource actually deleted from the cluster → `CANCELED`; a GPU-requiring workload failing `plane workload validate --environment` with a clear capability-mismatch message before submission; both `/metrics` endpoints showing real, non-zero data after a real run; and — the spec §45 acceptance test itself — `make dev` run twice in a row (proving idempotency) from a completely blank cluster, followed by `plane run examples/wordcount.yaml --environment local` reaching `SUCCEEDED` with verified output in the freshly-provisioned MinIO, then `make dev-down` cleanly tearing down only what it created.

**v0.2 ("Enterprise Storage") is complete.** A formal `StorageProvider` Protocol (`control_plane/storage_provider.py`) now mirrors `ExecutionProvider`'s capability model. VAST is supported in both its S3-compatible mode (a thin adapter delegating to `S3StorageProvider`) and its NFS mode — the latter required a new cross-cutting `volume_mounts` channel from storage provider through `ResolvedWorkload` to the Kubernetes execution provider's pod spec, since NFS access isn't expressible as `spark.hadoop.*` config. ADLS is supported with both static-key and workload-identity credential paths. The artifact repository abstraction (spec §51) resolves `artifact://name/version` references through a per-environment binding table before a provider ever sees the workload, closing a gap every example workload had silently relied on since v0.1. Runtime profiles (spec §18) let a workload request a named profile (e.g. `high-memory`) that each environment translates independently — a Kubernetes `nodeSelector` or a Databricks `node_type_id` override — with no profile match falling back to default behavior rather than erroring. A minimal operational UI (`ui/`, spec §32 — React/TypeScript/Vite/TanStack Query, no component library) gives read access to runs, workloads, environments, datasets, and providers, plus cancel/logs on a run's detail page — a pure client of the public REST API, no privileged backend of its own.

VAST and ADLS are translation-layer providers verified against unit tests and fakes, not live hardware — there's no self-hostable open-source VAST substitute and no Azure subscription available, so (like Databricks in v0.1) they're tracked as credentials/hardware-blocked rather than silently skipped. VAST's S3 mode was additionally live-verified against the real MinIO already running on the remote cluster, since its wire protocol is genuinely S3-compatible. Runtime profiles, the artifact abstraction, and the NFS volume-mount manifest shape were all live-verified against the real remote Kubernetes cluster; the UI was live-verified in a real browser against the real remote API.

**v0.3 ("Databricks Portability") is complete — including a real live run, once real workspace credentials became available.** A real bug was found and fixed early on: `provider_factory.build_execution_provider()` used to construct a live `WorkspaceClient()` eagerly for any Databricks profile, which crashed with an unrelated SDK auth error whenever no Databricks credentials were configured — even for `plane workload validate`, which never touches a client. Client construction is now lazy, built only inside `submit()`/`status()`/`cancel()` via OAuth M2M (`host`/`client_id`/`client_secret`), with credentials resolved through the same `credential_reference` pattern the storage providers already use. The Databricks Runtime <-> Spark version compatibility matrix was corrected against current public docs (Databricks Runtime 17.3 LTS ships Spark 4.0.0, 18.x ships Spark 4.1.0 — no runtime ships 4.2 yet), so `plane workload validate` no longer falsely reports a Spark-4.2 workload as Databricks-compatible. `plane workload validate` also gained a repeatable `--environment` flag for static, offline cross-provider portability checks in one command.

The static half of the hero demo: `plane workload validate examples/claims-normalization.yaml --environment k8s-vast-s3 --environment azure-dbx-v1` — the same workload, same artifact reference, no application changes — reports `PASS` against both a real Kubernetes+VAST environment and a Databricks+ADLS environment, in one command, over the real API.

The live half arrived once real OAuth M2M credentials for a real workspace became available — the first real Databricks authentication in this project's history. Three real, load-bearing findings came out of that run, each worked through to an actual fix rather than reported and left: (1) this workspace administratively forbids classic job clusters ("Only serverless compute is supported") — `DatabricksExecutionProvider` gained a `serverless` mode, submitting via a job-level `JobEnvironment` instead of `new_cluster`; (2) the example wheel's own declared `pyspark==4.2.0` pip dependency broke Serverless's library installer (no `--no-deps` equivalent) — moved to an optional dev-only dependency, since it was only ever needed for local testing; (3) Serverless's Spark Connect session rejects `spark.conf.get()` for any custom key outright, and the Jobs API's own `environment_variables_key` turned out not to actually reach a serverless task's process environment either (confirmed via a raw SDK call) — `PythonWheelTask.parameters`, read back via `sys.argv`, is what actually works, and `examples/wordcount_app`'s job code now falls back to it.

`examples/wordcount-dbx.yaml` — the same wheel, same code as `examples/wordcount.yaml`, only `runtime.spark` differs (4.1, not 4.2, the same precedent `claims-normalization.yaml` already established) — ran through the real `plane run` → API → reconciler → provider pipeline and reached `SUCCEEDED`, with real Parquet output independently confirmed in the target Unity Catalog volume. Storage is Databricks-native (Unity Catalog Volumes), not S3: the on-prem MinIO this project's S3 leg runs against isn't network-reachable from Databricks' cloud compute, a real topology constraint rather than a code gap. See [issue #8](../../issues/8) and [issue #12](../../issues/12) for the full trace of findings.

**v0.4 ("Conformance & Reliability") is complete.** Unlike v0.1-v0.3, nothing in this milestone's actual scope was blocked on missing credentials — it's reliability, security, and testing-framework work, not a live-Databricks demo. Retry semantics classify every provider error as retryable or terminal (`RetryableProviderError`/`TerminalProviderError`), covering `submit()`, `status()`, and `cancel()` alike; the reconciler claims runs via `SELECT ... FOR UPDATE SKIP LOCKED` before submitting, so concurrent reconciler replicas are HA-safe, and a stable per-run idempotency token flows into Databricks's `jobs.submit()` while Kubernetes recovers from a 409 by reading back the resource instead of failing a run that already succeeded. OIDC authentication (real JWT/JWKS verification) and a five-role RBAC hierarchy (`api/auth.py`) gate every mutating endpoint, opt-in via `PORTAGE_AUTH_MODE` so nothing changes by default. An identity-bearing audit trail (`AuditEvent`, spec §36) records every privileged action, exportable via `GET /v1/audit`. A shared provider contract test suite runs the same behavioral checks against every execution and storage provider — the automated, always-on form of "provider certification." A semantic result comparator (never byte-for-byte, spec §22) backs a dynamic conformance runner, `plane conformance test`, and a provider certification report, `plane conformance report`, matching spec §78's own compatibility-matrix format.

Three real bugs were found and fixed via live verification against the real remote infrastructure, not caught by any mocked unit test: a duplicate-submission crash window in the reconciler; a `MissingGreenlet` response-serialization bug from recording an audit event right before returning the same ORM object; and a retry-classification gap where `status()`/`cancel()` weren't covered by the submit-path fix, so a transient network blip while polling could fail a run that was actually fine. A live fault-injection test killed a real Spark driver pod mid-run and confirmed the reconciler correctly reported `FAILED`; a separate test killed and restarted the API process while a different run was `RUNNING` and confirmed zero impact, since the reconciler operates as a fully independent process.

**v0.5 ("Table & Query Integration") is complete.** Unlike v0.2/v0.3 (VAST/Databricks/Azure — genuinely inaccessible from here), Iceberg REST Catalog, Trino, and Kyuubi are all real, self-hostable open-source systems, so every piece of this milestone was live-verified end to end rather than left as a translation-layer prototype — including Kyuubi, which spec §68 marks explicitly optional. A `Dataset` binding can now be `kind: table` (spec §11) alongside the existing `kind: path`, resolving into `spark.portable.dataset.<name>.identifier` plus per-run Iceberg REST catalog registration (`spec/dataset/v1alpha1.py::resolve_iceberg_catalog_config()`) — a no-op for path-only workloads, so v0.1-v0.4 behavior is unchanged. `deploy/dev/iceberg-rest.yaml`/`trino.yaml`/`kyuubi.yaml` stand up the real catalog and both query engines (Kyuubi via a custom image, `providers/query/kyuubi/image/` — no official prebuilt Kyuubi+Spark image exists), composed behind the same portability boundary as every other provider (spec §38: "the framework still does not own the query engine"). See [`docs/providers/query.md`](docs/providers/query.md) for the JDBC connection strings and the real Iceberg/Spark/S3 compatibility gaps found live-verifying this — none of it worked on the first attempt, and each fix is a genuine, previously-undocumented-until-tried defect, not a guess.

Live-verified on the remote cluster: a real Iceberg table written by one Portage-submitted Spark job, read back by a second, completely independent Portage-submitted Spark job purely through the logical dataset name (not a hardcoded path), then queried by both Trino and Kyuubi via `beeline`/the `trino` CLI — four independent readers, one consistent table, through the shared REST catalog.

**v0.6 ("Ecosystem Integration") is complete.** Unlike v0.1-v0.5, this milestone's scope was deliberately unplanned until requested — spec §69 gates it on "only features demonstrated by actual users" — and was decomposed into exactly the four integrations asked for: webhooks + events API, API CRUD completeness, a Terraform provider, an Airflow provider, and Spark Declarative Pipelines support. Webhook delivery (`control_plane/webhooks.py`, `reconciler/service.py::deliver_webhooks()`) hooks into the single choke point every run state transition already flows through (`run_service.transition_run_state()`), HMAC-signing each POST; the events-query half of "webhooks, events API" turned out to already exist (`GET /v1/runs/{id}/events`). Every config resource (`environments`, `execution-profiles`, `storage-profiles`, `workloads`, `datasets`) now supports `PUT`/`DELETE`, with the database's existing `ondelete="RESTRICT"` foreign keys translated into a clean `409` instead of a raw `500`. `terraform/provider-portage/` (Go, `terraform-plugin-framework`) wraps that CRUD surface with four resources; `integrations/airflow/` (`PortageHook`/`PortageRunOperator`/`PortageRunSensor`) is a self-contained REST client with no dependency on the backend package, matching spec §39's "cross-system orchestrators call the public job API" boundary.

Two real, load-bearing findings came out of live verification rather than being predicted up front. Terraform's `provider` is a reserved resource-block meta-argument — a schema attribute with that exact name (matching the REST API's own JSON field) got misparsed by `terraform init` as a reference to an unrelated provider, fixed by renaming the Terraform-facing attribute to `provider_type`. And Spark Declarative Pipelines' `spark-pipelines run` command requires a Spark Connect session — bootstrapping one in-process inside a plain `spark-submit` driver (this project's current per-run `SparkApplication` model) hits a classloader wall even after pulling in the right Maven artifact, confirmed via direct reproduction on the real cluster and through 4 real `plane run` submissions that correctly reached `RUNNING` before failing at exactly that boundary. The spec-layer and Kubernetes-manifest translation for it are built and unit-tested; genuinely running one to `SUCCEEDED` needs a persistent Spark Connect server as its own deployed resource — out of scope for this issue, documented for future work.

Live-verified on the remote cluster: a real webhook subscription firing with a correct, verifiably-signed payload after a real run; real `PUT`/`DELETE` round trips against the API; a real `terraform init && apply && destroy` cycle creating and tearing down a real environment/profile; a real Airflow DAG running `PortageRunOperator` against the real API to `SUCCEEDED`; and, for Declarative Pipelines, the honest negative result described above.

**v1.0 ("GA") is complete.** Unlike prior milestones, this one was scoped directly from spec §71's 7 exit criteria rather than open feature requests — an audit pass against the actual codebase first (not assumed from memory of prior close-out claims) found 4 real gaps, decomposed into 4 issues. The workload/environment/dataset/artifact schemas are promoted from `v1alpha1` to stable `v1` (`docs/architecture/STABILITY.md`) — the deprecated name still parses with a warning, per a documented deprecation policy — and `docs/architecture/PROVIDER_SDK.md` documents the `ExecutionProvider`/`StorageProvider` Protocols as the stable third-party extension surface. `charts/portage` (new) is a real Helm chart for Portage's own control plane — 2+ API/reconciler replicas, a pre-install migration hook against bring-your-own PostgreSQL, and TLS terminated at an `Ingress` — filling the `charts/` gap left open since v0.1. `tests/chaos/` (new) turns v0.4's one-off manual chaos demonstration into a repeatable suite covering control-plane restart recovery, provider outage recovery, idempotent submission under real concurrency, and an HA-specific dual-replica-kill scenario — spec §57's "mandatory v1.0 acceptance test." `examples/claims_app` (new) implements `claims-normalization.yaml` for real — a genuine two-input join (raw claims joined against a provider reference table), not just a map/reduce — closing the "nontrivial workload" portability criterion on solid ground instead of stretching `wordcount`'s single-input map/reduce to fit.

The chaos suite found and fixed four real bugs, none hypothetical. The Kubernetes provider never caught `urllib3.exceptions.MaxRetryError` (only `ApiException`, which only wraps a response the server actually sent) — a genuine network partition to the provider was falling through to the reconciler's unclassified-exception handler and permanently failing the run instead of retrying; also added an explicit request timeout, since without one a call against an unreachable server could hang far longer than any retry budget before even raising. `poll_active_runs()` had no dedicated `TerminalProviderError` handler (unlike `submit_new_runs()`), mislabeling a deliberately classified error as "unclassified." A genuine race in idempotency-key creation could create two `Run` rows under real concurrent duplicate submissions and 500 the loser instead of cleanly deduplicating — fixed with an atomic `INSERT ... ON CONFLICT DO NOTHING`, since a plain insert-then-catch also hit a real SQLAlchemy asyncio/greenlet edge case on session reuse right after the conflict. And the HA-specific test surfaced an intermittent, genuine `401 Unauthorized` from the real Kubernetes API server under reconciler/API pod churn — investigated thoroughly (four plausible mechanisms ruled out live, including a 400-request concurrent-polling reproduction across two real pods) without a confirmed root cause, then mitigated with a principled, bounded, evidence-driven retry policy and verified empirically (4/4 clean chaos-suite runs afterward, versus roughly half failing with the same signature before).

`claims_app` is the actual "same workload, same artifact, no application changes, equivalent output on both providers" hero demo spec §66 always intended: the exact same workload file and artifact reference ran to `SUCCEEDED` on both a real Kubernetes environment and a real Databricks Serverless environment, with **byte-identical output** on both.

One exit criterion remains a documented, accepted gap rather than a silent one: VAST NFS-mode and ADLS storage stay translation-layer-only (tested against fakes, never real hardware) — no VAST hardware or Azure subscription is available to this project, the same class of constraint that blocked Databricks until real credentials arrived in v0.3. Every other spec §71 criterion — portable contract stability, production-tested execution on both providers, PySpark/JVM runtime, the full reliability suite, OIDC/RBAC/machine-identity/secrets/audit/TLS, and cross-provider portability of a genuinely nontrivial workload — is live-verified, not assumed.

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
| `terraform/provider-portage/` | Terraform provider (Go) wrapping the config-resource REST API |
| `integrations/airflow/` | Airflow provider/operator, self-contained REST client |
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
