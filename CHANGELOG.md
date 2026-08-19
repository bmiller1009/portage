# Changelog

All notable changes to Portage are documented in this file. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Detailed,
milestone-by-milestone development history (bugs found, live-verification
narratives) lives in [`docs/releases/`](docs/releases/), not here — this
file is the release-facing summary.

## [1.0.1] — 2026-08-19

Patch release. One real behavioral fix plus documentation/wording corrections — no new features, no architecture changes.

### Fixed

- **Certification cross-product bug** (`control_plane/certification.py`): `plane conformance report` could mark an execution+storage combination `PASS` just because each side was independently live-verified elsewhere, without that exact pair ever having been run together (e.g. Databricks + VAST-S3, never actually paired, was incorrectly reportable as `PASS`). Now tracks verified pairs explicitly (`_LIVE_VERIFIED_COMBINATIONS`) instead of computing a cross-product. No public API/CLI/UI contract change — only which combinations earn `PASS`.
- **`CERTIFIED` terminology**: tightened the definition in README.md and docs/verification/v1.0.0.md — it previously read as "the contract suite itself runs against real infrastructure," which is backwards for suites that are deliberately fake/mock-backed by design. Now requires both a passing contract/conformance suite **and** separate live-infrastructure evidence.
- **`tests/README.md`** table formatting: restructured dense single-paragraph cells with `<br>`-separated points for readability.
- **Verification record accuracy**: docs/verification/v1.0.0.md had accreted a contradictory tag-SHA reference from post-tag edits. Corrected the tag's actual target (`0c03415`) and froze that document to the tagged release's state — post-1.0.0 fixes (including this release) now live in [`docs/verification/v1.0.1.md`](docs/verification/v1.0.1.md).
- Byte-identical output wording and an infrastructure hostname were also cleaned up across several docs ahead of this release — see docs/verification/v1.0.1.md for the full list.

## [1.0.0] — 2026-08-19

First GA release. Portage's central claim — the same portable Spark
workload can execute across different infrastructure providers without
application-code changes — is live-verified end to end on real
infrastructure, not simulated.

### Portable workload contract

- `workload`/`environment`/`dataset`/`artifact` schemas promoted from
  `runtime/v1alpha1` to stable `runtime/v1` (same field shapes — a rename,
  not a redesign). The deprecated name still parses, with a warning, under
  a documented deprecation policy (`docs/architecture/STABILITY.md`).
- `ExecutionProvider`/`StorageProvider` are documented as the stable
  third-party extension surface (`docs/architecture/PROVIDER_SDK.md`).

### Execution

- **Kubernetes**: production-tested across every milestone — real
  `SparkApplication` submission via the Apache Spark Kubernetes Operator,
  JVM JAR and Python wheel artifacts, cancellation, capability matching.
- **Databricks**: real OAuth M2M-authenticated live runs against the
  Serverless compute path; JVM JAR and Python wheel artifacts, capability
  matching against a curated Databricks Runtime ↔ Spark version
  compatibility matrix. The classic-cluster submission path is
  implemented and contract-tested, but was never live-run against a real
  workspace — this project's own verification workspace administratively
  forbids classic job clusters ("Only serverless compute is supported").

### Storage

- **S3** and **VAST (S3-compatible mode)**: live-verified.
- **VAST (NFS mode)** and **ADLS**: translation-layer providers, verified
  against unit tests and fakes — no VAST hardware or Azure subscription is
  available to this project (see Known Limitations).

### Conformance

- A semantic (never byte-for-byte) result comparator — schema, row count,
  null counts, partition coverage, deterministic hashes — backs
  `plane conformance test` (dynamic, run-based) and
  `plane conformance report` (static, provider-certification-style).
  Certification is pair-specific — an execution+storage combination is
  only `PASS` if that exact pair was actually verified, never inferred
  from each side being independently verified elsewhere (see
  [README.md's Certified combinations](README.md#certified-combinations)).
- `examples/claims_app`: a genuine two-input join workload, the actual
  hero-demo proof — same workload file, same artifact reference,
  semantically equivalent output (the guarantee above) on both a real
  Kubernetes environment and a real Databricks Serverless environment;
  this run's output also happened to be byte-identical.

### HA / reconciliation

- The reconciler runs safely as 2+ concurrent replicas
  (`SELECT ... FOR UPDATE SKIP LOCKED`), retries provider errors classified
  as retryable vs terminal, and recovers cleanly from API/reconciler
  process restarts mid-run.
- `tests/chaos/` is a repeatable, live fault-injection suite (spec §57's
  "mandatory v1.0 acceptance test") covering control-plane restart
  recovery, provider outage recovery, idempotent submission under real
  concurrency, and an HA dual-replica-kill scenario.
- `charts/portage`: a real Helm chart for Portage's own control plane —
  2+ API/reconciler replicas, a pre-install PostgreSQL migration hook, and
  TLS terminated at an `Ingress`.

### Security

- OIDC authentication (JWT/JWKS verification) and a five-role RBAC
  hierarchy gate every mutating endpoint, opt-in via `PORTAGE_AUTH_MODE`.
- Machine identity via OAuth2 bearer/M2M; secret references
  (`credential_reference`) instead of inline credentials throughout.
- An identity-bearing audit trail records every privileged action
  (`GET /v1/audit`).

### REST API / CLI / UI

- Full CRUD (including `PUT`/`DELETE`) across every config resource
  (environments, execution/storage profiles, workloads, datasets).
- Webhooks (HMAC-signed) and an events API (`GET /v1/runs/{id}/events`).
- `plane` CLI is a pure REST client — no direct database or provider
  access.
- A minimal operational UI (`ui/`) — read access to runs, workloads,
  environments, datasets, providers, plus cancel/logs on a run.
- `terraform/provider-portage/` and `integrations/airflow/` wrap the
  public REST API for infrastructure-as-code and orchestration use cases.

### Known limitations

- VAST NFS-mode and ADLS storage remain translation-layer-only — tested
  against fakes, never real hardware/a real Azure subscription.
- Spark Declarative Pipelines: the spec-layer and Kubernetes-manifest
  translation are built and unit-tested, but running one to completion
  needs a persistent Spark Connect server as its own deployed resource —
  out of scope for this release, documented as future work.
- Third-party execution/storage providers require a core-code branch in
  `control_plane/provider_factory.py` today (contract-tested, stable
  interfaces — not a dynamic/hot-loadable plugin mechanism).
