# Portable Spark Runtime — Architecture & Development Specification

- **Status:** GA architecture baseline
- **Release:** Portage 1.0.0
- **Project name:** Portage
- **License recommendation:** Apache License 2.0
- **Primary delivery model:** Open source, self-hosted
- **Primary interfaces:** REST API, CLI, minimal operational UI
- **Core objective:** Execute the same portable Spark workload against multiple compute and storage environments without application rewrites.

The architecture is deliberately narrow: the workload portability contract is the product; everything else either implements that contract or is an integration. This keeps the project from drifting into building another lakehouse platform.

## 1. Executive Summary

Portable Spark Runtime is a deliberately small, open-source control plane for running Apache Spark workloads independently of the infrastructure on which those workloads execute.

The fundamental abstraction is a **Portable Workload Contract**.

A workload describes:

- application artifact
- entry point
- Spark runtime requirements
- logical input and output datasets
- resource requirements
- execution policy
- identity requirements
- arguments and configuration

It does **not** directly describe:

- Kubernetes pods
- Databricks clusters
- VAST mount paths
- S3 buckets
- ADLS containers
- Kubernetes service accounts
- Databricks Jobs API payloads

Those concerns are resolved by environment-specific providers.

Conceptually:

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

The target user is an organization that:

- runs serious Spark workloads;
- owns Kubernetes and/or substantial storage infrastructure;
- does not want to adopt an entire Hadoop/lakehouse platform merely to run Spark;
- wants the option to use Databricks without making Databricks the application architecture;
- wants storage and compute separated;
- values open standards and infrastructure portability.

The framework is not intended to compete feature-for-feature with Databricks, Cloudera, IOMETE, Stackable, Airflow, Trino, Iceberg, or Kubernetes.

Instead, it composes those technologies behind a stable portability boundary.

## 2. Problem Statement

Modern Spark applications are frequently coupled to their execution environment through assumptions such as:

```
hdfs://...
s3a://...
abfss://...
/vast/...

dbutils.*
Databricks-specific job constructs
YARN configuration
Kubernetes Spark CRDs
cloud-specific credentials
provider-specific catalog names
```

This creates substantial migration cost.

A Spark application that is logically:

```
read claims
normalize claims
write normalized claims
```

becomes physically encoded as:

```
read hdfs://prod-cloudera-01/data/claims
...
write hdfs://prod-cloudera-01/warehouse/claims_normalized
```

or:

```
read abfss://claims@prodaccount...
```

The application has therefore become coupled to infrastructure.

Portable Spark Runtime introduces a layer between application intent and infrastructure implementation.

The desired experience is:

```
plane run claims-normalization --environment onprem-prod
```

which resolves to:

```
Apache Spark 4.x
Kubernetes
VAST
```

while:

```
plane run claims-normalization --environment azure-prod
```

resolves to:

```
Databricks
ADLS
```

without changing the application artifact or logical workload definition.

## 3. Product Thesis

The product is not a Spark platform.

The product is:

> An open workload portability contract for distributed Spark applications, plus a reference control plane implementing that contract.

The primary invariant is:

> A conforming workload MUST NOT belong to the infrastructure on which it happens to execute.

Everything in the architecture should be evaluated against that statement.

## 4. Design Principles

### 4.1 Portability First

Portable behavior takes precedence over provider convenience.

A feature supported only by one execution provider may still be exposed, but MUST be explicitly identified as provider-specific.

Provider-specific configuration must never silently become part of the portable specification.

### 4.2 Upstream First

Do not rebuild infrastructure already owned by healthy upstream projects.

Examples:

| Concern | Upstream owner |
|---|---|
| Spark execution | Apache Spark |
| Spark on Kubernetes | Apache Spark Kubernetes Operator |
| distributed SQL | Trino/Kyuubi integration |
| table format | Apache Iceberg |
| Kubernetes orchestration | Kubernetes |
| authorization policy | OPA / engine-native policy |
| identity | OIDC/OAuth |
| metrics | OpenTelemetry/Prometheus |

The official Apache Spark Kubernetes Operator is now an Apache Spark subproject. Its current 1.0.0 release supports Spark 4.0–4.2 and Kubernetes 1.34–1.36. (Apache GitHub)

That should be the Kubernetes execution primitive rather than implementing proprietary Spark pod management.

### 4.3 API First

Every platform capability must be available through a documented API.

The UI is a client of the API.

There must be no privileged UI-only backend.

Expected interaction priority:

1. REST
2. CLI
3. SDK
4. UI

### 4.4 Thin Control Plane

The control plane coordinates workloads.

It must not become:

- a data warehouse;
- a distributed scheduler;
- a secret vault;
- an identity provider;
- an object store;
- a notebook environment;
- an ETL designer;
- a proprietary catalog;
- a query optimizer.

### 4.5 Failure Isolation

Control-plane failure must not terminate running Spark applications.

Once successfully submitted, the underlying execution provider owns execution.

If the control plane becomes unavailable:

```
Spark job continues
        │
        ▼
control plane recovers
        │
        ▼
reconciler rediscovers job
        │
        ▼
state converges
```

### 4.6 Semantic Portability, Not Fictional Equivalence

Kubernetes Spark and Databricks Runtime will never be completely identical.

For example, Databricks Runtime 19 is currently powered by Apache Spark 4.2.0, while Databricks Runtime 18 LTS uses Spark 4.1.0. Databricks layers additional behavior and libraries on those runtimes. (Databricks Documentation)

Therefore portability means:

> The workload produces semantically equivalent results under supported execution environments.

It does not mean:

> The runtimes are binary-identical.

## 5. Explicit Non-Goals

These should be published prominently in the README.

Portable Spark Runtime will not initially build:

- Notebook environment
- BI/dashboard platform
- Visual ETL designer
- General workflow orchestrator
- Distributed SQL engine
- Distributed filesystem
- Object store
- Data warehouse
- ML platform
- Model registry
- Streaming platform
- Kafka distribution
- Secret manager
- Identity provider
- Data marketplace
- Enterprise catalog UI
- Lineage engine
- Vector database
- AI agent framework

Projects that solve these problems should integrate with the runtime. They should not be absorbed by it.

## 6. High-Level Architecture

```
 ┌────────────────────────────────────────────────────────────────┐
 │                         CLIENTS                                │
 │                                                                │
 │        CLI        REST SDK        CI/CD        Minimal UI      │
 └──────────────────────────────┬─────────────────────────────────┘
                                 │
                                 ▼
 ┌────────────────────────────────────────────────────────────────┐
 │                      CONTROL PLANE                             │
 │                                                                │
 │  API Gateway / REST API                                       │
 │                                                                │
 │  Authentication                                               │
 │  Authorization                                                │
 │  Workload validation                                          │
 │  Environment resolution                                       │
 │  Dataset resolution                                           │
 │  Provider registry                                            │
 │  Run lifecycle                                                │
 │  Audit                                                        │
 │  Reconciliation                                                │
 │  Portability/conformance                                      │
 │                                                                │
 └────────────────┬──────────────────────┬────────────────────────┘
                   │                      │
                   ▼                      ▼
         EXECUTION PROVIDERS        DATA PROVIDERS
                   │                      │
         ┌─────────┴─────────┐    ┌───────┼───────┐
         │                   │    │       │       │
   Kubernetes/Spark     Databricks VAST   S3     ADLS
         │                   │
         ▼                   ▼
   Spark K8s Operator    Jobs API
         │
         ▼
   Apache Spark

                 OPTIONAL INTEGRATION PLANE

         ┌──────────────────┬──────────────────┬────────────────┐
         │                  │                  │                │
        Trino             Kyuubi            Iceberg            OPA
         │                  │                  │
         └──────────────────┴──────────────────┴────────────────┘
```

## 7. Core Domain Model

The system should have six primary concepts.

### 7.1 Workload

A logical Spark application definition.

Example:

```yaml
apiVersion: runtime/v1alpha1
kind: SparkWorkload

metadata:
  name: claims-normalization
  version: "4.18.2"

runtime:
  spark: "4.2"

application:
  type: python-wheel
  artifact: artifact://claims-normalization/4.18.2
  entryPoint: claims.jobs.normalize

arguments:
  - "--business-date"
  - "${RUN_DATE}"

datasets:
  inputs:
    claims:
      dataset: claims.raw

  outputs:
    normalized:
      dataset: claims.normalized

resources:
  driver:
    cores: 2
    memory: 8Gi

  executor:
    cores: 4
    memory: 16Gi

  scaling:
    minExecutors: 10
    maxExecutors: 100

execution:
  timeout: 4h
  retries: 0
```

This document is portable. It contains no Kubernetes or Databricks objects.

## 8. Environment Profile

Infrastructure-specific behavior is represented by an Environment.

Example:

```yaml
kind: Environment

metadata:
  name: onprem-prod

execution:
  provider: kubernetes
  profile: spark-prod

data:
  provider: vast
  profile: vast-prod

catalog:
  provider: iceberg-rest
  profile: catalog-prod

identity:
  provider: kubernetes-workload
```

Azure:

```yaml
kind: Environment

metadata:
  name: azure-prod

execution:
  provider: databricks
  profile: databricks-prod

data:
  provider: adls
  profile: azure-prod

catalog:
  provider: iceberg-rest
  profile: catalog-prod
```

Application developers should normally select only:

```
workload + environment
```

## 9. Logical Dataset Model

Storage portability requires separating logical data identity from physical location.

Applications should refer to:

```
claims.raw
claims.normalized
provider.reference
eligibility.monthly
```

rather than:

```
/vast/prod/claims/raw
s3a://prod-data/claims/raw
abfss://claims@prod...
```

Dataset binding:

```yaml
kind: Dataset

metadata:
  name: claims.raw

bindings:

  onprem-prod:
    kind: path
    uri: /vast/data/claims/raw

  aws-prod:
    kind: path
    uri: s3a://company-prod/claims/raw

  azure-prod:
    kind: path
    uri: abfss://claims@companyprod/claims/raw
```

The resolver injects the environment-specific location at runtime.

## 10. Avoid Requiring a Runtime SDK

A critical design choice:

> Portable Spark applications should not be forced to import a proprietary framework library.

Instead, dataset mappings should be injected as Spark configuration:

```
portable.dataset.claims.raw.uri
portable.dataset.claims.normalized.uri
```

Application code can therefore use:

```python
claims_uri = spark.conf.get(
    "portable.dataset.claims.raw.uri"
)

df = spark.read.parquet(claims_uri)
```

A convenience SDK may offer:

```python
datasets.uri("claims.raw")
```

but this must remain optional.

The raw configuration contract should be stable and public. This prevents lock-in to the runtime itself.

## 11. Table-Level Portability

File paths alone are insufficient for modern analytical tables.

The framework should therefore distinguish:

```
PATH DATASET
TABLE DATASET
```

Example table binding:

```yaml
metadata:
  name: claims.normalized

kind: table

bindings:

  onprem-prod:
    identifier: analytics.claims.normalized

  azure-prod:
    identifier: analytics.claims.normalized
```

Apache Iceberg is the preferred neutral table format because Spark and Trino can both operate against it and Iceberg has a standardized REST Catalog API. (Apache Iceberg)

The framework should support Iceberg strongly. It should not require Iceberg for every workload. Plain Parquet/ORC remains valid.

## 12. Data Movement Is Explicitly Out of Scope

This distinction is critical.

Portable Spark Runtime maps:

```
claims.raw

ONPREM → VAST location
AZURE  → ADLS location
AWS    → S3 location
```

It does not guarantee the physical bytes already exist in each location.

Therefore:

```
dataset mapping ≠ data replication
```

Bulk movement such as:

```
HDFS → VAST
VAST → S3
S3 → ADLS
```

is a migration/data-management concern.

Migration utilities may eventually be supplied as companion tooling, but automatic cross-environment replication must not become an implicit feature of workload execution.

## 13. Execution Provider Interface

All execution providers implement the same logical interface.

Conceptually:

```python
class ExecutionProvider(Protocol):

    async def validate(
        self,
        workload: ResolvedWorkload
    ) -> ValidationResult:
        ...

    async def submit(
        self,
        run: RunRequest
    ) -> ProviderRun:
        ...

    async def status(
        self,
        provider_run_id: str
    ) -> ProviderStatus:
        ...

    async def cancel(
        self,
        provider_run_id: str
    ) -> None:
        ...

    async def logs(
        self,
        provider_run_id: str
    ) -> LogReference:
        ...

    async def capabilities(
        self
    ) -> CapabilitySet:
        ...
```

Provider APIs must be capability-driven rather than relying on `if provider == ...` logic throughout the codebase.

## 14. Kubernetes Execution Provider

The Kubernetes provider translates a portable workload into an Apache Spark Kubernetes Operator resource.

Current upstream operator 1.0.0 supports Spark 4.0, 4.1 and 4.2 and Kubernetes 1.34–1.36. (GitHub)

Flow:

```
PortableWorkload
       │
       ▼
resolver
       │
       ▼
KubernetesExecutionProvider
       │
       ▼
SparkApplication / SparkApp
       │
       ▼
Apache Spark K8s Operator
       │
       ▼
driver + executor pods
```

The framework must not implement its own Spark scheduler.

## 15. Kubernetes Storage Behavior

For VAST:

```
DURABLE DATA       → VAST
SHUFFLE / SPILL    → node-local ephemeral NVMe
```

VAST currently provides Kubernetes CSI support including NFSv3 and NFSv4 access. (Vast Data Knowledge Base)

Do not automatically use networked persistent storage for Spark local shuffle.

Default:

```
spark.local.dir → local ephemeral volume
```

Persistent/network shuffle must be opt-in and explicitly justified.

## 16. Databricks Execution Provider

The Databricks provider translates the same workload into a Databricks Jobs API submission.

Databricks exposes a one-time workload submission endpoint specifically for running workloads without first creating persistent job definitions. (Databricks Documentation)

The provider should target the currently recommended Jobs API generation; Databricks currently recommends Jobs API 2.2 for new clients. (Databricks Documentation)

Preferred mappings:

```
Python workload → Python wheel task
JVM workload    → JAR task
```

Python wheel execution is a first-class Databricks job task today. (Databricks Documentation)

Avoid notebooks as the canonical portable workload artifact. Avoid legacy `spark_submit_task` as the standard translation.

## 17. Application Artifact Strategy

Portable artifacts must not assume container portability.

A Kubernetes container image does not map naturally to every Databricks execution mode.

Therefore separate:

```
APPLICATION ARTIFACT

from

EXECUTION RUNTIME IMAGE
```

Supported v1 application artifacts:

```
Python
  Python wheel
  + declared entry point

JVM
  JAR
  + main class
```

Kubernetes may execute those artifacts inside a framework-managed Spark base image. Databricks attaches the same logical artifact through its supported library/task mechanisms. Databricks currently supports custom Python, Java and Scala libraries, including wheels and JARs. (Databricks Documentation)

## 18. Runtime Profiles

Do not expose arbitrary Spark configuration as the normal user experience.

Define reusable Runtime Profiles:

```yaml
runtimeProfiles:

  standard:
    spark: "4.2"

  high-memory:
    spark: "4.2"

  gpu:
    spark: "4.2"
```

Provider translation can determine the actual infrastructure. For example:

```
high-memory

Kubernetes:
  nodeSelector = memory-optimized

Databricks:
  node type = appropriate memory SKU
```

## 19. Provider-Specific Overrides

Advanced infrastructure always requires escape hatches.

Allow:

```yaml
providerOverrides:
  kubernetes:
    ...
```

or:

```yaml
providerOverrides:
  databricks:
    ...
```

However, any provider override MUST:

1. be namespaced;
2. be ignored by unrelated providers;
3. cause portability validation to report degraded portability;
4. never silently alter the portable contract.

Example:

```
PORTABILITY STATUS

Core workload        PASS
Dataset mappings     PASS
Runtime              PASS

Provider overrides:

kubernetes           2
databricks           0

Overall:

PORTABLE WITH PROVIDER-SPECIFIC OVERRIDES
```

## 20. Provider Capability Model

Every execution provider publishes capabilities:

```json
{
  "sparkVersions": ["4.1", "4.2"],
  "languages": ["python", "jvm"],
  "dynamicAllocation": true,
  "gpu": true,
  "streaming": true,
  "localDisk": true,
  "sparkConnect": true
}
```

Validation happens before submission. Unsupported workloads must fail fast. Do not submit something known to be incompatible and wait for Spark to explode.

## 21. Portability Conformance

This is one of the core differentiators.

The system should provide:

```
plane validate workload.yaml
```

and eventually:

```
plane conformance test \
  claims-normalization \
  --environment onprem-prod \
  --environment azure-prod
```

Static validation should evaluate:

- Spark API compatibility
- artifact compatibility
- dataset bindings
- runtime capabilities
- provider overrides
- credential availability
- catalog compatibility
- resource requirements
- unsupported functionality

Dynamic conformance executes the same workload across multiple providers.

## 22. Result Comparison

Cross-provider tests must compare semantic output, not physical files.

Do not expect:

```
Parquet file A == Parquet file B
```

because file layout, partition sizing, compression and metadata may legitimately differ.

Instead compare:

- schema
- row count
- null distribution
- partition coverage
- key aggregates
- business invariants
- deterministic row hashes
- table snapshots where appropriate

For very large datasets, use partition-level validation rather than requiring full binary comparison.

## 23. Run Lifecycle

Canonical states:

```
ACCEPTED
VALIDATING
SUBMITTING
QUEUED
RUNNING
SUCCEEDED
FAILED
CANCELING
CANCELED
UNKNOWN
LOST
```

Provider states are mapped into this canonical model. Never leak dozens of provider-specific lifecycle states into client code. Provider-native status should remain available as diagnostic metadata.

## 24. Reconciliation Architecture

Submission should be asynchronous.

```
POST /v1/runs

        │
        ▼

persist ACCEPTED
        │
        ▼
HTTP 202 + run_id

        │
        ▼

reconciliation worker
        │
        ▼
validate
        │
        ▼
submit provider workload
        │
        ▼
poll/watch provider
        │
        ▼
update canonical state
```

Benefits:

- API latency does not depend on Databricks/Kubernetes latency;
- provider outages do not block API threads;
- retries become controlled;
- restart recovery is straightforward.

## 25. Idempotency

Submission API must accept an idempotency key:

```
Idempotency-Key: 89283b...
```

The same request with the same key must never accidentally create duplicate runs.

This is especially important for:

- CI/CD
- scheduled execution
- network retries
- automation agents
- pipeline systems

## 26. Retry Semantics

The runtime may safely retry:

- provider API timeout
- HTTP 429
- temporary HTTP 5xx
- Kubernetes API transient failure
- control-plane interruption before confirmed submission

The runtime must not silently rerun an application after confirmed execution failure unless the workload explicitly requests retries.

Application execution can have side effects. Therefore:

```
submission retry ≠ workload retry
```

These concepts must remain separate.

## 27. Metadata Persistence

Recommended persistence: **PostgreSQL**.

No Kafka. No Redis in the initial architecture. No distributed cache unless real load proves one necessary.

Core tables:

```
environments
execution_profiles
storage_profiles
dataset_bindings
workload_definitions
runs
run_events
provider_runs
audit_events
idempotency_keys
runtime_profiles
```

PostgreSQL remains authoritative for control-plane metadata. The actual execution system remains authoritative for running workload state. The reconciler converges the two.

## 28. Logs

Do not put complete Spark logs into PostgreSQL.

The control plane stores:

- run_id
- provider_run_id
- log reference
- failure summary
- timestamps
- diagnostic metadata

Logs remain in appropriate logging infrastructure. Kubernetes environments may use: Loki, Elastic, Splunk, cloud logging. Spark event logs should be written to durable storage and remain usable by Spark History Server. Databricks logs remain provider-managed. The UI presents a normalized diagnostic view and links/streams deeper logs as authorized.

## 29. Observability

OpenTelemetry should be the primary telemetry standard.

Every action should correlate:

- request_id
- run_id
- workload_id
- provider_run_id
- environment

Metrics should include:

- runs submitted
- runs succeeded
- runs failed
- submission latency
- queue latency
- execution duration
- provider errors
- reconciliation lag
- API error rates
- storage-resolution failures
- portability-validation failures

The runtime should export rather than become a dashboard product.

## 30. REST API

Initial API:

```
POST   /v1/runs
GET    /v1/runs/{id}
DELETE /v1/runs/{id}

GET    /v1/runs
GET    /v1/runs/{id}/events
GET    /v1/runs/{id}/logs

POST   /v1/validate
POST   /v1/conformance

GET    /v1/workloads
POST   /v1/workloads

GET    /v1/environments
GET    /v1/environments/{name}

GET    /v1/datasets
GET    /v1/datasets/{name}

GET    /v1/providers
GET    /v1/providers/{name}/capabilities

GET    /health
GET    /ready
GET    /metrics
```

OpenAPI documentation must be generated and committed as a release artifact.

## 31. CLI

Placeholder executable: `plane`.

Core commands:

```
plane run
plane status
plane cancel
plane logs

plane workload validate
plane workload inspect

plane dataset list
plane dataset resolve

plane environment list
plane environment inspect

plane provider list

plane conformance test
```

The CLI must use the public REST API. No direct database access. No hidden Kubernetes access.

## 32. Minimal UI

The UI is operational, not an authoring studio.

Primary pages:

```
Runs
Workloads
Environments
Datasets
Providers
Conformance
System
```

Run view:

```
claims-normalization
──────────────────────────────────

Status        RUNNING
Environment   onprem-prod
Execution     Kubernetes
Storage       VAST
Spark         4.2
Started       02:14:17
Duration      18m 42s

Driver        healthy
Executors     42 / 42
Retries       0

Inputs
claims.raw → VAST

Outputs
claims.normalized → VAST

Events →
Logs →
Cancel →
```

Do not build: drag/drop DAGs, notebook editor, large SQL IDE, BI visualization, workflow designer.

## 33. Security Architecture

Security should be delegated to established enterprise identity systems.

Human authentication:

```
OIDC
   │
Entra / Okta / Ping / etc.
   │
   ▼
control plane
```

Authorization:

```
external identity/groups
   │
   ▼
small internal RBAC model
```

Initial roles:

```
PlatformAdmin
Operator
Developer
Analyst
Viewer
```

Kubernetes already provides ServiceAccounts as workload identities and RBAC as its authorization model. (Kubernetes)

## 34. Machine Authentication

Machine identity must be distinct from human identity.

Support:

- OAuth2 client credentials
- OIDC federation
- Kubernetes ServiceAccounts
- cloud workload identities
- Databricks service principals

Databricks currently supports short-lived OAuth machine-to-machine authentication for service principals and recommends federation where appropriate. (Databricks Documentation)

Avoid personal-access tokens as the architectural default.

## 35. Secrets

Never create a proprietary secret vault.

Configuration stores references such as:

```yaml
credential:
  provider: azure-key-vault
  reference: analytics/prod/storage
```

Potential providers: Kubernetes Secrets, Azure Key Vault, AWS Secrets Manager, HashiCorp Vault.

Secrets must:

- never appear in persisted run specifications;
- never appear in application logs;
- never be returned through API responses;
- be resolved only when required;
- preferably use short-lived workload credentials rather than static secrets.

## 36. Audit

Every privileged operation should produce an audit event:

- identity
- timestamp
- action
- resource
- environment
- workload/run
- result
- source
- correlation id

Example:

```
brad@example.com
2026-08-15T16:24:18Z
RUN_CANCEL
claims-normalization/8fa22
onprem-prod
SUCCESS
```

Audit should be exportable to SIEM systems. Do not build a SIEM.

## 37. Multi-Tenancy

Initial releases should target: one organization with multiple teams/environments. Not: thousands of mutually untrusted SaaS tenants.

Kubernetes execution can isolate workloads using namespaces. Example:

```
spark-dev
spark-test
spark-prod
```

Hard multi-tenant SaaS isolation can be addressed later if commercial adoption warrants it. Do not burden v0.x with unnecessary tenancy complexity.

## 38. Query Plane

A federated SQL/query plane is useful but not part of core v0.1.

Preferred integration: Trino + Kyuubi where gateway functionality is useful.

Trino exposes connectors for numerous external systems and includes first-class Iceberg support. (Trino)

Architecture:

```
JDBC / SQL
     │
     ▼
   Trino
     │
 ┌───┼──────────────┐
 │   │              │
VAST S3/ADLS   PostgreSQL/etc.
```

Do not write a custom SQL engine.

## 39. Pipeline Strategy

Do not build a general-purpose Airflow replacement.

For Spark-native data pipelines, evaluate and integrate Apache Spark Declarative Pipelines. Spark 4.2 currently includes Spark Declarative Pipelines as a framework for reliable and testable batch and streaming pipelines. (Apache Spark)

For cross-system orchestration: Airflow, Kestra, Dagster, Argo, etc. should call the public job API.

The runtime MAY eventually provide very small primitives such as: run groups, dependency metadata, webhooks/events — but should not become the owner of general workflow semantics.

## 40. AI Strategy

AI is not core infrastructure.

Future AI functionality should operate strictly as an API client.

Possible tools:

```
catalog.search
dataset.describe
run.status
run.events
run.logs
query.execute
```

Good examples:

- "What tables contain pharmacy claims?"
- "Why did claims-normalization fail?"
- "Why did today's run take twice as long?"
- "Compare claim volume month over month."

AI must inherit the requesting user's permissions. It receives no privileged storage credentials.

No model hosting. No vector database. No proprietary agent framework. No embedding platform.

## 41. Recommended Technology Stack

**Control Plane**

- Python 3.12+
- FastAPI
- Pydantic
- SQLAlchemy
- asyncpg
- Alembic

Reasoning: control-plane throughput is modest; async network orchestration dominates CPU requirements; Python accelerates provider development; excellent Kubernetes/cloud SDK ecosystem; OpenAPI generation is straightforward. A future rewrite into Go is unnecessary unless operational evidence justifies it.

**Database**

- PostgreSQL 16+
- No additional persistence technology for v0.x.

**Kubernetes Integration**

- official Kubernetes Python client
- Apache Spark Kubernetes Operator
- Helm 3

**Databricks Integration**

- official Databricks SDK
- Jobs API 2.2
- OAuth M2M/federation

**CLI**

- Python
- Typer or equivalent
- HTTP client against public REST API

**UI**

- React
- TypeScript
- Vite
- TanStack Query
- Keep dependencies modest.

**Telemetry**

- OpenTelemetry
- Prometheus-compatible metrics
- structured JSON logging

**Test Stack**

- pytest
- pytest-asyncio
- mypy/pyright
- ruff
- kind
- Docker
- Helm

## 42. Repository Layout

Recommended monorepo:

```
/
├── api/
│
├── control_plane/
│
├── reconciler/
│
├── providers/
│   ├── execution/
│   │   ├── kubernetes/
│   │   └── databricks/
│   │
│   ├── storage/
│   │   ├── vast/
│   │   ├── s3/
│   │   └── adls/
│   │
│   └── identity/
│
├── spec/
│   ├── workload/
│   ├── environment/
│   └── dataset/
│
├── conformance/
│
├── cli/
│
├── ui/
│
├── charts/
│
├── sdk/
│   └── python/
│
├── examples/
│
├── tests/
│   ├── unit/
│   ├── contract/
│   ├── integration/
│   ├── e2e/
│   ├── security/
│   └── chaos/
│
├── docs/
│   ├── architecture/
│   ├── adr/
│   └── providers/
│
└── README.md
```

## 43. Provider SDK

Provider architecture needs explicit interfaces from the beginning.

Each provider should have:

- manifest
- capability declaration
- configuration schema
- validation implementation
- translation implementation
- health check
- contract tests
- documentation

A provider cannot be considered supported simply because it compiles. It must pass the provider conformance suite.

As of v1.0, this interface is documented concretely in
[PROVIDER_SDK.md](PROVIDER_SDK.md) against the two real `ExecutionProvider`/
`StorageProvider` implementations (Kubernetes/Databricks, S3/VAST/ADLS) —
see also [STABILITY.md](STABILITY.md) for the workload/environment/dataset
schema stability policy.

## 44. Plugin Strategy

Do not implement unrestricted arbitrary code loading into the control plane in v0.x. Official providers live in-tree initially.

Later:

```
external provider process
        │
        ▼
stable provider protocol
```

can permit third-party providers without dependency conflicts or arbitrary code execution inside the control plane.

Do not prematurely design a complex plugin runtime.

## 45. Development Environment

Developer setup should be exceptionally easy.

Target:

```
git clone ...

make dev
```

should establish:

- kind Kubernetes cluster
- PostgreSQL
- MinIO
- Spark Operator
- control plane
- example Spark runtime

Then:

```
plane run examples/wordcount.yaml \
    --environment local
```

should succeed.

A developer should not need: AWS account, Azure subscription, Databricks, VAST, enterprise OIDC — to contribute.

## 46. Production Operating Environment

**Control Plane**

Recommended:

- Kubernetes
- 2+ API replicas
- 2+ reconciler replicas
- external/HA PostgreSQL
- Ingress/load balancer
- TLS
- OIDC
- OpenTelemetry exporter

The control plane itself should remain container-portable.

**Kubernetes Spark Target**

Initial certified target:

- Kubernetes 1.34+
- Apache Spark Kubernetes Operator 1.0+
- Apache Spark 4.2

The current operator requirements explicitly call for Spark 4.0+ and Kubernetes 1.34+. (Apache GitHub)

**Databricks Target**

Initial conformance target:

- Databricks Runtime 19
- Spark 4.2 compatibility class
- Jobs API 2.2

Databricks Runtime 19 is currently based on Apache Spark 4.2.0. (Databricks Documentation)

A later LTS compatibility profile should target Databricks Runtime 18 LTS/Spark 4.1 where customers prioritize long support windows. (Databricks Documentation)

## 47. Storage Provider Requirements

Every storage provider must implement:

- configuration validation
- URI/path resolution
- credential resolution
- health/connectivity checks
- Spark configuration generation
- provider diagnostics
- capability declaration

Storage providers should not attempt to emulate identical filesystem semantics.

## 48. VAST Provider

Support two conceptual access modes where appropriate: VAST NFS, VAST S3.

VAST currently supports Kubernetes CSI-backed NFS access, while VAST also exposes S3 functionality. (Vast Data Knowledge Base)

The provider configuration should explicitly specify protocol. Example:

```yaml
type: vast

protocol: nfs

mount:
  path: /vast
```

Do not pretend NFS and S3 are equivalent internally.

## 49. S3 Provider

Responsibilities:

- URI resolution
- credential generation/federation
- Spark Hadoop S3 configuration
- endpoint configuration
- path-style options where necessary
- connectivity tests

Must support: AWS S3, and should eventually support compatible implementations through configuration. Do not make every S3-compatible vendor a separate core provider unless behavior genuinely differs.

## 50. Azure Provider

Preferred logical storage target: ADLS / ABFS.

Responsibilities mirror S3: logical mapping, identity, filesystem/account configuration, Spark connector configuration, health.

Workload identity should be preferred to static storage keys.

## 51. Artifact Repository

Artifacts should use a separate abstraction from datasets. Example:

```
artifact://claims-normalization/4.18.2
```

Provider/environment resolution might yield: S3 object, ADLS object, internal artifact repository.

This prevents workload definitions from embedding deployment infrastructure.

## 52. Testing Strategy

Testing is central to the product because portability itself is a claim that must be proven.

Testing should exist in six layers.

### 52.1 Unit Tests

Test: workload parsing, schema validation, dataset resolution, environment resolution, capability matching, state transitions, provider mappings, RBAC, configuration merging, retry decisions.

Target: fast, isolated, no external infrastructure.

### 52.2 Provider Contract Tests

Every ExecutionProvider runs the same suite. Example: submit valid workload, reject invalid workload, discover status, cancel workload, handle missing workload, handle provider timeout, handle duplicate submission, return capabilities, recover after control-plane restart.

Every StorageProvider similarly runs: resolve logical dataset, reject invalid mapping, resolve identity, health check, generate Spark configuration, never expose secrets.

## 53. Integration Tests

CI should create: kind + Spark Kubernetes Operator + MinIO + PostgreSQL. Run real Spark applications.

Tests should include: PySpark wheel, JVM JAR, input/output storage, dynamic allocation, failure, cancel, large logs, invalid Spark configuration.

Mocks are insufficient for core integration behavior.

## 54. Cloud Integration Tests

Emulators are useful but cannot certify cloud compatibility.

Nightly/periodic tests should run against real: AWS S3, Azure storage, Databricks workspace. VAST conformance requires access to an actual VAST environment.

Official certification should always distinguish: EMULATED, TESTED, CERTIFIED.

## 55. End-to-End Portability Tests

Canonical fixture workload:

```
input table
     │
     ▼
Spark transformation
     │
     ▼
output table
```

Run on:

```
Kubernetes + S3
Kubernetes + VAST
Kubernetes + ADLS
Databricks + S3
Databricks + ADLS
```

where infrastructure is available. Compare semantic results.

This should become the project's most important CI artifact.

## 56. Failure Injection

The framework exists partly because distributed systems fail.

Therefore deliberately test: kill Spark executor, kill Spark driver, kill Kubernetes worker, restart control-plane API, restart reconciler, restart PostgreSQL connection, expire credentials, inject storage latency, Kubernetes API 429, Kubernetes API 5xx, Databricks API 429, Databricks API timeout, network interruption during submission, network interruption after submission.

The desired behavior must be defined for every scenario.

## 57. Control Plane Recovery Test

Critical scenario:

```
submit Spark job
       │
       ▼
job RUNNING
       │
       ▼
destroy entire control plane
       │
       ▼
Spark job continues
       │
       ▼
restore control plane
       │
       ▼
reconciliation discovers run
       │
       ▼
correct final state
```

This is a mandatory v1.0 acceptance test.

## 58. Data Correctness Tests

Portability conformance fixtures must include: primitive types, timestamps, decimals, nulls, nested structs, arrays/maps, partitioned data, schema evolution, large strings, Unicode, timezone-sensitive data.

This is particularly important across upstream Spark and Databricks.

## 59. Security Tests

Test at minimum: unauthenticated API access, expired JWT, invalid issuer, wrong audience, role escalation, cross-project access, secret leakage, credential logging, malicious artifact URI, path traversal, configuration injection, Spark-conf injection, SSRF, malformed provider payload, audit completeness.

Dependency scanning and container scanning should run on every release. Produce an SBOM. Sign release container images.

## 60. Performance Tests

Control-plane performance is not Spark performance. Test them separately.

Control-plane benchmarks should include: concurrent submissions, large run history, provider polling/reconciliation, API read traffic, concurrent log requests.

Initial engineering target: 1,000 concurrently tracked runs, 100 near-simultaneous submissions, multiple environments.

These are design/test targets, not marketing guarantees.

## 61. Spark Scale Tests

Separately test: executor counts, driver pressure, Kubernetes API pressure, dynamic allocation, large shuffle, multi-TB reads, concurrent workloads.

Do not attempt to simulate all serious Spark performance in public CI. Maintain a dedicated scale-test profile.

## 62. Release Philosophy

Use semantic versioning for implementation: `0.1.0`, `0.2.0`, ... `1.0.0`.

Version the workload specification independently: `v1alpha1`, `v1beta1`, `v1`.

Software upgrades must never silently change workload semantics.

## 63. Phase 0 — Architectural Spike

**Goal**

Prove the underlying premise before building a product.

**Build**

- Portable workload schema
- Dataset mapping
- Kubernetes provider
- Databricks provider prototype
- S3 mapping
- CLI prototype

**Demonstration**

Execute one identical PySpark artifact:

```
Kubernetes/Spark/S3
        │
        AND
        │
Databricks/S3
```

**Exit Criteria**

- same artifact
- same arguments
- same logical dataset identifiers
- semantically identical output
- no code modifications

If this fails fundamentally, stop.

## 64. v0.1 — Portable Spark Core

**Features**

REST API, CLI, PostgreSQL metadata, workload specification v1alpha1, environment profiles, dataset bindings, Kubernetes provider, S3 provider, Spark 4.2, PySpark wheel support, JVM JAR support, run lifecycle, cancel, logs, metrics.

**Explicitly Missing**

Databricks, VAST, Azure, UI, OIDC, Trino, pipelines, AI.

**Success Criterion**

A new developer can:

```
make dev
plane run examples/example.yaml
```

and execute a real Spark workload on kind/MinIO.

## 65. v0.2 — Enterprise Storage

**Add**

VAST provider, ADLS provider, storage profiles, artifact abstraction, runtime profiles, provider capability model, minimal operational UI.

**Success Criterion**

Same Kubernetes Spark workload can use VAST, S3, ADLS through environment changes only.

## 66. v0.3 — Databricks Portability

**Add**

Databricks provider, Jobs API integration, OAuth M2M, Python wheel mapping, JAR mapping, static portability validation, runtime compatibility matrix.

**Hero Demonstration**

```
plane run claims-etl --environment onprem
```

```
Kubernetes
VAST
Spark 4.2
SUCCESS
```

then:

```
plane run claims-etl --environment azure
```

```
Databricks
ADLS
Spark-compatible runtime
SUCCESS
```

Same workload. Same artifact. No application changes.

## 67. v0.4 — Conformance & Reliability

**Add**

Dynamic conformance runner, semantic output comparison, fault injection suite, provider certification framework, reconciliation hardening, idempotency, audit trail, OIDC, RBAC, workload identity, HA deployment profile.

This release transforms the project from interesting tooling into credible infrastructure.

## 68. v0.5 — Table & Query Integration

**Add**

Iceberg REST Catalog integration, table dataset bindings, Trino integration, optional Kyuubi integration, JDBC documentation.

The framework still does not own the query engine.

## 69. v0.6 — Ecosystem Integration

Potential additions: Airflow provider/operator, Dagster/Kestra integrations, Spark Declarative Pipelines support, webhooks, events API, Terraform provider/modules, SCIM, OPA integration, additional storage providers.

Only features demonstrated by actual users should enter this milestone.

## 70. Post-1.0 AI

Only after the execution/data abstractions are stable:

- Ask my data
- Ask why my job failed
- Explain performance change
- Find relevant datasets
- Generate read-only SQL

AI must remain a client.

## 71. v1.0 Exit Criteria

Do not call the project 1.0 until all of these are true.

**Portable Contract**

workload spec stable, environment spec stable, dataset spec stable, provider SDK stable.

**Execution**

Kubernetes provider production-tested, Databricks provider production-tested.

**Storage**

At least: VAST, S3, ADLS.

**Runtime**

PySpark, JVM.

**Reliability**

control-plane restart recovery, provider outage recovery, idempotent submission, HA deployment, fault-injection suite.

**Security**

OIDC, RBAC, machine identity, secret references, audit, TLS.

**Portability**

At least one nontrivial workload executes successfully against Kubernetes AND Databricks with equivalent results.

## 72. Do

- use upstream projects wherever possible;
- keep the portable workload specification provider-neutral;
- expose capability differences honestly;
- fail portability validation early;
- make every UI operation available through REST;
- use logical dataset identifiers;
- separate application artifacts from runtime containers;
- prefer workload identity over long-lived credentials;
- use local ephemeral storage for Spark shuffle by default;
- treat control-plane state as recoverable;
- test actual failure scenarios;
- publish compatibility matrices;
- make conformance tests public;
- document unsupported behavior explicitly;
- keep installation trivial for developers.

## 73. Don't

Do not:

- build another Spark scheduler;
- build another distributed filesystem;
- build another identity provider;
- build another workflow engine;
- make notebooks the workload format;
- expose Kubernetes CRDs as the public workload contract;
- expose Databricks job definitions as the public workload contract;
- store raw credentials;
- silently retry failed user applications;
- promise identical storage semantics across NFS/S3/ADLS;
- claim Databricks is identical to upstream Spark;
- use network storage for Spark shuffle by default;
- hide provider-specific behavior;
- create UI-only capabilities;
- create a proprietary table format;
- add Kafka/Redis because "distributed systems usually have them";
- add features merely to match Databricks or Cloudera.

## 74. Major Caveats

### 74.1 Storage Isn't Actually Interchangeable

NFS, S3 and ADLS have different semantics. The abstraction must normalize configuration and logical addressing without pretending those differences do not exist.

### 74.2 Data Must Physically Exist

Running the same workload in Azure does not magically move 30 TB from VAST to ADLS. Data movement is separate.

### 74.3 Provider-Specific Spark Differences Exist

Databricks may implement optimizations, libraries, configuration and behavioral changes not present upstream. Conformance must measure outcomes rather than assume equivalence.

### 74.4 Native Dependencies Are Difficult

Python wheels containing native code and JVM dependencies can behave differently across runtime environments. The compatibility suite must detect this.

### 74.5 Streaming Is Harder Than Batch

Long-running Structured Streaming applications have checkpoint and state-store concerns that significantly complicate environment portability. Initial portability guarantees should focus on batch. Streaming should be marked experimental until explicitly certified.

### 74.6 NFS Identity Is Not IAM

VAST/NFS POSIX identity and object-store IAM models are fundamentally different. Storage portability must not promise identical authorization semantics.

### 74.7 Performance Will Differ

Equivalent output does not imply equivalent runtime. The conformance report should optionally include: wall-clock duration, executor hours, data read, data written, shuffle, estimated infrastructure cost. Performance differences are information, not necessarily portability failures.

## 75. Open-Source Strategy

Recommended license: Apache-2.0.

Open source: workload specification, control plane, CLI, UI, official provider implementations, provider SDK, conformance suite, Helm charts, documentation.

Do not cripple the OSS version by withholding basic OIDC, audit, VAST, S3, Azure, Databricks functionality.

The project's credibility depends on genuinely preventing vendor lock-in.

## 76. Governance

Initially: lead maintainer + public GitHub + public roadmap + issues + PR review.

No foundation. No steering committee. No elaborate RFC bureaucracy.

If multiple organizations eventually become meaningful contributors, governance can evolve.

## 77. Potential Commercial Layer

Commercialization should happen around operations, not artificial feature removal.

Possible commercial offerings: enterprise support, SLAs, certified builds, long-term-support releases, fleet management, upgrade assistance, security/compliance packages, Cloudera migration assistance, HDFS → modern storage migration tooling, managed control plane, deployment consulting, provider certification.

The runtime itself should remain useful without buying anything.

## 78. Provider Certification

Long-term, a powerful differentiator could be public compatibility certification. Example:

```
Portable Runtime Conformance 1.0

Apache Spark/Kubernetes + AWS S3       PASS
Apache Spark/Kubernetes + VAST NFS     PASS
Apache Spark/Kubernetes + VAST S3      PASS
Apache Spark/Kubernetes + ADLS         PASS
Databricks Runtime 19 + AWS S3         PASS
Databricks Runtime 19 + ADLS           PASS
```

Third-party vendors could execute the public suite themselves. The conformance tests should be deterministic and open source.

**Design invariant: capabilities compose, verification evidence does not.** An execution provider declaring Spark support and a storage provider declaring data access means that combination is logically usable — the two capability declarations compose freely. It does not mean that exact execution+storage pair has been live-verified. `plane conformance report`'s `PASS` status for a given pair must be backed by evidence of that specific pair actually having been run together, never inferred from each side being independently verified elsewhere (`control_plane/certification.py`'s `_LIVE_VERIFIED_COMBINATIONS` tracks pairs explicitly for exactly this reason — see its module docstring for a real bug this distinction fixed in v1.0.0).

## 79. Primary Product Metrics

Do not initially optimize for: number of features, number of integrations, lines of code.

Measure:

- **Time to First Job** — `git clone` → successful Spark job
- **Portability Rate** — percentage of workloads that execute on multiple providers without provider overrides
- **Run Reliability** — successful control-plane submissions, reconciliation failures, duplicate executions, lost runs
- **Upgrade Compatibility** — how many existing workload specs continue working unchanged across runtime upgrades
- **Operational Burden** — how much infrastructure is required to operate the platform

## 80. Architectural Success Test

The project is succeeding if an application team can think:

> I have a Spark application.

rather than:

> I have a Kubernetes Spark application.

or:

> I have a Databricks application.

And infrastructure teams can independently decide where it runs, where its data lives, how it authenticates, what runtime implements it — without rewriting business logic.

## 81. Ultimate Architecture

```
                         APPLICATIONS
                              │
                    Portable Workload Spec
                              │
            ┌─────────────────┴──────────────────┐
            │                                     │
           REST                                 CLI/UI
            │                                     │
            └─────────────────┬───────────────────┘
                               │
                       CONTROL PLANE
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                     │
      Resolution           Conformance            Security
          │                    │                     │
          └────────────────────┼────────────────────┘
                               │
                  EXECUTION ABSTRACTION
                               │
              ┌─────────────────┴────────────────┐
              │                                   │
          Kubernetes                          Databricks
              │                                   │
     Spark K8s Operator                       Jobs API
              │                                   │
        Apache Spark                       Spark Runtime
              │                                   │
              └─────────────────┬─────────────────┘
                               │
                     LOGICAL DATA PLANE
                               │
                 ┌──────────────┼──────────────┐
                 │              │              │
                VAST            S3            ADLS
                 │              │              │
                 └──────────────┼──────────────┘
                               │
                   OPTIONAL OPEN TABLES
                               │
                            Iceberg
                               │
                        OPTIONAL QUERY
                               │
                         Trino/Kyuubi
```

## 82. Final Architectural Principle

The project should be able to make this statement without qualification:

> Your Spark code belongs to you. The framework provides a portable contract for executing it.
>
> Kubernetes is replaceable. Databricks is replaceable. VAST is replaceable. S3 is replaceable. Azure is replaceable. The framework itself is open and replaceable.
>
> The workload remains yours.

That should be the architectural constraint against which every future feature request is judged.

---

The biggest design decision in this spec is not building Trino, pipelines, Iceberg, or AI into the core execution path. They become optional integrations after workload portability is proven. That made Phase 0 → v0.3 a surprisingly tractable project: prove the same real Spark artifact can execute on K8s/VAST or S3 and Databricks/ADLS through one contract. That worked cleanly (live-verified through v1.0), and everything since has been additive rather than existential.
