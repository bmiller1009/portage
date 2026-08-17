# 0003. Thin control plane, no absorbed subsystems

## Status

Accepted

## Context

Platforms in this space (Cloudera, Databricks, IOMETE, Stackable) tend to grow into full lakehouse suites — notebooks, BI, workflow orchestration, catalogs, secret management — because each capability seems individually reasonable to add. That path competes with well-run upstream projects on their own turf and turns a narrow portability tool into a large, hard-to-trust platform (spec §4.4, §5).

## Decision

The control plane coordinates workload submission, validation, resolution, and lifecycle tracking, and nothing else. It must not become a data warehouse, distributed scheduler, secret vault, identity provider, object store, notebook environment, ETL designer, proprietary catalog, or query optimizer. The explicit non-goals list in spec §5 (notebooks, BI/dashboards, visual ETL, general workflow orchestration, distributed SQL, distributed filesystem, object store, data warehouse, ML platform, model registry, streaming platform, Kafka distribution, secret manager, identity provider, data marketplace, catalog UI, lineage engine, vector database, AI agent framework) is published in the README and treated as binding, not aspirational.

## Consequences

Feature requests that would pull one of these capabilities in-tree get redirected to "integrate with an existing project" (Trino for query, Airflow/Dagster for orchestration, Vault/cloud KMS for secrets, OIDC providers for identity). This is a standing filter for scope creep, not a one-time decision.
