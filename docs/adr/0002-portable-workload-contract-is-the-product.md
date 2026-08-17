# 0002. Portable workload contract is the product

## Status

Accepted

## Context

Spark applications routinely become coupled to their execution environment through `hdfs://`, `s3a://`, `abfss://` paths, `dbutils.*` calls, Databricks-specific job constructs, YARN config, and Kubernetes Spark CRDs (spec §2). This coupling is what makes moving a Spark workload between infrastructures expensive, and it's the actual problem Portage exists to solve — not running Spark better, not building a nicer lakehouse.

## Decision

The Portable Workload Contract — an infrastructure-neutral description of application artifact, entry point, Spark runtime requirements, logical datasets, resources, execution policy, identity, and arguments (spec §1, §7) — is the product. Everything else in the system either implements that contract (execution providers, storage providers) or integrates with it (Trino, Iceberg, AI). The invariant every feature is judged against: **a conforming workload MUST NOT belong to the infrastructure on which it happens to execute** (spec §3).

## Consequences

Kubernetes pods, Databricks clusters, VAST mount paths, S3 buckets, ADLS containers, Kubernetes service accounts, and Databricks Jobs API payloads must never leak into the portable workload schema (`spec/workload/`). Any feature proposal that would require a workload author to know which infrastructure they're targeting should be rejected or pushed into `providerOverrides` (see ADR 0010), not the core schema.
