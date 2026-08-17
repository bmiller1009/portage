# 0004. Upstream-first: no custom Spark scheduler

## Status

Accepted

## Context

Kubernetes-native Spark execution and Databricks job submission are both already solved problems with actively maintained upstream implementations: the Apache Spark Kubernetes Operator (now an Apache Spark subproject, 1.0.0 supports Spark 4.0–4.2 and Kubernetes 1.34–1.36) and the Databricks Jobs API (currently 2.2, with a one-time-run submission endpoint for workloads that don't need a persistent job definition) (spec §4.2, §14, §16).

## Decision

The Kubernetes execution provider translates a resolved portable workload into a `SparkApplication` custom resource and submits it via the official Spark Kubernetes Operator — it does not manage Spark pods directly or implement its own scheduling logic. The Databricks execution provider translates into a Jobs API 2.2 one-time submission, preferring Python wheel and JAR tasks over notebooks or the legacy `spark_submit_task`. This pattern generalizes: distributed SQL goes through Trino/Kyuubi, table format through Iceberg, orchestration through Kubernetes itself, authorization policy through OPA or engine-native policy, identity through OIDC/OAuth, metrics through OpenTelemetry/Prometheus (spec §4.2).

## Consequences

Execution providers are translation layers, not schedulers — driver/executor pod lifecycle is entirely the operator's responsibility, and job run lifecycle on Databricks is entirely the Jobs API's responsibility. This keeps Portage's own failure surface small and means Spark version/Kubernetes version compatibility tracks the upstream operator's support matrix rather than a custom one.
