# 0006. Logical dataset model, no mandatory SDK

## Status

Accepted

## Context

Storage portability requires separating logical data identity (`claims.raw`) from physical location (`/vast/data/claims/raw`, `s3a://company-prod/claims/raw`, `abfss://claims@companyprod/claims/raw`) — spec §9. The tempting implementation is a runtime SDK that application code imports to resolve dataset URIs. But that would recreate exactly the kind of framework lock-in Portage exists to prevent (spec §10): an application written against a proprietary `datasets.uri(...)` call is no more portable than one written against `dbutils`.

## Decision

Dataset bindings are resolved by the control plane/CLI and injected as plain Spark configuration keys — `spark.portable.dataset.claims.raw.uri` — which application code reads via `spark.conf.get(...)`. This raw configuration contract is the stable, public interface. A convenience SDK (`sdk/python/`) may wrap it with a nicer API, but it must remain strictly optional, never required.

**Amendment (Phase 0 live testing):** the original spec draft's example (§10) shows the bare key `portable.dataset.claims.raw.uri`, with no `spark.` prefix. Running a real job against a real Kubernetes Spark Operator cluster during Phase 0 showed this doesn't work: Spark 4.2's `RuntimeConfig` silently drops any `--conf` key that isn't `spark.*`-namespaced ("Ignoring non-Spark config property"), so a bare key is accepted into the SparkApplication's `sparkConf` map without error but is never actually visible to `spark.conf.get()` inside the driver — the job fails at runtime with `SQL_CONF_NOT_FOUND`. The implemented contract therefore prefixes every injected key with `spark.` (`spec/dataset/v1alpha1.py`'s `resolve_dataset_config`). This is exactly the kind of correction ADR 0011 anticipates: verify against real behavior rather than assume the spec's draft example was already correct.

## Consequences

A Portage application never needs to depend on a Portage library to be portable — only to run against the configuration contract, which is trivial to reimplement or vendor if the project ever goes away. This is the concrete mechanism behind "the workload remains yours" (spec §82).
