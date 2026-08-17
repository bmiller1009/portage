# 0008. PostgreSQL-only metadata store

## Status

Accepted

## Context

Control-plane metadata (environments, execution/storage profiles, dataset bindings, workload definitions, runs, run events, provider runs, audit events, idempotency keys, runtime profiles — spec §27) needs durable, queryable, transactional storage. It would be easy to reach for Kafka for run events or Redis for caching/idempotency-key lookups "because distributed systems usually have them" (spec §73) — but control-plane throughput is modest (async network orchestration, not high-volume streaming), and every added stateful dependency is more for a self-hosted, `make dev`-friendly project to operate.

## Decision

PostgreSQL 16+ is the only persistence technology in v0.x. No Kafka, no Redis, no other distributed cache unless real operational load proves one necessary (spec §27, §41). PostgreSQL remains authoritative for control-plane metadata; the execution provider remains authoritative for actual running-workload state; the reconciler converges the two. Complete Spark logs do not go into PostgreSQL — only references, summaries, and diagnostic metadata (spec §28); full logs stay in the environment's own logging infrastructure (Loki, Elastic, Splunk, Spark History Server, provider-managed Databricks logs).

## Consequences

Local development only needs one stateful dependency (`make dev` provisions PostgreSQL, not a message broker or cache cluster). If a future scale target genuinely requires a queue or cache, that's a new ADR justified by measured load — not a default architectural choice made up front.
