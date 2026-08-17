# 0007. Async submission with reconciliation

## Status

Accepted

## Context

If `POST /v1/runs` synchronously waited on Kubernetes or Databricks submission, API latency and availability would be hostage to provider latency and outages, and a control-plane crash mid-request could leave a run in an ambiguous state. But control-plane failure must never terminate a running Spark job (spec §4.5) — once submitted, execution providers own their workloads independently of Portage's own health.

## Decision

`POST /v1/runs` persists the run as `ACCEPTED` and returns `202` with a `run_id` immediately. An async reconciliation worker (`reconciler/`) then validates, submits to the provider, polls/watches provider status, and converges the canonical run state (spec §24). On control-plane restart, the reconciler rediscovers in-flight runs and converges rather than assuming state.

## Consequences

API read/write latency is decoupled from provider latency; provider outages degrade reconciliation lag, not API availability; retries are centralized and controllable in the reconciler rather than duplicated per-request. This is also why idempotency keys (`Idempotency-Key` header, spec §25) are load-bearing — async submission plus client retries means duplicate-run prevention can't rely on synchronous request/response semantics.
