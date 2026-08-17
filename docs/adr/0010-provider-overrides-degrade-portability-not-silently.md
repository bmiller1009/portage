# 0010. Provider overrides degrade portability, never silently

## Status

Accepted

## Context

Advanced infrastructure always needs escape hatches — some workloads legitimately require Kubernetes- or Databricks-specific configuration that has no portable equivalent (spec §19). Refusing to allow any provider-specific configuration would push teams to abandon the contract entirely for anything nontrivial. But allowing it without guardrails risks the opposite failure: provider-specific settings quietly becoming load-bearing, and the workload silently stops being portable without anyone noticing.

## Decision

`providerOverrides` are permitted, but must be namespaced per provider (`providerOverrides.kubernetes`, `providerOverrides.databricks`), ignored entirely by unrelated providers, and must cause portability validation to report a degraded status — `PORTABLE WITH PROVIDER-SPECIFIC OVERRIDES` rather than a plain `PORTABLE` (spec §19, example in §19). They must never silently alter the portable contract itself.

## Consequences

`plane validate` and the conformance report must always surface override counts per provider, and this status must be visible wherever workload health is shown (CLI, UI, API). A workload with overrides isn't rejected, but its portability claim is honest rather than fictional — this is also how the project can eventually offer a credible provider certification story (spec §78) without overrides quietly undermining it.
