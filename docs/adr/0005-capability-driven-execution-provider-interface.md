# 0005. Capability-driven execution provider interface

## Status

Accepted

## Context

With at least two execution providers (Kubernetes, Databricks) and a stated intent to eventually support third-party providers (spec §44), provider-specific branching (`if provider == "databricks": ...`) scattered through the control plane would make it impossible to reason about what's portable versus provider-specific, and would make adding a provider a search-and-modify exercise across the whole codebase.

## Decision

All execution providers implement one `ExecutionProvider` protocol — `validate`, `submit`, `status`, `cancel`, `logs`, `capabilities` (spec §13) — and publish a capability set (supported Spark versions, languages, dynamic allocation, GPU, streaming, local disk, Spark Connect — spec §20). The control plane validates a workload against a provider's declared capabilities *before* submission and fails fast on incompatibility, rather than submitting something known-incompatible and waiting for Spark to fail. Storage providers follow the same pattern (spec §47).

## Consequences

Adding a provider means implementing the protocol and passing the provider contract test suite (`tests/contract/`, spec §43, §52.2) — it does not require touching `control_plane/` internals. Code that branches on provider identity outside the `providers/` tree is a review-blocking smell.
