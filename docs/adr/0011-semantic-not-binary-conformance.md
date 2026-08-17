# 0011. Semantic, not binary, conformance

## Status

Accepted

## Context

Kubernetes Spark and Databricks Runtime will never be binary-identical — Databricks Runtime 19 (Spark 4.2.0) and Databricks Runtime 18 LTS (Spark 4.1.0) both layer additional behavior and libraries on top of upstream Spark (spec §4.6). File-level output comparison (`Parquet file A == Parquet file B`) is doomed to fail even for genuinely correct, portable workloads, because file layout, partition sizing, compression, and metadata legitimately differ across environments (spec §22).

## Decision

Conformance ("does this workload produce the same result across providers?") is defined as semantic equivalence: matching schema, row count, null distribution, partition coverage, key aggregates, business invariants, deterministic row hashes, and table snapshots where applicable — not byte-for-byte file equality (spec §6, §22). For very large datasets, use partition-level validation instead of full binary comparison. Data correctness fixtures must exercise primitive types, timestamps, decimals, nulls, nested structs, arrays/maps, partitioned data, schema evolution, large strings, Unicode, and timezone-sensitive data (spec §58), since these are exactly where upstream/Databricks divergence tends to surface.

## Consequences

The conformance runner (`conformance/`) and `tests/e2e/` are built around comparators for these semantic properties, not diff tools. Performance differences (wall-clock, executor hours, cost) are reported as information alongside a conformance result, not treated as a portability failure on their own (spec §74.7) — "equivalent output" and "equivalent runtime" are deliberately different claims.
