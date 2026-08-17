# conformance

Static and dynamic portability conformance (`docs/architecture/spec.md` §21–22). Static validation (`plane validate workload.yaml`) checks Spark API compatibility, artifact compatibility, dataset bindings, runtime capabilities, provider overrides, credential availability, catalog compatibility, resource requirements, and unsupported functionality — before submission, so incompatible workloads fail fast rather than exploding inside Spark.

Dynamic conformance (`plane conformance test`) executes the same workload across multiple providers and compares semantic output — schema, row count, null distribution, partition coverage, key aggregates, business invariants, deterministic row hashes — never raw byte-for-byte file comparison (§22, ADR 0011).

Not yet implemented — Phase 0 exit demonstration depends on a minimal version of this; full static+dynamic runner is v0.4 milestone.
