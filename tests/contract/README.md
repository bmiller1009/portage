# tests/contract

The provider conformance suite (`docs/architecture/spec.md` §43, §52.1). Every `ExecutionProvider` runs the same suite: submit valid workload, reject invalid workload, discover status, cancel, handle missing workload, handle provider timeout, handle duplicate submission, return capabilities, recover after control-plane restart. Every storage provider runs: resolve logical dataset, reject invalid mapping, resolve identity, health check, generate Spark configuration, never expose secrets. A provider isn't "supported" until it passes this suite.

Not yet implemented — depends on the Phase 0 provider prototypes.
