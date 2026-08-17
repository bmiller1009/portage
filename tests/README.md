# tests

Six layers, per `docs/architecture/spec.md` §52: `unit/`, `contract/` (every provider runs the same suite), `integration/` (real kind + Spark Operator + MinIO + PostgreSQL), `e2e/` (cross-provider portability fixtures), `security/`, and `chaos/` (failure injection). Portability is a claim the project makes about itself, and these suites are how that claim gets proven rather than asserted.

Only `unit/` has content so far.
