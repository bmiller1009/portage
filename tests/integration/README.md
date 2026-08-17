# tests/integration

CI-run tests against a real local stack: kind + Apache Spark Kubernetes Operator + MinIO + PostgreSQL, running real Spark applications — PySpark wheel, JVM JAR, input/output storage, dynamic allocation, failure, cancel, large logs, invalid Spark configuration (`docs/architecture/spec.md` §53). Mocks are insufficient for this layer.

Not yet implemented — v0.1 milestone.
