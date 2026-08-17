# spec/dataset

The `Dataset` binding schema (`docs/architecture/spec.md` §9, §11): maps a logical dataset name (`claims.raw`) to a per-environment physical location — either a `path` binding (file/object URI) or a `table` binding (catalog identifier, e.g. an Iceberg table). Dataset mapping is explicitly not data replication (§12) — it assumes the bytes already exist in the target environment.

Not yet implemented — Phase 0 backlog item (`Dataset binding model (path-based) + resolver`) covers the initial path-binding version.
