# spec/dataset

The `Dataset` binding schema (`docs/architecture/spec.md` §9, §11): maps a logical dataset name (`claims.raw`) to a per-environment physical location — either a `path` binding (file/object URI) or a `table` binding (catalog identifier, e.g. an Iceberg table). Dataset mapping is explicitly not data replication (§12) — it assumes the bytes already exist in the target environment.

`v1alpha1.py` implements path-kind bindings and `resolve_dataset_config()`, which produces the `portable.dataset.<name>.uri` Spark configuration entries a workload needs (ADR 0006). Table-kind bindings (§11) remain out of scope — later milestone.
