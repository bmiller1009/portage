# spec

The portable contract itself — the part of the system that is the product (`docs/architecture/spec.md` §3, ADR 0002). Four schemas, each versioned independently of the software (§62):

- `workload/` — the `SparkWorkload` definition (§7): artifact, entry point, runtime, arguments, logical datasets, resources, execution policy. Contains no Kubernetes or Databricks objects.
- `environment/` — the `Environment` profile (§8): which execution provider, storage provider, catalog, and identity provider a named environment (`onprem-prod`, `azure-prod`) resolves to.
- `dataset/` — the `Dataset` binding (§9, §11): maps a logical dataset name to a per-environment path or table identifier.
- `artifact/` — the `Artifact` binding (§51): the same shape of problem as `dataset/`, but for the application artifact itself — `artifact://claims-normalization/4.18.2` resolves to a per-environment S3/ADLS object (or an internal repository) rather than being embedded in the workload definition directly.

Schema changes here are the highest-stakes changes in the repo — they define what "portable" means.
