# Kubernetes execution provider — Phase 0 live-run results

Real infrastructure, real job, real output — run on 2026-08-17 against a dedicated Phase 0 remote host (kind cluster, not a production target).

## What was actually running

| Component | Version, confirmed live (not assumed) |
|---|---|
| Kubernetes (kind node image) | v1.34.0 |
| Apache Spark Kubernetes Operator (Helm chart) | `spark-kubernetes-operator` 1.8.0 |
| Apache Spark Kubernetes Operator (app version) | 1.0.0 |
| Spark base image | `apache/spark:4.2.0-python3` |
| SparkApplication CRD | `spark.apache.org/v1` (also serves `v1beta1`) |
| Storage | MinIO (S3-API-compatible), in-cluster |

All of this matches the spec's target (`docs/architecture/spec.md` §46: Kubernetes 1.34+, Operator 1.0+, Spark 4.2) — confirmed by inspecting the live CRD schema (`kubectl get crd sparkapplications.spark.apache.org -o json`) rather than assumed from the spec text.

## SparkApplication shape (this operator generation, not the legacy `sparkoperator.k8s.io`)

No `mainApplicationFile` field exists on this CRD. The Python application entry point is expressed as `pyFiles` (a `local://` path baked into the container image) plus `driverArgs`; all Spark/Kubernetes configuration goes through the flat `sparkConf` map, including our `spark.portable.dataset.*.uri` keys. See `providers/execution/kubernetes/provider.py`'s `build_spark_application()` and `providers/execution/kubernetes/image/launcher.py` (the generic entry-point launcher baked into the base image).

For JVM (`jvm-jar`), the CRD *does* expose `mainClass` and `jars` (both plain strings, confirmed via `kubectl get crd sparkapplications.spark.apache.org -o json | jq '.spec.versions[] | select(.name=="v1") | .schema.openAPIV3Schema.properties.spec.properties | keys'`) — `spark-submit --class` has no `sparkConf` equivalent, so it must be a dedicated field, unlike Python's dynamic-import launcher trick. No generic-launcher indirection is needed for JAR: `mainClass`/`jars` dispatch directly. Live-verified against the base image's bundled `spark-examples.jar` (`org.apache.spark.examples.SparkPi`) — reached `SUCCEEDED` twice, and the driver's own shutdown stack trace shows `org.apache.spark.examples.SparkPi$.main(SparkPi.scala:40)` actually executing.

**Bug found via this live run, fixed**: `KubernetesExecutionProvider.logs()` had been generating `kubectl logs -l spark.apache.org/app-name=...` — the wrong label. The operator actually labels driver pods `spark.operator/spark-app-name=<run-name>` (confirmed via `kubectl get pod --show-labels` against a real running driver), not `spark.apache.org/app-name`. The old label silently matched zero pods.

## What it took to get a real job running

Three things the spec doesn't (and shouldn't have to) call out, discovered empirically:

1. **RBAC.** The operator's Helm chart doesn't provision the `spark` workload ServiceAccount/RoleBinding in a namespace unless told which namespace(s) to target (`workloadResources.namespaces.data`). It also needs a config *reload* — a `helm upgrade` alone doesn't make the running operator pod pick up a new watched-namespace list; it requires `kubectl rollout restart`.
2. **`deletecollection` RBAC.** The chart's default `spark-workload-clusterrole` grants `delete` but not `deletecollection` on pods/configmaps. Spark's own Kubernetes scheduler backend cleans up executors via a label-selector bulk delete, which needs `deletecollection` specifically. Without it, the driver's *shutdown* throws even when the actual computation succeeded — this can look like a false failure. Fixed with a small supplementary ClusterRole/RoleBinding (see the remote bring-up notes below), additive rather than editing the chart-owned role. The same gap also surfaced for `persistentvolumeclaims` while live-verifying JVM JAR support (issue #21) — the driver's shutdown-time cleanup tried a `deletecollection` on PVCs and got the identical 403; the run still reached `SUCCEEDED` (same non-fatal pattern), but the supplementary RBAC should cover PVCs too, not just pods/configmaps — tracked as part of #23's `make dev` RBAC manifest.
3. **`spark.conf.get()` silently drops non-`spark.*` keys.** See ADR 0006's amendment — this is a dataset-model fix, not a Kubernetes-provider fix, but it only surfaced by running a real job.

## Result

```
$ plane run examples/wordcount.yaml --environment k8s-remote
submitted: provider_run_id=wordcount-9acfe16a
status: SUBMITTING (Unreported)
status: RUNNING (RunningHealthy)
...
status: SUCCEEDED (Succeeded)
SUCCEEDED: wordcount-9acfe16a
```

Output read back from MinIO (`wordcount/output/*.snappy.parquet`) and verified correct against the seeded input — e.g. `the: 250, fox: 150, dog: 100`, plus eleven singleton words at 50 each, exactly matching 50 repetitions of the 3-line seed text.

## Reproducing / tearing down

The remote bring-up (kind cluster, operator install, MinIO, RBAC) was done by hand over SSH during this session, not scripted — that scripting is a natural v0.1 `make dev` task, not Phase 0 scope (`docs/architecture/spec.md` §45). Key commands, in order:

```
kind create cluster --config kind-config.yaml   # 1 control-plane + 2 workers
helm repo add spark-kubernetes-operator https://apache.github.io/spark-kubernetes-operator
helm install spark-kubernetes-operator spark-kubernetes-operator/spark-kubernetes-operator -n spark-operator --create-namespace
helm upgrade spark-kubernetes-operator spark-kubernetes-operator/spark-kubernetes-operator \
  -n spark-operator --set workloadResources.namespaces.create=false \
  --set workloadResources.namespaces.data="{default}"
kubectl rollout restart deployment/spark-kubernetes-operator -n spark-operator
# + the supplementary deletecollection ClusterRole/RoleBinding above
# + MinIO Deployment/Service in namespace portage-storage
```
