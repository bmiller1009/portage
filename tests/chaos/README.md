# tests/chaos

Live fault-injection tests against a real Kubernetes cluster with the
`charts/portage` Helm release installed (`docs/architecture/spec.md`
§56–57). The control-plane recovery test — destroy part of the control
plane while a job is running, confirm the job continues and
reconciliation converges — is spec §57's "mandatory v1.0 acceptance
test."

Not run in CI: needs a live cluster, a real Kubernetes execution provider
the reconciler can actually submit Spark jobs to, and enough plumbing
(RBAC, MinIO, the Spark Operator) that only the project's remote dev box
has — same category as `tests/integration/test_s3_provider_minio.py`.
Run manually, e.g. over SSH on that box:

```bash
PORTAGE_CHAOS_NAMESPACE=portage-system \
MINIO_ENDPOINT=http://minio.portage-storage.svc.cluster.local:9000 \
python -m pytest tests/chaos -v -s
```

## What this expects to already exist

- The `charts/portage` Helm release installed in `$PORTAGE_CHAOS_NAMESPACE`
  (default release name `portage`; override via `PORTAGE_CHAOS_RELEASE`),
  with `reconciler.inClusterKubeconfig.enabled=true` so the reconciler can
  submit to the same cluster it runs in.
- A ClusterRole/RoleBinding granting the reconciler's ServiceAccount
  (`<namespace>:default` unless overridden) `get/list/watch/create/update/
  patch/delete` on `sparkapplications.spark.apache.org` in the namespace
  Spark workloads run in, plus `get/list/watch` on `pods`/`pods/log`.
- A `credentials.existingSecretName` Secret with
  `PORTAGE_CHAOS_MINIO_ACCESS_KEY`/`PORTAGE_CHAOS_MINIO_SECRET_KEY` keys,
  so the seeded StorageProfile's `credential_reference` resolves.
- The Spark Kubernetes Operator and a reachable MinIO with the `wordcount/
  input.txt` object already seeded (`scripts/dev-up.sh` sets both of
  these up as part of the regular dev stack).

## Files

- `conftest.py` — shared fixtures: seeds a real `kubernetes`+`s3`
  Environment via the live API (`environment_name`), registers the real
  `wordcount.yaml` fixture as a WorkloadDefinition (`workload_ref`),
  port-forwards the API (`api_base_url`/`api_client`), and pod/Deployment
  helpers (`delete_pod`, `wait_for_ready_replicas`,
  `wait_for_pod_count`, `wait_for_terminal_state`).
- `test_idempotent_submission.py` — concurrent duplicate `POST /v1/runs`
  with the same `Idempotency-Key`, racing across both real API replicas.
- `test_control_plane_recovery.py` — kills the sole API or reconciler pod
  (scaled to 1 replica first, so the kill isn't masked by the other HA
  replica) mid-run and confirms recovery.
- `test_provider_outage_recovery.py` — a throwaway reconciler pod with an
  unreachable Kubernetes API address baked in at pod-creation time (not a
  runtime config swap — see `start_outage_test_reconciler`'s docstring
  for why that approach turned out to be non-deterministic), proving a
  run waits out a genuine provider network partition instead of failing
  outright.
- `test_ha_deployment.py` — kills one of two API replicas and one of two
  reconciler replicas simultaneously, confirming the run still completes
  with no visible availability gap. **Intermittently flaky** — see the
  module docstring and [#57](https://github.com/bmiller1009/portage/issues/57)
  for a real, live-confirmed, not-yet-root-caused finding distinct from
  the bugs this suite already found and fixed.

## Real bugs this suite found and fixed (not hypothetical)

- `providers/execution/kubernetes/provider.py`: the Kubernetes API being
  genuinely unreachable raises `urllib3.exceptions.MaxRetryError`, which
  was never caught (only `ApiException`, which only wraps a response the
  server actually sent) — a real network partition was falling through
  to the reconciler's unclassified-exception handler and permanently
  failing the run instead of retrying. Also added an explicit
  `_request_timeout` — without one, a call against an unreachable server
  could hang far longer than any reasonable retry budget before even
  raising.
- `reconciler/service.py`'s `poll_active_runs()`: `TerminalProviderError`
  had no dedicated `except` clause (unlike `submit_new_runs()`), so a
  deliberately classified terminal error fell through to the generic
  handler and got mislabeled "unclassified error" in the run's own
  failure message.
- `control_plane/repositories.py`'s `create_idempotency_key()`: two
  genuinely concurrent `POST /v1/runs` calls with the same
  `Idempotency-Key` could both pass the initial existence check before
  either committed, creating two `Run` rows and then hitting a raw
  `IntegrityError` (a 500) on the losing request instead of cleanly
  deduplicating. Fixed with an atomic `INSERT ... ON CONFLICT DO NOTHING`
  instead of a plain insert + exception handling — the latter also hit a
  genuine SQLAlchemy asyncio/greenlet edge case when the same session
  kept being used right after the conflict.
