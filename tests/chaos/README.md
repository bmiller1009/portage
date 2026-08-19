# tests/chaos

Live fault-injection tests against a real Kubernetes cluster with the
`charts/portage` Helm release installed (`docs/architecture/spec.md`
§56–57). The control-plane recovery test — destroy part of the control
plane while a job is running, confirm the job continues and
reconciliation converges — is spec §57's "mandatory v1.0 acceptance
test."

Not run on every PR: needs a live cluster, a real Kubernetes execution
provider the reconciler can actually submit Spark jobs to, and enough
plumbing (RBAC, MinIO, the Spark Operator) that only the project's remote
dev box has — same category as `tests/integration/test_s3_provider_minio.py`.
Instead, `.github/workflows/live-provider.yml` runs this suite weekly
(and on manual `workflow_dispatch`) by SSHing onto that box and running
the same command a developer would run by hand — see that workflow file
for the required repository secrets. To run it yourself manually, e.g.
over SSH on that box:

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
- `test_idempotent_submission.py` — two distinct idempotency scenarios:
  concurrent duplicate `POST /v1/runs` with the same `Idempotency-Key`
  racing across both real API replicas, and (v1.0.0) a client whose own
  connection times out before it sees the response, retrying — a
  different failure shape (the client doesn't know whether the first
  attempt landed, rather than two attempts racing each other) — proving
  both land on exactly one run.
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
  with no visible availability gap. Used to fail intermittently with a
  genuine `401 Unauthorized` from the real Kubernetes API server on a
  reconciler status() poll — never root-caused despite ruling out four
  plausible mechanisms live, but empirically mitigated (4/4 clean runs
  after the fix, vs. roughly half failing before) by treating 401 as
  retryable in `providers/execution/kubernetes/provider.py`. See the
  module docstring and [#57](https://github.com/bmiller1009/portage/issues/57)
  for the full history.

## Known, deliberate limitation: no Databricks-API-interruption test

`test_provider_outage_recovery.py` fault-injects a genuine Kubernetes API
partition (an unreachable address baked into a throwaway reconciler pod)
because that's a real, controllable failure mode against a
self-hosted cluster this project fully controls. There is no equivalent
test for Databricks: its Jobs API is a hosted third-party service with no
fault-injectable surface this project can reach into (no way to make
*just* the Databricks control plane unreachable without also cutting the
reconciler's own network access entirely, which wouldn't isolate anything
provider-specific). `providers/execution/databricks/provider.py`'s
`requests.exceptions.ConnectionError`/`Timeout` handling is applied by
analogy to the Kubernetes provider's live-confirmed `MaxRetryError` fix
(same comment, same reasoning) — a principled extrapolation, not
independently live-verified, and it stays that way; forcing a synthetic
test here wouldn't prove anything a real partition would actually
exercise.

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
- `providers/execution/kubernetes/provider.py` (#57): under heavy
  reconciler/API pod churn, a `status()`/`cancel()` call can get a
  genuine, real (correctly-signed, well-formed) `401 Unauthorized` from
  the live Kubernetes API server, sandwiched between successful calls
  using the exact same unmodified credential chain — a shape far more
  consistent with a transient server-side hiccup under load than an
  actual authentication problem. The precise mechanism was never
  confirmed despite ruling out four plausible causes live (see
  `test_ha_deployment.py`'s docstring); the fix treats 401 as retryable
  alongside the other transient status codes, empirically eliminating
  the failure (4/4 clean runs afterward vs. roughly half failing
  before).
