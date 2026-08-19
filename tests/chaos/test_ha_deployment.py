"""HA-specific fault injection (spec.md §71 Reliability — "HA deployment"):
with both the API and reconciler at their normal 2-replica configuration
(charts/portage's default, unlike test_control_plane_recovery.py's
single-replica scenarios), kill one of each mid-run and confirm the run
still reaches a terminal state with no visible availability gap — the
other replica of each keeps serving throughout, which is the entire point
of running 2 in the first place.

KNOWN OPEN FINDING (tracked separately, not fixed by this suite): this
specific test is intermittently flaky (~1 in 3 runs observed) with a real,
reproducible failure distinct from the two bugs this milestone's other
work already fixed (MaxRetryError misclassification in
providers/execution/kubernetes/provider.py; the "unclassified error"
mislabeling in reconciler/service.py's poll_active_runs). The run
correctly submits (QUEUED) and its first status() poll succeeds, but a
*later* status() poll against the same still-existing SparkApplication
occasionally gets a genuine `(401) Unauthorized` from the real Kubernetes
API server — not a connection failure, a real signed response — which
_raise_classified() correctly treats as terminal (401 is not in
_RETRYABLE_API_STATUS_CODES), so the run fails outright.

Ruled out live, not guessed:
- Global-Configuration-object races from concurrent load_kube_config()
  calls across different ExecutionProfiles (asyncio.gather reproduction
  inside the real reconciler pod never showed cross-contamination — each
  CustomObjectsApi instance's configuration.host stayed correctly scoped
  throughout).
- A freshly-created pod's projected ServiceAccount token not being ready
  yet (a brand-new pod's very first API call, tested immediately after
  Running, succeeded cleanly every time — no propagation delay observed).
- reconciler/service.py's own control flow is strictly sequential
  (asyncio.run(run_forever(...)), one tick fully awaited before the
  next) — there is no concurrency *within* one reconciler process.

Not yet ruled out: the two reconciler *replicas* run as independent
processes and both poll_active_runs() globally (not partitioned/leader-
elected) every interval, so two pods can genuinely poll the same run's
status() within moments of each other under normal HA operation — this
is the leading remaining hypothesis but hasn't been confirmed as the
actual mechanism. Left as an open, separately-tracked investigation
rather than claimed fixed."""

import time

import httpx

from tests.chaos.conftest import (
    delete_pod,
    pod_names,
    wait_for_ready_replicas,
    wait_for_terminal_state,
)


def test_run_completes_despite_killing_one_api_and_one_reconciler_replica(
    api_client, environment_name, workload_ref
):
    api_pods_before = pod_names("api")
    reconciler_pods_before = pod_names("reconciler")
    assert len(api_pods_before) == 2, f"expected 2 API pods, found {api_pods_before}"
    assert len(reconciler_pods_before) == 2, f"expected 2 reconciler pods, found {reconciler_pods_before}"

    workload_name, workload_version = workload_ref
    resp = api_client.post(
        "/v1/runs",
        json={
            "workload_name": workload_name,
            "workload_version": workload_version,
            "environment_name": environment_name,
        },
    )
    resp.raise_for_status()
    run_id = resp.json()["id"]

    # Give the reconciler pool a moment to actually claim the run before
    # killing anything — killing before any work started would trivially
    # "survive" without proving the in-flight case.
    deadline = time.time() + 30
    state = "ACCEPTED"
    while time.time() < deadline and state == "ACCEPTED":
        time.sleep(2)
        state = api_client.get(f"/v1/runs/{run_id}").json()["state"]

    delete_pod(api_pods_before[0])
    delete_pod(reconciler_pods_before[0])

    # Availability check: the API must keep answering throughout the
    # replacement window (the surviving replica of each, not a gap) —
    # sampled continuously rather than checked once, so a brief outage
    # can't hide between polls. One retry per sample absorbs a stale
    # pooled keep-alive connection to the specific pod just killed
    # (confirmed live as a real, expected httpx.RemoteProtocolError right
    # after a kill — normal client-side connection-pool churn any real
    # client would also retry through, not a server-side gap) without
    # masking a genuine one: if the retry *also* fails, that's real
    # unavailability and still fails the assertion below.
    availability_deadline = time.time() + 30
    while time.time() < availability_deadline:
        try:
            resp = api_client.get(f"/v1/runs/{run_id}")
        except httpx.TransportError:
            resp = api_client.get(f"/v1/runs/{run_id}")
        assert resp.status_code == 200, f"API was unavailable during replica replacement: {resp.status_code}"
        time.sleep(2)

    wait_for_ready_replicas("api", 2, timeout=90)
    wait_for_ready_replicas("reconciler", 2, timeout=90)

    final_state = wait_for_terminal_state(api_client, run_id, timeout=180)
    assert final_state == "SUCCEEDED", f"run did not complete despite HA: ended in {final_state}"
