"""HA-specific fault injection (spec.md §71 Reliability — "HA deployment"):
with both the API and reconciler at their normal 2-replica configuration
(charts/portage's default, unlike test_control_plane_recovery.py's
single-replica scenarios), kill one of each mid-run and confirm the run
still reaches a terminal state with no visible availability gap — the
other replica of each keeps serving throughout, which is the entire point
of running 2 in the first place.

HISTORICAL FINDING, now mitigated (#57): this test used to fail
intermittently (observed roughly every other run) with a failure distinct
from the two bugs this milestone's other work fixed (MaxRetryError
misclassification in providers/execution/kubernetes/provider.py; the
"unclassified error" mislabeling in reconciler/service.py's
poll_active_runs). The run would correctly submit (QUEUED) and its first
status() poll would succeed, but a *later* status() poll against the
same still-existing SparkApplication occasionally got a genuine `(401)
Unauthorized` from the real Kubernetes API server — not a connection
failure, a real signed response.

Ruled out live, not guessed, as the mechanism:
- Global-Configuration-object races from concurrent load_kube_config()
  calls across different ExecutionProfiles (an asyncio.gather
  reproduction inside the real reconciler pod never showed
  cross-contamination — each CustomObjectsApi instance's
  configuration.host stayed correctly scoped throughout).
- A freshly-created pod's projected ServiceAccount token not being ready
  yet (a brand-new pod's very first API call, tested immediately after
  Running, succeeded cleanly every time — no propagation delay observed).
- reconciler/service.py's own control flow is strictly sequential
  (asyncio.run(run_forever(...)), one tick fully awaited before the
  next) — no concurrency *within* one reconciler process.
- Two reconciler replicas concurrently polling the *same* resource: 400
  concurrent get_namespaced_custom_object calls fired from two real pods
  against the same SparkApplication, in a tight loop, produced zero
  errors.

The exact server-side trigger was never pinned down. Given every
isolated reproduction attempt came back clean, but the failure was
real and reproducible in the actual chaos scenario, providers/execution/
kubernetes/provider.py now includes 401 in _RETRYABLE_API_STATUS_CODES —
a live-evidence-driven, bounded accommodation (see that constant's own
comment for the full reasoning), not a general "401s are safe to
retry" policy. Empirically: 4 consecutive live runs of this exact test
passed cleanly after the change, versus roughly half failing with this
exact signature before it."""

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
