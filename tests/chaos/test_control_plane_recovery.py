"""Control-plane and reconciler restart recovery (spec.md §71 Reliability
— "control-plane restart recovery"; spec §57's "mandatory v1.0 acceptance
test"). Each test temporarily scales its target Deployment to a single
replica so the kill isn't masked by the other HA replica picking up the
slack — that's deliberately a separate scenario
(test_ha_deployment.py) — and restores the original replica count
afterward regardless of outcome."""

import time

import httpx

from tests.chaos.conftest import (
    delete_pod,
    pod_names,
    port_forward_api,
    wait_for_ready_replicas,
    wait_for_terminal_state,
)


def test_reconciler_restart_recovers_in_flight_run(
    api_client, environment_name, workload_ref, single_reconciler_replica
):
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

    # Give the (single) reconciler a real chance to pick this run up and
    # start submitting it before killing it — a kill before any work
    # started would trivially "recover" without proving anything.
    deadline = time.time() + 30
    state = "ACCEPTED"
    while time.time() < deadline and state == "ACCEPTED":
        time.sleep(2)
        state = api_client.get(f"/v1/runs/{run_id}").json()["state"]

    pods_before = pod_names("reconciler")
    assert len(pods_before) == 1, f"expected exactly 1 reconciler pod, found {pods_before}"
    delete_pod(pods_before[0])

    # Kubernetes must reschedule a replacement — this is the Deployment's
    # own self-healing, not application logic, but it's the first half of
    # "restart recovery" being real rather than assumed.
    wait_for_ready_replicas("reconciler", 1, timeout=90)
    pods_after = pod_names("reconciler")
    assert pods_after != pods_before, "expected a genuinely new pod, not the same one surviving"

    final_state = wait_for_terminal_state(api_client, run_id, timeout=180)
    assert final_state == "SUCCEEDED", f"run did not recover cleanly: ended in {final_state}"

    # No duplicate/ghost submission: exactly the one run, one clean event
    # history ending in SUCCEEDED (not e.g. two SUBMITTING->RUNNING cycles
    # from being resubmitted after the kill).
    events = api_client.get(f"/v1/runs/{run_id}/events").json()
    succeeded_events = [e for e in events if e["to_state"] == "SUCCEEDED"]
    assert len(succeeded_events) == 1


def test_api_restart_does_not_lose_accepted_run(
    api_client, environment_name, workload_ref, single_api_replica
):
    """Kills the API pod immediately after it accepts a run — proving run
    state lives entirely in PostgreSQL, not pod-local memory, since the
    replacement pod (a cold process with no memory of the request that
    created this run) must still be able to serve it correctly."""
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

    pods_before = pod_names("api")
    assert len(pods_before) == 1
    delete_pod(pods_before[0])
    wait_for_ready_replicas("api", 1, timeout=90)
    pods_after = pod_names("api")
    assert pods_after != pods_before

    # api_client's session-scoped port-forward tunnel does not reliably
    # survive the window where the Service had zero ready backends
    # (confirmed live — see port_forward_api's docstring), so this opens
    # a genuinely fresh tunnel on a different port for the verification
    # itself, now that a replacement pod is confirmed ready.
    with port_forward_api(18132) as fresh_base_url, httpx.Client(base_url=fresh_base_url, timeout=30) as fresh_client:
        resp = fresh_client.get(f"/v1/runs/{run_id}")
    resp.raise_for_status()
    assert resp.json()["id"] == run_id
    assert resp.json()["state"] in {"ACCEPTED", "VALIDATING", "SUBMITTING", "QUEUED", "RUNNING", "SUCCEEDED"}
