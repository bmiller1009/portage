"""Provider outage recovery (spec.md §71 Reliability — "provider outage
recovery"). Simulates the Kubernetes execution provider's own API being
genuinely unreachable (a network partition, not just an error response) via
a throwaway reconciler pod whose kubeconfig points at a non-routable
address from the moment it's created — then kills that pod and lets the
real, correctly-configured reconciler take over, proving a run submitted
during the outage waits it out and completes rather than failing outright
the first time the provider can't be reached."""

import time

from tests.chaos.conftest import (
    FULLNAME,
    UNREACHABLE_SERVER,
    kubectl,
    start_outage_test_reconciler,
    stop_outage_test_reconciler,
    wait_for_pod_count,
    wait_for_ready_replicas,
    wait_for_terminal_state,
)


def _scale_reconciler_to_zero() -> None:
    kubectl("scale", f"deployment/{FULLNAME}-reconciler", "--replicas=0")
    # Waiting for readyReplicas to empty is not enough on its own — see
    # wait_for_pod_count's docstring for the confirmed-live race this
    # avoids.
    wait_for_ready_replicas("reconciler", 0)
    wait_for_pod_count("reconciler", 0)


def _scale_reconciler_to_two() -> None:
    kubectl("scale", f"deployment/{FULLNAME}-reconciler", "--replicas=2")
    wait_for_ready_replicas("reconciler", 2)


def test_run_survives_provider_outage_and_recovers(api_client, environment_name, workload_ref):
    # The real reconciler must not also be processing runs while the
    # throwaway outage-test pod is active, or it would race to submit
    # this run correctly before the outage scenario has a chance to prove
    # anything.
    _scale_reconciler_to_zero()
    pod_name, configmap_name = start_outage_test_reconciler(UNREACHABLE_SERVER)
    try:
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

        # The provider is unreachable, so the run must NOT fail outright.
        # urllib3 retries the underlying TCP connect internally before
        # MaxRetryError is even raised (confirmed live this can take well
        # over a minute against a genuinely non-routable address, not
        # just _API_REQUEST_TIMEOUT_SECONDS's 30s) — the exact retry
        # cadence isn't this test's concern (that's
        # test_submit_raises_retryable_when_api_server_unreachable's job,
        # a fast, precise unit test); this test's job is the end-to-end
        # property: never FAILED while the outage lasts, watched
        # continuously so a brief FAILED blip can't be missed by only
        # sampling occasionally.
        deadline = time.time() + 60
        while time.time() < deadline:
            state = api_client.get(f"/v1/runs/{run_id}").json()["state"]
            assert state != "FAILED", (
                "run failed outright during the outage — a provider being unreachable "
                "should be retried, not treated as a confirmed execution failure"
            )
            assert state in {"ACCEPTED", "SUBMITTING"}, f"unexpected state during outage: {state}"
            time.sleep(3)
    finally:
        stop_outage_test_reconciler(pod_name, configmap_name)
        _scale_reconciler_to_two()

    final_state = wait_for_terminal_state(api_client, run_id, timeout=180)
    assert final_state == "SUCCEEDED", f"run did not recover after the outage cleared: ended in {final_state}"
