"""Idempotent submission under real concurrency (spec.md §71 Reliability
— "idempotent submission"; spec §25). control_plane/run_service.py's
create_run() already has sequential-idempotency-key coverage; this proves
the same guarantee holds under genuine concurrent requests racing across
the two real API replicas the Helm chart deploys — not two calls in the
same process."""

import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx

from tests.chaos.conftest import unique


def test_concurrent_duplicate_submission_creates_exactly_one_run(api_client, environment_name, workload_ref):
    workload_name, workload_version = workload_ref
    idempotency_key = unique("chaos-idem")

    def submit(_: int):
        return api_client.post(
            "/v1/runs",
            json={
                "workload_name": workload_name,
                "workload_version": workload_version,
                "environment_name": environment_name,
            },
            headers={"Idempotency-Key": idempotency_key},
        )

    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(submit, range(10)))

    for resp in responses:
        assert resp.status_code in (200, 202), f"unexpected status {resp.status_code}: {resp.text}"

    run_ids = {resp.json()["id"] for resp in responses}
    assert len(run_ids) == 1, f"expected exactly one run, got {len(run_ids)}: {run_ids}"

    # Exactly one of the ten concurrent requests should be the "real"
    # creation (202); every other one is a replay of the same run (200).
    created = [r for r in responses if r.status_code == 202]
    replayed = [r for r in responses if r.status_code == 200]
    assert len(created) == 1
    assert len(replayed) == 9

    # And the winning run_id is a real, valid run id (proves this wasn't
    # just returning garbage that happened to be equal).
    uuid.UUID(run_ids.pop())


def test_retry_after_client_side_timeout_creates_exactly_one_run(
    api_client, environment_name, workload_ref
):
    """spec §71's "retry after network timeout -> no duplicate execution"
    — distinct from the concurrent-race case above: here the *client's
    own* connection times out waiting for the response (it never sees
    whether the first attempt succeeded), not two requests racing each
    other. A real client faced with this has to retry not knowing if the
    first attempt landed — proving that's safe is the whole point of
    idempotency keys existing at all (spec §25)."""
    workload_name, workload_version = workload_ref
    idempotency_key = unique("chaos-idem-timeout")

    # A read timeout tight enough that the client gives up before seeing
    # the response, but loose enough that the request genuinely reaches
    # the real remote API first — POST /v1/runs only ever persists
    # ACCEPTED and returns (spec §24, no synchronous provider call), so
    # the server-side operation completes and commits regardless of
    # whether this client is still listening for the reply.
    try:
        api_client.post(
            "/v1/runs",
            json={
                "workload_name": workload_name,
                "workload_version": workload_version,
                "environment_name": environment_name,
            },
            headers={"Idempotency-Key": idempotency_key},
            timeout=httpx.Timeout(connect=10.0, read=0.001, write=10.0, pool=10.0),
        )
        # If it didn't time out (a very fast round trip), that's fine too
        # — the retry below still has to be safe either way.
    except httpx.TimeoutException:
        pass

    # The client, not knowing whether the first attempt landed, retries
    # with the identical Idempotency-Key — a normal timeout this time.
    retry_resp = api_client.post(
        "/v1/runs",
        json={
            "workload_name": workload_name,
            "workload_version": workload_version,
            "environment_name": environment_name,
        },
        headers={"Idempotency-Key": idempotency_key},
    )
    assert retry_resp.status_code in (200, 202), f"unexpected status: {retry_resp.text}"
    run_id = retry_resp.json()["id"]

    # A second retry (simulating the client retrying yet again, e.g. it
    # timed out again) must land on the exact same run, not a new one.
    second_retry = api_client.post(
        "/v1/runs",
        json={
            "workload_name": workload_name,
            "workload_version": workload_version,
            "environment_name": environment_name,
        },
        headers={"Idempotency-Key": idempotency_key},
    )
    assert second_retry.status_code == 200, "a true replay must never be 202 (a fresh creation)"
    assert second_retry.json()["id"] == run_id
