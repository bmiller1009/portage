"""Idempotent submission under real concurrency (spec.md §71 Reliability
— "idempotent submission"; spec §25). control_plane/run_service.py's
create_run() already has sequential-idempotency-key coverage; this proves
the same guarantee holds under genuine concurrent requests racing across
the two real API replicas the Helm chart deploys — not two calls in the
same process."""

import uuid
from concurrent.futures import ThreadPoolExecutor

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
