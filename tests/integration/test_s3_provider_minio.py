"""Live integration test for the S3 provider against a real MinIO endpoint.

Not run in CI (tests/integration is excluded from the unit-only CI job —
see .github/workflows/ci.yml and docs/architecture/spec.md §53). Requires
MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY in the environment —
on the Phase 0 remote box these live in ~/.portage/minio.env.
"""

import os

import pytest

from providers.storage.s3.provider import S3ConnectionProfile, S3StorageProvider

pytestmark = pytest.mark.skipif(
    "MINIO_ENDPOINT" not in os.environ,
    reason="requires a live MinIO endpoint (MINIO_ENDPOINT env var)",
)


@pytest.fixture
def provider() -> S3StorageProvider:
    profile = S3ConnectionProfile(
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        endpoint_url=os.environ["MINIO_ENDPOINT"],
    )
    return S3StorageProvider(profile)


def test_health_check_against_real_minio(provider):
    assert provider.health_check() is True


def test_seeded_bucket_is_reachable(provider):
    client = provider._client()
    resp = client.list_objects_v2(Bucket="portage-phase0", Prefix="wordcount/")
    keys = [obj["Key"] for obj in resp.get("Contents", [])]
    assert "wordcount/input.txt" in keys
