"""End-to-end proof that PORTAGE_AUTH_MODE=enforced actually gates real
routes through FastAPI's dependency injection (not just api/auth.py's
functions in isolation, tested in tests/unit/test_auth.py) — and that the
default (disabled) mode leaves every existing unauthenticated call
working exactly as before. Same self-signed-JWT approach as test_auth.py,
no live IdP needed."""

import time
from unittest.mock import AsyncMock

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from jwt.algorithms import RSAAlgorithm

from api import auth
from api.main import app
from control_plane import repositories
from control_plane.db import get_db_session
from control_plane.models import Environment

ISSUER = "https://test-idp.example.com/"
AUDIENCE = "portage-api"
KID = "enforcement-test-key"


async def _fake_session():
    yield None


app.dependency_overrides[get_db_session] = _fake_session
client = TestClient(app)


@pytest.fixture
def keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


@pytest.fixture(autouse=True)
def _wire_fake_jwks(keypair, monkeypatch):
    _private_key, public_key = keypair
    jwk_dict = RSAAlgorithm.to_jwk(public_key, as_dict=True)
    jwk_dict["kid"] = KID
    signing_key = jwt.PyJWK(jwk_dict)

    class FakeJWKClient:
        def get_signing_key_from_jwt(self, token):
            return signing_key

    monkeypatch.setattr(auth, "_jwk_client", lambda: FakeJWKClient())
    monkeypatch.setenv("PORTAGE_OIDC_JWKS_URI", "https://test-idp.example.com/jwks")
    monkeypatch.setenv("PORTAGE_OIDC_ISSUER", ISSUER)
    monkeypatch.setenv("PORTAGE_OIDC_AUDIENCE", AUDIENCE)


def _token(private_key, *, groups=()):
    now = int(time.time())
    claims = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "test-user",
        "email": "brad@example.com",
        "groups": list(groups),
        "iat": now,
        "exp": now + 300,
    }
    return jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": KID})


def test_disabled_mode_allows_unauthenticated_requests(monkeypatch):
    """The default — every existing CLI/UI/live-verification call must
    keep working with zero Authorization header, exactly as before this
    issue."""
    monkeypatch.setenv("PORTAGE_AUTH_MODE", "disabled")
    monkeypatch.setattr(repositories, "list_environments", AsyncMock(return_value=[]))

    resp = client.get("/v1/environments")

    assert resp.status_code == 200


def test_enforced_mode_rejects_unauthenticated_request(monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_MODE", "enforced")

    resp = client.get("/v1/environments")

    assert resp.status_code == 401


def test_enforced_mode_allows_sufficient_role(keypair, monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_MODE", "enforced")
    monkeypatch.setenv("PORTAGE_AUTH_GROUP_ROLES", "viewers:Viewer")
    monkeypatch.setattr(repositories, "list_environments", AsyncMock(return_value=[]))
    private_key, _ = keypair
    token = _token(private_key, groups=["viewers"])

    resp = client.get("/v1/environments", headers={"Authorization": f"Bearer {token}"})

    assert resp.status_code == 200


def test_enforced_mode_403s_insufficient_role_for_privileged_action(keypair, monkeypatch):
    """Role escalation coverage (spec §59): a Viewer-only token must not
    be able to create an Environment (Operator+)."""
    monkeypatch.setenv("PORTAGE_AUTH_MODE", "enforced")
    monkeypatch.setenv("PORTAGE_AUTH_GROUP_ROLES", "viewers:Viewer")
    private_key, _ = keypair
    token = _token(private_key, groups=["viewers"])

    resp = client.post(
        "/v1/environments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "name": "x",
            "execution_provider": "kubernetes",
            "execution_profile_name": "y",
            "storage_provider": "s3",
            "storage_profile_name": "z",
        },
    )

    assert resp.status_code == 403


def test_enforced_mode_allows_senior_role_for_privileged_action(keypair, monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_MODE", "enforced")
    monkeypatch.setenv("PORTAGE_AUTH_GROUP_ROLES", "platform-team:Operator")
    monkeypatch.setattr(
        repositories,
        "create_environment",
        AsyncMock(
            return_value=Environment(
                name="x",
                execution_provider="kubernetes",
                execution_profile_name="y",
                storage_provider="s3",
                storage_profile_name="z",
            )
        ),
    )
    private_key, _ = keypair
    token = _token(private_key, groups=["platform-team"])

    resp = client.post(
        "/v1/environments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "name": "x",
            "execution_provider": "kubernetes",
            "execution_profile_name": "y",
            "storage_provider": "s3",
            "storage_profile_name": "z",
        },
    )

    assert resp.status_code == 201


def test_enforced_mode_rejects_tampered_token(keypair, monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_MODE", "enforced")
    private_key, _ = keypair
    token = _token(private_key)
    tampered = token[:-4] + ("AAAA" if token[-4:] != "AAAA" else "BBBB")

    resp = client.get("/v1/environments", headers={"Authorization": f"Bearer {tampered}"})

    assert resp.status_code == 401
