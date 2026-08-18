"""OIDC/RBAC unit tests (spec §33-34, §59, §67). Fully self-contained: a
throwaway RSA keypair signs a test JWT, a fake JWKS client hands back its
public key — no live IdP account needed, unlike the Databricks-style
blockers elsewhere in this project."""

import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

from api import auth

ISSUER = "https://test-idp.example.com/"
AUDIENCE = "portage-api"
KID = "test-key-1"


@pytest.fixture
def keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


@pytest.fixture
def fake_jwk_client(keypair, monkeypatch):
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


def _make_token(private_key, *, groups=(), sub="user-123", email="brad@example.com", **overrides):
    now = int(time.time())
    claims = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": sub,
        "email": email,
        "groups": list(groups),
        "iat": now,
        "exp": now + 300,
        **overrides,
    }
    return jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": KID})


def test_verify_token_returns_identity_with_mapped_roles(keypair, fake_jwk_client, monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_GROUP_ROLES", "spark-devs:Developer,platform-admins:PlatformAdmin")
    private_key, _ = keypair
    token = _make_token(private_key, groups=["spark-devs"])

    identity = auth.verify_token(token)

    assert identity.subject == "user-123"
    assert identity.email == "brad@example.com"
    assert identity.roles == frozenset({auth.ROLE_DEVELOPER})
    assert identity.source == "oidc"


def test_verify_token_ignores_unmapped_groups(keypair, fake_jwk_client, monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_GROUP_ROLES", "spark-devs:Developer")
    private_key, _ = keypair
    token = _make_token(private_key, groups=["some-other-group"])

    identity = auth.verify_token(token)

    assert identity.roles == frozenset()


def test_verify_token_rejects_expired_token(keypair, fake_jwk_client, monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_GROUP_ROLES", "")
    private_key, _ = keypair
    token = _make_token(private_key, exp=int(time.time()) - 60)

    with pytest.raises(jwt.ExpiredSignatureError):
        auth.verify_token(token)


def test_verify_token_rejects_wrong_issuer(keypair, fake_jwk_client, monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_GROUP_ROLES", "")
    private_key, _ = keypair
    token = _make_token(private_key, iss="https://not-the-real-idp.example.com/")

    with pytest.raises(jwt.InvalidIssuerError):
        auth.verify_token(token)


def test_verify_token_rejects_wrong_audience(keypair, fake_jwk_client, monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_GROUP_ROLES", "")
    private_key, _ = keypair
    token = _make_token(private_key, aud="some-other-api")

    with pytest.raises(jwt.InvalidAudienceError):
        auth.verify_token(token)


def test_verify_token_rejects_tampered_signature(keypair, fake_jwk_client, monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_GROUP_ROLES", "")
    private_key, _ = keypair
    token = _make_token(private_key)
    tampered = token[:-4] + ("AAAA" if token[-4:] != "AAAA" else "BBBB")

    with pytest.raises(jwt.InvalidSignatureError):
        auth.verify_token(tampered)


def test_verify_token_rejects_wrong_key_entirely(fake_jwk_client, monkeypatch):
    """Role-escalation-style attack: a token signed by a *different*
    private key than the one the fake JWKS client will hand back."""
    monkeypatch.setenv("PORTAGE_AUTH_GROUP_ROLES", "")
    other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    token = _make_token(other_key)

    with pytest.raises(jwt.InvalidSignatureError):
        auth.verify_token(token)


@pytest.mark.asyncio
async def test_get_current_identity_disabled_mode_ignores_header(monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_MODE", "disabled")
    identity = await auth.get_current_identity(authorization=None)
    assert identity is auth.UNAUTHENTICATED
    assert identity.source == "unauthenticated"


@pytest.mark.asyncio
async def test_get_current_identity_enforced_mode_requires_header(monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_MODE", "enforced")
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc_info:
        await auth.get_current_identity(authorization=None)
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_get_current_identity_enforced_mode_valid_token(keypair, fake_jwk_client, monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_MODE", "enforced")
    monkeypatch.setenv("PORTAGE_AUTH_GROUP_ROLES", "spark-devs:Developer")
    private_key, _ = keypair
    token = _make_token(private_key, groups=["spark-devs"])

    identity = await auth.get_current_identity(authorization=f"Bearer {token}")

    assert identity.roles == frozenset({auth.ROLE_DEVELOPER})


@pytest.mark.asyncio
async def test_get_current_identity_enforced_mode_invalid_token_is_401_not_500(
    keypair, fake_jwk_client, monkeypatch
):
    monkeypatch.setenv("PORTAGE_AUTH_MODE", "enforced")
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc_info:
        await auth.get_current_identity(authorization="Bearer not-a-real-jwt")
    assert exc_info.value.status_code == 401


def test_auth_mode_rejects_invalid_value(monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_MODE", "sort-of-enforced")
    with pytest.raises(auth.AuthConfigError):
        auth._auth_mode()


def test_group_role_map_rejects_unknown_role(monkeypatch):
    monkeypatch.setenv("PORTAGE_AUTH_GROUP_ROLES", "some-group:NotARealRole")
    with pytest.raises(auth.AuthConfigError):
        auth._group_role_map()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "caller_roles,minimum_role,should_pass",
    [
        (frozenset({auth.ROLE_VIEWER}), auth.ROLE_VIEWER, True),
        (frozenset({auth.ROLE_VIEWER}), auth.ROLE_OPERATOR, False),
        (frozenset({auth.ROLE_OPERATOR}), auth.ROLE_VIEWER, True),  # seniority
        (frozenset({auth.ROLE_DEVELOPER}), auth.ROLE_OPERATOR, False),
        (frozenset({auth.ROLE_PLATFORM_ADMIN}), auth.ROLE_OPERATOR, True),
        (frozenset(), auth.ROLE_VIEWER, False),  # role escalation: no role at all
    ],
)
async def test_require_role_enforces_seniority(monkeypatch, caller_roles, minimum_role, should_pass):
    from fastapi import HTTPException

    identity = auth.Identity(subject="x", email=None, roles=caller_roles, source="oidc")
    check = auth.require_role(minimum_role)

    if should_pass:
        result = await _call_check(check, identity)
        assert result is identity
    else:
        with pytest.raises(HTTPException) as exc_info:
            await _call_check(check, identity)
        assert exc_info.value.status_code == 403


async def _call_check(check, identity):
    # require_role() returns an async closure taking `identity` as a
    # Depends()-injected default — call it directly with an explicit
    # identity, bypassing FastAPI's DI container (unit-test seam).
    import inspect

    sig = inspect.signature(check)
    param_name = next(iter(sig.parameters))
    return await check(**{param_name: identity})
