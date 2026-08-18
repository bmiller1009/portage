"""OIDC authentication + RBAC authorization (docs/architecture/spec.md
§33-34, §36, §59, §67).

Human auth: JWT verification against the issuer's JWKS endpoint (spec
§33 delegates identity entirely to an external IdP — Entra/Okta/Ping —
never a proprietary user store here). Machine auth (spec §34) needing a
Bearer JWT — e.g. an OAuth2 client-credentials grant from CI/automation —
verifies identically on this side; only the token's issuance differs.

RBAC: the five spec roles, derived from the JWT's `groups` claim via a
configured group->role mapping (PORTAGE_AUTH_GROUP_ROLES). Roles form a
real hierarchy (Viewer < Analyst < Developer < Operator < PlatformAdmin,
each senior role able to do everything a junior one can), not just
disjoint labels — require_role(minimum_role) checks the caller's highest
mapped role against a minimum rank, per spec §33's own "Analyst" through
"PlatformAdmin" progression.

Enforcement is configurable (PORTAGE_AUTH_MODE=disabled|enforced,
default disabled) — same dev-only-by-default precedent already set by
this API's CORS stance (api/main.py) — so the CLI/UI/existing
live-verification workflow keeps working unchanged unless a deployment
opts in. When disabled, every caller is treated as PlatformAdmin (every
require_role() check trivially passes) and audit records a clearly
labeled "unauthenticated" source rather than fabricating an identity.
"""

import os
from dataclasses import dataclass
from functools import lru_cache

import jwt
from fastapi import Depends, Header, HTTPException
from jwt import PyJWKClient

ROLE_VIEWER = "Viewer"
ROLE_ANALYST = "Analyst"
ROLE_DEVELOPER = "Developer"
ROLE_OPERATOR = "Operator"
ROLE_PLATFORM_ADMIN = "PlatformAdmin"

# Cumulative seniority (spec §33's own role progression) — a caller with
# any role at or above the endpoint's minimum passes, not exact-match.
_ROLE_RANK: dict[str, int] = {
    ROLE_VIEWER: 0,
    ROLE_ANALYST: 1,
    ROLE_DEVELOPER: 2,
    ROLE_OPERATOR: 3,
    ROLE_PLATFORM_ADMIN: 4,
}


class AuthConfigError(Exception):
    pass


@dataclass(frozen=True)
class Identity:
    subject: str
    email: str | None
    roles: frozenset[str]
    # spec §36's audit "source" field — "oidc" for a real verified token,
    # "unauthenticated" when PORTAGE_AUTH_MODE=disabled, never fabricated.
    source: str


# Used only when PORTAGE_AUTH_MODE=disabled — the single highest role
# satisfies every require_role() check by construction (max-rank
# comparison), the same practical effect as "every role" without needing
# a separate bypass branch in require_role() itself.
UNAUTHENTICATED = Identity(
    subject="unauthenticated", email=None, roles=frozenset({ROLE_PLATFORM_ADMIN}), source="unauthenticated"
)


def _auth_mode() -> str:
    mode = os.environ.get("PORTAGE_AUTH_MODE", "disabled")
    if mode not in ("disabled", "enforced"):
        raise AuthConfigError(f"invalid PORTAGE_AUTH_MODE: {mode!r} (expected 'disabled' or 'enforced')")
    return mode


def _group_role_map() -> dict[str, str]:
    """PORTAGE_AUTH_GROUP_ROLES="platform-admins:PlatformAdmin,spark-devs:Developer"
    -> {"platform-admins": "PlatformAdmin", "spark-devs": "Developer"}."""
    raw = os.environ.get("PORTAGE_AUTH_GROUP_ROLES", "")
    mapping: dict[str, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair:
            continue
        group, _, role = pair.partition(":")
        if role not in _ROLE_RANK:
            raise AuthConfigError(f"unknown role in PORTAGE_AUTH_GROUP_ROLES: {role!r}")
        mapping[group] = role
    return mapping


@lru_cache(maxsize=1)
def _jwk_client() -> PyJWKClient:
    jwks_uri = os.environ["PORTAGE_OIDC_JWKS_URI"]
    return PyJWKClient(jwks_uri)


def verify_token(token: str) -> Identity:
    """Real JWT verification — signature (against the issuer's live JWKS),
    expiration, issuer, audience. Raises a jwt.PyJWTError subclass on any
    failure; callers translate that into a 401, never a 500."""
    issuer = os.environ["PORTAGE_OIDC_ISSUER"]
    audience = os.environ["PORTAGE_OIDC_AUDIENCE"]
    signing_key = _jwk_client().get_signing_key_from_jwt(token)
    claims = jwt.decode(
        token, signing_key.key, algorithms=["RS256"], audience=audience, issuer=issuer
    )
    role_map = _group_role_map()
    roles = frozenset(role_map[g] for g in claims.get("groups", []) if g in role_map)
    return Identity(subject=claims["sub"], email=claims.get("email"), roles=roles, source="oidc")


async def get_current_identity(authorization: str | None = Header(default=None)) -> Identity:
    if _auth_mode() == "disabled":
        return UNAUTHENTICATED
    if authorization is None or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing or invalid Authorization header")
    token = authorization.removeprefix("Bearer ")
    try:
        return verify_token(token)
    except jwt.PyJWTError as e:
        raise HTTPException(status_code=401, detail=f"invalid token: {e}") from e


def require_role(minimum_role: str):
    """FastAPI dependency factory — Depends(require_role(ROLE_OPERATOR))
    403s any caller whose highest mapped role ranks below minimum_role. A
    caller with zero mapped roles (valid token, no recognized group) has
    no rank at all and is always rejected by an enforced check."""
    minimum_rank = _ROLE_RANK[minimum_role]

    async def _check(identity: Identity = Depends(get_current_identity)) -> Identity:
        if not identity.roles or max(_ROLE_RANK[r] for r in identity.roles) < minimum_rank:
            raise HTTPException(status_code=403, detail=f"requires role '{minimum_role}' or higher")
        return identity

    return _check
