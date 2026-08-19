# tests/security

Security tests (`docs/architecture/spec.md` §59): unauthenticated API access, expired JWT, invalid issuer, wrong audience, role escalation, cross-project access, secret leakage, credential logging, malicious artifact URI, path traversal, configuration injection, Spark-conf injection, SSRF, malformed provider payload, audit completeness.

OIDC/RBAC landed in v0.4 (`api/auth.py`) and a subset of this list is already covered by `tests/unit/test_auth.py`/`test_api_auth_enforcement.py` (unauthenticated access, expired JWT, wrong issuer/audience, insufficient role). The remaining scenarios (role escalation attempts, secret leakage/credential logging, malicious artifact URI, path traversal, Spark-conf injection, SSRF, malformed provider payload, audit completeness) are tracked in [#66](https://github.com/bmiller1009/portage/issues/66).
