# tests/security

Security tests (`docs/architecture/spec.md` §59): unauthenticated API access, expired JWT, invalid issuer, wrong audience, role escalation, cross-project access, secret leakage, credential logging, malicious artifact URI, path traversal, configuration injection, Spark-conf injection, SSRF, malformed provider payload, audit completeness.

**Covered, as of v1.0.0**:
- Unauthenticated access, expired JWT, wrong issuer/audience, insufficient role, and explicit role-escalation attempts: `tests/unit/test_auth.py` + `tests/unit/test_api_auth_enforcement.py` (kept there rather than moved here — see `tests/README.md` for why; this is OIDC/RBAC's own behavioral contract, fast and DB-free, the same category as everything else under `tests/unit/`).
- Secret leakage / credential logging: `test_secret_leakage.py` in *this* directory — genuinely new content here rather than under `tests/unit/`, since it's a cross-cutting property (no secret leaks anywhere) rather than one module's behavioral contract. Covers credential-resolution error messages (S3/VAST/Databricks/ADLS) never containing a configured secret's actual value. The Kubernetes-provider-specific finding this pass also produced (`ApiException.__str__()` embedding raw response headers/body) is regression-tested where it was fixed, `tests/unit/test_kubernetes_provider.py::test_submit_error_message_omits_raw_response_headers_and_body`.

**Not yet covered — genuinely open, not silently skipped**: cross-project access, malicious artifact URI, path traversal, configuration injection, Spark-conf injection, SSRF, malformed provider payload, audit completeness. Tracked for future work; do not assume these are tested just because this directory now has content.
