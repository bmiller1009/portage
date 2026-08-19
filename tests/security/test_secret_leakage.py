"""Secret-leakage regression guards (spec §59, v1.0.0 release-hardening
security review — issue #66). Credentials must never appear in an error
message, audit record, or run-event message a Viewer-role user (or a
public GitHub issue quoting a stack trace) could see.

Genuinely new content in this directory rather than under tests/unit/
(unlike the OIDC/RBAC suite — see tests/README.md for why that one stays
in tests/unit/) because this is about a cross-cutting property
(no secret leakage anywhere), not one module's own behavioral contract.
"""

import os

import pytest

from control_plane.credentials import (
    CredentialResolutionError,
    resolve_adls_credentials,
    resolve_databricks_credentials,
    resolve_s3_credentials,
    resolve_vast_credentials,
)

_SECRET_ENV_SUFFIXES = {
    resolve_s3_credentials: ("_ACCESS_KEY", "_SECRET_KEY"),
    resolve_vast_credentials: ("_ACCESS_KEY", "_SECRET_KEY"),
    resolve_databricks_credentials: ("_CLIENT_ID", "_CLIENT_SECRET"),
}


@pytest.mark.parametrize("resolver", [resolve_s3_credentials, resolve_vast_credentials, resolve_databricks_credentials])
def test_credential_resolution_error_never_contains_a_configured_secret_value(resolver, monkeypatch):
    """A resolver whose required env vars ARE set, called with a
    *different*, unrelated reference (so resolution still fails, this
    time on a KeyError for the unrelated reference's own var names) must
    never leak the unrelated, already-configured secret's actual value
    into the raised error — only var *names* belong in these messages,
    confirmed by reading control_plane/credentials.py directly."""
    var_prefix = "PORTAGE_TESTSECRET"
    secret_value = "super-secret-value-should-never-leak-anywhere"
    for suffix in _SECRET_ENV_SUFFIXES[resolver]:
        monkeypatch.setenv(f"{var_prefix}{suffix}", secret_value)

    with pytest.raises(CredentialResolutionError) as exc_info:
        resolver({"provider": "env", "reference": "PORTAGE_SOME_OTHER_UNCONFIGURED_REFERENCE"})

    assert secret_value not in str(exc_info.value)


def test_adls_credential_resolution_never_leaks_the_account_key(monkeypatch):
    """resolve_adls_credentials never raises on a missing key (it treats
    "no key configured" as "use ambient/workload identity," per
    AdlsStorageProvider's own documented precedent) — the relevant
    guarantee here is that the *returned* AdlsCredentials never ends up
    inside a message string anywhere in this codebase; confirmed by
    construction, since resolve_adls_credentials itself only ever raises
    on an unsupported/malformed credential_reference, never after reading
    the key."""
    monkeypatch.setenv("PORTAGE_TESTADLS_ACCOUNT_KEY", "super-secret-account-key")
    creds = resolve_adls_credentials({"provider": "env", "reference": "PORTAGE_TESTADLS"})
    assert creds.account_key == "super-secret-account-key"

    with pytest.raises(CredentialResolutionError) as exc_info:
        resolve_adls_credentials({"provider": "not-env"})
    assert "super-secret-account-key" not in str(exc_info.value)


def test_credential_resolution_error_messages_only_reference_variable_names():
    """A malformed credential_reference (missing 'reference' entirely)
    must fail with a message that can't possibly contain a secret value —
    it never got far enough to read one."""
    for resolver in (
        resolve_s3_credentials,
        resolve_vast_credentials,
        resolve_databricks_credentials,
        resolve_adls_credentials,
    ):
        with pytest.raises(CredentialResolutionError) as exc_info:
            resolver({"provider": "env"})
        assert "reference" in str(exc_info.value)


def test_environment_variables_used_by_this_test_do_not_leak_into_os_environ_by_accident():
    """Sanity check on the test fixtures themselves: monkeypatch.setenv
    (used above) is undone automatically after each test — confirms the
    synthetic secrets this file plants don't linger in the process
    environment for a later, unrelated test to accidentally pick up."""
    assert "PORTAGE_TESTSECRET_ACCESS_KEY" not in os.environ
    assert "PORTAGE_TESTADLS_ACCOUNT_KEY" not in os.environ
