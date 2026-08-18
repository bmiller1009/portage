"""Resolves a StorageProfile.credential_reference to actual credential
values (docs/architecture/spec.md §35 — secrets are never persisted raw,
only a reference to where they can be resolved). Only the "env" provider
is implemented: real secret managers (Kubernetes Secrets, cloud KMS/vault
services) are future work, per spec §35's own "potential providers" list.
"""

import os
from dataclasses import dataclass


class CredentialResolutionError(Exception):
    pass


@dataclass
class DatabricksCredentials:
    # OAuth M2M (spec §66) — Databricks' unified-auth SDK takes these
    # directly as WorkspaceClient(host=..., client_id=..., client_secret=...).
    client_id: str
    client_secret: str


@dataclass
class AdlsCredentials:
    # None means "use workload identity" (spec §50: preferred over static
    # storage keys) rather than a static account key — ADLS's credential
    # shape genuinely isn't a key pair like S3/VAST's, so this can't reuse
    # _resolve_env_key_pair()'s tuple[str, str] return type.
    account_key: str | None


def _resolve_env_key_pair(credential_reference: dict) -> tuple[str, str]:
    provider = credential_reference.get("provider")
    if provider != "env":
        raise CredentialResolutionError(f"unsupported credential provider: {provider}")

    reference = credential_reference.get("reference")
    if not reference:
        raise CredentialResolutionError("credential_reference is missing 'reference'")

    access_key_var = f"{reference}_ACCESS_KEY"
    secret_key_var = f"{reference}_SECRET_KEY"
    try:
        return os.environ[access_key_var], os.environ[secret_key_var]
    except KeyError as e:
        raise CredentialResolutionError(
            f"credential reference '{reference}' requires {access_key_var} and "
            f"{secret_key_var} to be set in the reconciler's environment"
        ) from e


def resolve_s3_credentials(credential_reference: dict) -> tuple[str, str]:
    """{"provider": "env", "reference": "PORTAGE_MINIO"} ->
    (os.environ["PORTAGE_MINIO_ACCESS_KEY"], os.environ["PORTAGE_MINIO_SECRET_KEY"])
    """
    return _resolve_env_key_pair(credential_reference)


def resolve_vast_credentials(credential_reference: dict) -> tuple[str, str]:
    """VAST S3 mode uses the same key-pair auth model as S3 (spec §48) —
    same env-var-suffix convention as resolve_s3_credentials()."""
    return _resolve_env_key_pair(credential_reference)


def resolve_databricks_credentials(credential_reference: dict) -> DatabricksCredentials:
    """{"provider": "env", "reference": "PORTAGE_DATABRICKS"} ->
    DatabricksCredentials(os.environ["PORTAGE_DATABRICKS_CLIENT_ID"],
    os.environ["PORTAGE_DATABRICKS_CLIENT_SECRET"]) — OAuth M2M's client
    ID/secret pair isn't a key pair in the S3 sense, but the same
    provider/reference/env-var-suffix convention still applies, so this
    doesn't reuse _resolve_env_key_pair() (different suffixes) but mirrors
    its shape."""
    provider = credential_reference.get("provider")
    if provider != "env":
        raise CredentialResolutionError(f"unsupported credential provider: {provider}")

    reference = credential_reference.get("reference")
    if not reference:
        raise CredentialResolutionError("credential_reference is missing 'reference'")

    client_id_var = f"{reference}_CLIENT_ID"
    client_secret_var = f"{reference}_CLIENT_SECRET"
    try:
        return DatabricksCredentials(
            client_id=os.environ[client_id_var], client_secret=os.environ[client_secret_var]
        )
    except KeyError as e:
        raise CredentialResolutionError(
            f"credential reference '{reference}' requires {client_id_var} and "
            f"{client_secret_var} to be set in the reconciler's environment"
        ) from e


def resolve_adls_credentials(credential_reference: dict) -> AdlsCredentials:
    """{"provider": "env", "reference": "PORTAGE_ADLS"} -> AdlsCredentials
    from os.environ.get("PORTAGE_ADLS_ACCOUNT_KEY") — absent (not a
    KeyError, since this one's optional) means workload identity."""
    provider = credential_reference.get("provider")
    if provider != "env":
        raise CredentialResolutionError(f"unsupported credential provider: {provider}")

    reference = credential_reference.get("reference")
    if not reference:
        raise CredentialResolutionError("credential_reference is missing 'reference'")

    return AdlsCredentials(account_key=os.environ.get(f"{reference}_ACCOUNT_KEY"))
