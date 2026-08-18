"""Resolves a StorageProfile.credential_reference to actual credential
values (docs/architecture/spec.md §35 — secrets are never persisted raw,
only a reference to where they can be resolved). Only the "env" provider
is implemented: real secret managers (Kubernetes Secrets, cloud KMS/vault
services) are future work, per spec §35's own "potential providers" list.
"""

import os


class CredentialResolutionError(Exception):
    pass


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
