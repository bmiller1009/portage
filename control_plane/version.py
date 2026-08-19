"""Single authoritative software-version source (v1.0.0 release-hardening —
docs/architecture/spec.md's release-identity section). `pyproject.toml`'s
`[project].version` is authoritative; the CLI and the API's `GET /v1/build`
both read through here rather than each carrying an independently-hardcoded
copy, so there is exactly one place version drift could reappear.

git_sha/build_time are not knowable from installed package metadata — they
come from the Docker image's build-time ARGs (see Dockerfile), surfaced as
env vars. A source checkout or editable dev install has neither, so both
report "unknown" rather than guessing.
"""

import os
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _installed_version

from spec.stability import STABLE as WORKLOAD_SPEC_VERSION

_DISTRIBUTION_NAME = "portage-runtime"


def get_version() -> str:
    try:
        return _installed_version(_DISTRIBUTION_NAME)
    except PackageNotFoundError:
        return "0.0.0+unknown"


def get_git_sha() -> str:
    return os.environ.get("PORTAGE_GIT_SHA", "unknown")


def get_build_time() -> str:
    return os.environ.get("PORTAGE_BUILD_TIME", "unknown")


def get_workload_spec_version() -> str:
    return WORKLOAD_SPEC_VERSION
