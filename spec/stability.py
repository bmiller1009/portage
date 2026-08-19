"""Shared apiVersion deprecation helper (docs/architecture/STABILITY.md).

runtime/v1alpha1 is the pre-GA name for the workload/environment/dataset/
artifact schemas; runtime/v1 is the stable, promoted name (same field
shapes — this was a rename, not a redesign, confirmed by diffing each
schema module against its own v1alpha1 history before promotion). Both
strings still parse, so nothing that already targets v1alpha1 breaks;
this just makes the old name's deprecation visible instead of silent.
"""

import warnings

STABLE = "runtime/v1"
DEPRECATED = "runtime/v1alpha1"


def warn_if_deprecated(api_version: str, kind: str) -> None:
    if api_version == DEPRECATED:
        warnings.warn(
            f"apiVersion '{DEPRECATED}' is deprecated for {kind}; use '{STABLE}' instead.",
            DeprecationWarning,
            stacklevel=3,
        )
