"""Run failure taxonomy (v1.0.0 release-hardening). Before this, every
failed run carried only a free-text `RunEvent.message` — useful for a
human reading the events log, but nothing an operator's tooling could
branch on ("is this retryable?", "is this my workload's fault or the
provider's?"). `category`/`disposition` make that distinction structured
and queryable, derived from the exception hierarchy that already existed
(`RetryableProviderError`/`TerminalProviderError`, spec §26) rather than
replacing it.
"""

from typing import Literal

Category = Literal[
    "VALIDATION",
    "PROVIDER_SUBMISSION",
    "PROVIDER_TRANSIENT",
    "WORKLOAD_EXECUTION",
    "CONTROL_PLANE",
    "STORAGE_RESOLUTION",
    "AUTHORIZATION",
    "UNKNOWN_RECONCILIATION",
]

Disposition = Literal["retryable", "terminal", "user_action_required", "provider_action_required"]
