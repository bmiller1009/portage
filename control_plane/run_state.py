"""Canonical run lifecycle states (docs/architecture/spec.md §23).

Provider-native states are mapped into this enum so client code never has
to reason about SparkApplication currentStateSummary values, Databricks
Jobs API life_cycle_state/result_state pairs, or any other provider-specific
vocabulary directly.
"""

from enum import Enum


class RunState(str, Enum):
    ACCEPTED = "ACCEPTED"
    VALIDATING = "VALIDATING"
    SUBMITTING = "SUBMITTING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELING = "CANCELING"
    CANCELED = "CANCELED"
    UNKNOWN = "UNKNOWN"
    LOST = "LOST"


TERMINAL_STATES = {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELED, RunState.LOST}
