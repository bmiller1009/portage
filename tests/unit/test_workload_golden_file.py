"""Golden-file round-trip test for the portable workload schema (spec §26:
"Do not silently mutate v1 semantics in future releases" — golden-file
tests for workload parsing/serialization). Any accidental field rename,
removal, or shape change to spec/workload/v1alpha1.py's SparkWorkload will
fail this test loudly, forcing the golden file to be regenerated
deliberately (and reviewed in the same PR) rather than drifting silently.

To regenerate after an intentional, reviewed schema change:
    python3 -c "
    import json
    from spec.workload.v1alpha1 import parse_workload
    w = parse_workload('examples/claims-normalization.yaml')
    print(json.dumps(w.model_dump(mode='json'), indent=2, sort_keys=True))
    " > tests/unit/testdata/golden_workload.json
"""

import json
from pathlib import Path

from spec.workload.v1alpha1 import SparkWorkload, parse_workload

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"
GOLDEN_PATH = Path(__file__).resolve().parent / "testdata" / "golden_workload.json"


def test_serialized_shape_matches_golden_file():
    workload = parse_workload(EXAMPLES_DIR / "claims-normalization.yaml")
    actual = json.loads(json.dumps(workload.model_dump(mode="json"), sort_keys=True))
    golden = json.loads(GOLDEN_PATH.read_text())
    assert actual == golden


def test_golden_file_round_trips_through_the_schema_unchanged():
    """Parse -> serialize -> re-parse -> serialize again must be a fixed
    point — no field should be lost, defaulted differently, or reordered
    across a second pass."""
    golden = json.loads(GOLDEN_PATH.read_text())
    once = SparkWorkload.model_validate(golden)
    twice = SparkWorkload.model_validate(once.model_dump(mode="json"))
    assert once.model_dump(mode="json") == twice.model_dump(mode="json")


def test_provider_overrides_round_trips():
    golden = json.loads(GOLDEN_PATH.read_text())
    golden = {**golden, "providerOverrides": {"kubernetes": {"nodeSelector": {"disktype": "ssd"}}}}
    workload = SparkWorkload.model_validate(golden)
    assert workload.providerOverrides == {"kubernetes": {"nodeSelector": {"disktype": "ssd"}}}
    assert workload.model_dump(mode="json")["providerOverrides"] == {
        "kubernetes": {"nodeSelector": {"disktype": "ssd"}}
    }
