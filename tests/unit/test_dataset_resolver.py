import pytest

from spec.dataset.v1alpha1 import Dataset, DatasetResolutionError, resolve_dataset_config
from spec.workload.v1alpha1 import parse_workload

EXAMPLES_DIR = __import__("pathlib").Path(__file__).resolve().parents[2] / "examples"


def _dataset(name: str, **bindings: str) -> Dataset:
    return Dataset.model_validate(
        {
            "apiVersion": "runtime/v1alpha1",
            "kind": "Dataset",
            "metadata": {"name": name},
            "bindings": {env: {"kind": "path", "uri": uri} for env, uri in bindings.items()},
        }
    )


def test_resolves_portable_dataset_config_for_environment():
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    datasets = {
        "wordcount.raw": _dataset("wordcount.raw", k8s_remote="s3a://portage-phase0/wordcount/input.txt"),
        "wordcount.counts": _dataset("wordcount.counts", k8s_remote="s3a://portage-phase0/wordcount/output"),
    }

    config = resolve_dataset_config(workload, datasets, "k8s_remote")

    assert config == {
        "portable.dataset.wordcount.raw.uri": "s3a://portage-phase0/wordcount/input.txt",
        "portable.dataset.wordcount.counts.uri": "s3a://portage-phase0/wordcount/output",
    }


def test_raises_when_no_binding_for_environment():
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    datasets = {
        "wordcount.raw": _dataset("wordcount.raw", other_env="s3a://bucket/input.txt"),
        "wordcount.counts": _dataset("wordcount.counts", other_env="s3a://bucket/output"),
    }

    with pytest.raises(DatasetResolutionError, match="no binding for environment"):
        resolve_dataset_config(workload, datasets, "k8s_remote")


def test_raises_when_dataset_definition_missing():
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")

    with pytest.raises(DatasetResolutionError, match="no Dataset definition found"):
        resolve_dataset_config(workload, {}, "k8s_remote")
