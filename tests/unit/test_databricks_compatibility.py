"""providers/execution/databricks/compatibility.py — the curated Spark <->
Databricks Runtime table, plus the optional live cross-check."""

from types import SimpleNamespace

from providers.execution.databricks import compatibility


def test_resolve_cluster_version_uses_curated_table():
    assert compatibility.resolve_cluster_version("4.2") == "19.x-scala2.13"
    assert compatibility.resolve_cluster_version("4.1") == "18.2.x-scala2.13"
    assert compatibility.resolve_cluster_version("4.0") == "17.3.x-scala2.13"


def test_resolve_cluster_version_returns_none_without_override():
    assert compatibility.resolve_cluster_version("4.3") is None


def test_resolve_cluster_version_uses_override_when_not_in_curated_table():
    assert compatibility.resolve_cluster_version("4.3", override="20.x-scala2.13") == "20.x-scala2.13"


def test_resolve_cluster_version_curated_table_wins_over_override():
    """An override for a Spark version already in the curated table must
    never shadow the known-good mapping."""
    assert compatibility.resolve_cluster_version("4.2", override="99.x-bogus") == "19.x-scala2.13"


def _fake_client(*version_pairs: tuple[str, str]) -> SimpleNamespace:
    versions = [SimpleNamespace(key=key, name=name) for key, name in version_pairs]
    return SimpleNamespace(clusters=SimpleNamespace(spark_versions=lambda: SimpleNamespace(versions=versions)))


def test_cross_check_returns_none_when_workspace_confirms_the_mapping():
    client = _fake_client(("19.x-scala2.13", "19 (includes Apache Spark 4.2.0, Scala 2.13)"))
    warning = compatibility.cross_check_against_workspace(client, "4.2", "19.x-scala2.13")
    assert warning is None


def test_cross_check_warns_when_cluster_version_key_missing_from_workspace():
    client = _fake_client(("18.2.x-scala2.13", "18.2 (includes Apache Spark 4.1.0, Scala 2.13)"))
    warning = compatibility.cross_check_against_workspace(client, "4.2", "19.x-scala2.13")
    assert warning is not None
    assert "19.x-scala2.13" in warning


def test_cross_check_warns_when_workspace_name_does_not_mention_expected_spark_version():
    client = _fake_client(("19.x-scala2.13", "19 (includes some other engine)"))
    warning = compatibility.cross_check_against_workspace(client, "4.2", "19.x-scala2.13")
    assert warning is not None
    assert "4.2" in warning


def test_cross_check_never_raises_when_workspace_is_unreachable():
    client = SimpleNamespace(clusters=SimpleNamespace(spark_versions=lambda: (_ for _ in ()).throw(RuntimeError)))
    warning = compatibility.cross_check_against_workspace(client, "4.2", "19.x-scala2.13")
    assert warning is None
