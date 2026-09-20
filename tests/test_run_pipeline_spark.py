from pathlib import Path

import pytest

pytestmark = pytest.mark.spark

CONFIG = """
systems:
  seed:
    type: csv
    path: customer.csv

bronze:
  tables:
    customer:
      source:
        system: seed
        object: customer
      business_key: [id]
      strategy: scd1
"""


def test_run_pipeline_default_path_reuses_an_already_active_spark_session(
    tmp_path: Path, spark, monkeypatch
):
    # The `spark` fixture's own build_local_session() call made itself the
    # active session. run_pipeline() must reuse it via getActiveSession()
    # rather than building a second local[*] session - forcing local[*]
    # would be wrong on a real Databricks/Fabric cluster, which already
    # has a distributed session. build_local_session() raising proves the
    # active-session path was actually taken, not just that .getOrCreate()
    # happened to hand back the same session either way.
    def _must_not_be_called(*a, **k):
        raise AssertionError("build_local_session() should not run when a session is active")

    monkeypatch.setattr("pipetree.adapters.spark.session.build_local_session", _must_not_be_called)

    (tmp_path / "customer.csv").write_text("id,name\n1,Alice\n")
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(CONFIG)

    from pipetree import run_pipeline

    digest = run_pipeline(config_path, execution_id=1)

    assert digest.succeeded
    written = spark.table("bronze.customer").collect()
    assert written[0]["name"] == "Alice"


def test_run_pipeline_falls_back_to_a_local_session_when_none_is_active(
    tmp_path: Path, spark, monkeypatch
):
    # No active session to reuse: falls back to build_local_session().
    from pyspark.sql import SparkSession

    monkeypatch.setattr(SparkSession, "getActiveSession", classmethod(lambda cls: None))
    monkeypatch.setattr(
        "pipetree.adapters.spark.session.build_local_session", lambda *a, **k: spark
    )

    (tmp_path / "customer.csv").write_text("id,name\n1,Alice\n")
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(CONFIG)

    from pipetree import run_pipeline

    digest = run_pipeline(config_path, execution_id=1)

    assert digest.succeeded
