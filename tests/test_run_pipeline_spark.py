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


TAGGED_CONFIG = """
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
      tags: [daily]

silver:
  tables:
    customer_names:
      logic: customer_names.sql
      business_key: [id]
      strategy: replace
      depends_on: [customer]
      tags: [reporting]
"""


def test_a_tag_run_notes_the_unrefreshed_input_and_records_run_tags(tmp_path: Path, spark):
    from pipetree import run_pipeline
    from pipetree.adapters.spark.runlog_writer import DeltaRunLogWriter
    from pipetree.runlog.notifier import format_digest

    (tmp_path / "customer.csv").write_text("id,name\n1,Alice\n")
    (tmp_path / "customer_names.sql").write_text("SELECT id, name FROM bronze.customer")
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(TAGGED_CONFIG)
    log = DeltaRunLogWriter(spark, table_fqn="tagrun_meta.run_log")

    run_pipeline(config_path, execution_id=1, run_log_writer=log)
    digest = run_pipeline(config_path, execution_id=2, tags=["reporting"], run_log_writer=log)

    assert digest.succeeded
    assert digest.results["silver.customer_names"].notes == ["input not refreshed: bronze.customer"]
    assert "notes: input not refreshed: bronze.customer" in format_digest(digest)
    rows = spark.table("tagrun_meta.run_log").collect()
    run_tags = {(r["_execution_id"], r["table_fqn"]): r["run_tags"] for r in rows}
    assert run_tags[(1, "bronze.customer")] == []
    assert run_tags[(2, "silver.customer_names")] == ["reporting"]
    assert run_tags[(2, "bronze.customer")] == ["reporting"]
