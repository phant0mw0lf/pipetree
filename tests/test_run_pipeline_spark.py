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


def test_run_pipeline_default_path_builds_a_real_spark_session_and_delta_table(
    tmp_path: Path, spark, monkeypatch
):
    # run_pipeline() with no adapter builds its own SparkSession via
    # build_local_session() - point that at the shared test session instead
    # of starting a second one.
    monkeypatch.setattr(
        "pipetree.adapters.spark.session.build_local_session", lambda *a, **k: spark
    )

    (tmp_path / "customer.csv").write_text("id,name\n1,Alice\n")
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(CONFIG)

    from pipetree import run_pipeline

    digest = run_pipeline(config_path, execution_id=1)

    assert digest.succeeded
    written = spark.table("bronze.customer").collect()
    assert written[0]["name"] == "Alice"
