from pathlib import Path

import pytest
from click.testing import CliRunner

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


def test_run_command_executes_a_real_local_pipeline_and_exits_zero(
    tmp_path: Path, spark, monkeypatch
):
    monkeypatch.setattr(
        "pipetree.adapters.spark.session.build_local_session", lambda *a, **k: spark
    )
    (tmp_path / "customer.csv").write_text("id,name\n1,Alice\n")
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(CONFIG)

    from pipetree.cli import main

    result = CliRunner().invoke(main, ["run", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "succeeded" in result.output
    assert spark.table("bronze.customer").collect()[0]["name"] == "Alice"
