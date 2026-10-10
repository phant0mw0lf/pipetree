"""Databricks entrypoint for the pipetree example pipeline.

Run as a `spark_python_task` via the Databricks Asset Bundle in this
directory (`databricks bundle deploy && databricks bundle run
pipetree_example`) - see README.md for the exact commands and the Key Vault
setup.

Parameters (positional, matching databricks.yml): catalog, key_vault_url,
key_vault_credential. The last two are optional: leave them empty for a
pipeline without `{secret: ...}` values.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession

from pipetree import run_pipeline
from pipetree.adapters.spark.adapter import SparkAdapter
from pipetree.config.loader import load_config
from pipetree.model import PipelineConfig
from pipetree.platform.databricks import DatabricksPlatform

CATALOG = sys.argv[1] if len(sys.argv) > 1 else "main"
KEY_VAULT_URL = (sys.argv[2] if len(sys.argv) > 2 else "") or None
KEY_VAULT_CREDENTIAL = (sys.argv[3] if len(sys.argv) > 3 else "") or None
CONFIG_PATH = (
    Path(sys.argv[4])
    if len(sys.argv) > 4
    else Path(__file__).resolve().parent.parent / "pipeline.yaml"
)


def main() -> int:
    # Injected into globals by the Databricks runtime for any code
    # actually running on a cluster; not importable, so this fails with a
    # clear NameError anywhere else instead of a confusing one later.
    dbutils = globals()["dbutils"]

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "no active SparkSession - this script expects to run on a Databricks cluster"
        )

    # Loaded here (and again inside run_pipeline()) only to get
    # config.systems for the adapter - the same pattern examples/run_demo.py
    # uses locally.
    raw = load_config(CONFIG_PATH)
    config = PipelineConfig.from_validated_raw(raw)

    platform = DatabricksPlatform(
        dbutils=dbutils,
        catalog=CATALOG,
        key_vault_url=KEY_VAULT_URL,
        key_vault_credential=KEY_VAULT_CREDENTIAL,
    )
    adapter = SparkAdapter(
        spark, systems=config.systems, base_dir=CONFIG_PATH.parent, platform=platform
    )

    digest = run_pipeline(CONFIG_PATH, adapter=adapter)
    return digest.exit_code


if __name__ == "__main__":
    sys.exit(main())
