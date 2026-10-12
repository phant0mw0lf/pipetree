# Databricks notebook source
# MAGIC %md
# MAGIC # pipetree example - live run
# MAGIC
# MAGIC Runs `examples/pipeline.yaml` and shows the run live: the dependency tree
# MAGIC under the run cell, redrawn as tables finish, with the run digest under it
# MAGIC at the end. A run with a failed table fails the task. Setup, parameters and
# MAGIC the Key Vault option: see `README.md`.

# COMMAND ----------

# ruff: noqa: E402 - notebook cells: imports come after the widget cell

# Parameters: the job's notebook_task `base_parameters` (databricks.yml) fill
# these widgets. The vault parameters are optional: leave them empty for a
# pipeline without `{secret: ...}` values. `acs_credential` is the Unity Catalog
# service credential that sends the failure email (`notifications.email`).
dbutils.widgets.text("catalog", "main")  # noqa: F821 - notebook global
dbutils.widgets.text("key_vault_url", "")  # noqa: F821
dbutils.widgets.text("key_vault_credential", "")  # noqa: F821
dbutils.widgets.text("acs_credential", "")  # noqa: F821
dbutils.widgets.text("tags", "")  # noqa: F821
dbutils.widgets.text("config_path", "")  # noqa: F821

# COMMAND ----------

from pathlib import Path

from pipetree import run_pipeline
from pipetree.adapters.spark.adapter import SparkAdapter
from pipetree.config.loader import load_config
from pipetree.graph.select import split_names
from pipetree.model import PipelineConfig
from pipetree.notebook import LiveGraphView
from pipetree.notify.credential import ACS_RESOURCE
from pipetree.platform.databricks import DatabricksPlatform

catalog = dbutils.widgets.get("catalog")  # noqa: F821
key_vault_url = dbutils.widgets.get("key_vault_url") or None  # noqa: F821
key_vault_credential = dbutils.widgets.get("key_vault_credential") or None  # noqa: F821
acs_credential = dbutils.widgets.get("acs_credential") or None  # noqa: F821
tags = split_names(dbutils.widgets.get("tags"))  # noqa: F821 - comma separated, empty = every table
config_path = Path(dbutils.widgets.get("config_path"))  # noqa: F821

# The pipeline writes plain schema.table names: make them land in `catalog`
# and not in the workspace's default catalog.
spark.sql(f"USE CATALOG `{catalog}`")  # noqa: F821

# Loaded here (and again inside run_pipeline()) only to get config.systems for
# the adapter - the same pattern examples/run_demo.py uses locally.
config = PipelineConfig.from_validated_raw(load_config(config_path))

platform = DatabricksPlatform(
    dbutils=dbutils,  # noqa: F821
    catalog=catalog,
    key_vault_url=key_vault_url,
    key_vault_credential=key_vault_credential,
    # The audience of Azure Communication Services, for `notifications.email`.
    service_credentials={ACS_RESOURCE: acs_credential} if acs_credential else None,
)
adapter = SparkAdapter(
    spark,  # noqa: F821
    systems=config.systems,
    base_dir=config_path.parent,
    platform=platform,
)

digest = run_pipeline(
    config_path,
    adapter=adapter,
    observer=LiveGraphView(title="pipetree example"),
    tags=tags or None,
)

# COMMAND ----------

# A failed table fails the task (and with it the job run).
if digest.exit_code != 0:
    raise RuntimeError(f"pipetree run {digest.execution_id} FAILED (exit code {digest.exit_code})")
