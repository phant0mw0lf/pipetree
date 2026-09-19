"""A local[*] SparkSession configured for Delta - the one real engine, per
the decision that the local dev path and Fabric/Databricks share the same
code, not a separate lightweight substitute."""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def build_local_session(
    app_name: str = "pipetree", warehouse_dir: str | None = None
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
    )
    if warehouse_dir is not None:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)

    return configure_spark_with_delta_pip(builder).getOrCreate()
