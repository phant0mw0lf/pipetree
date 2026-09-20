"""The real run log writer: `_meta.pipeline_run_log` as a Delta table,
written once per run with a `replaceWhere` on `_execution_id` - so a
re-run of the same execution id (e.g. after a crash) replaces its rows
instead of duplicating them, and every other run's rows are untouched."""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession


class DeltaRunLogWriter:
    def __init__(self, spark: SparkSession, table_fqn: str = "_meta.pipeline_run_log") -> None:
        self._spark = spark
        self._table_fqn = table_fqn

    def write(self, rows: list[dict[str, Any]], *, execution_id: int) -> None:
        if not rows:
            return

        schema_name = self._table_fqn.split(".")[0]
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        df = self._spark.createDataFrame(rows)

        writer = df.write.format("delta").mode("overwrite")
        if self._spark.catalog.tableExists(self._table_fqn):
            writer = writer.option("replaceWhere", f"_execution_id = {execution_id}")
        writer.saveAsTable(self._table_fqn)
