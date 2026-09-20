"""The real run log writer: `_meta.pipeline_run_log` as a Delta table,
written once per run with a `replaceWhere` on `_execution_id` - so a
re-run of the same execution id (e.g. after a crash) replaces its rows
instead of duplicating them, and every other run's rows are untouched."""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

# Matches pipetree.runlog.collector.build_run_log_rows() exactly. Needed
# explicitly because a column that's None in every row of a batch (a
# fully successful run has no error_type anywhere, for instance) can't
# have its type inferred from the row data alone.
_SCHEMA = StructType(
    [
        StructField("_execution_id", LongType(), nullable=False),
        StructField("table_fqn", StringType(), nullable=False),
        StructField("layer", StringType(), nullable=True),
        StructField("strategy", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("attempts", LongType(), nullable=True),
        StructField("started_at", LongType(), nullable=True),
        StructField("ended_at", LongType(), nullable=True),
        StructField("duration_ms", LongType(), nullable=True),
        StructField("rows_written", LongType(), nullable=True),
        StructField("duplicates_dropped", LongType(), nullable=True),
        StructField("schema_changes", ArrayType(StringType()), nullable=True),
        StructField("error_type", StringType(), nullable=True),
        StructField("error_message", StringType(), nullable=True),
    ]
)


class DeltaRunLogWriter:
    def __init__(self, spark: SparkSession, table_fqn: str = "_meta.pipeline_run_log") -> None:
        self._spark = spark
        self._table_fqn = table_fqn

    def write(self, rows: list[dict[str, Any]], *, execution_id: int) -> None:
        if not rows:
            return

        schema_name = self._table_fqn.split(".")[0]
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        df = self._spark.createDataFrame(rows, schema=_SCHEMA)

        writer = df.write.format("delta").mode("overwrite")
        if self._spark.catalog.tableExists(self._table_fqn):
            writer = writer.option("replaceWhere", f"_execution_id = {execution_id}")
        writer.saveAsTable(self._table_fqn)
