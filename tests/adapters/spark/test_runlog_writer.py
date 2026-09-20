import pytest

from pipetree.adapters.spark.runlog_writer import DeltaRunLogWriter
from pipetree.runlog.writer import RunLogWriter

pytestmark = pytest.mark.spark


def test_satisfies_the_run_log_writer_protocol(spark):
    assert isinstance(DeltaRunLogWriter(spark, table_fqn="rl_a.log"), RunLogWriter)


def test_creates_the_table_on_first_write(spark):
    writer = DeltaRunLogWriter(spark, table_fqn="rl_b.log")
    rows = [{"_execution_id": 1, "table_fqn": "bronze.orders", "status": "succeeded"}]

    writer.write(rows, execution_id=1)

    written = spark.table("rl_b.log").collect()
    assert len(written) == 1
    assert written[0]["table_fqn"] == "bronze.orders"


def test_replaces_only_the_given_execution_id_on_rewrite(spark):
    writer = DeltaRunLogWriter(spark, table_fqn="rl_c.log")
    writer.write(
        [{"_execution_id": 1, "table_fqn": "bronze.a", "status": "succeeded"}], execution_id=1
    )
    writer.write(
        [{"_execution_id": 2, "table_fqn": "bronze.a", "status": "succeeded"}], execution_id=2
    )

    # re-running execution_id 1 (e.g. after a crash) replaces its rows, not append
    writer.write(
        [{"_execution_id": 1, "table_fqn": "bronze.a", "status": "failed"}], execution_id=1
    )

    rows = spark.table("rl_c.log").collect()
    by_execution_id = {r["_execution_id"]: r["status"] for r in rows}
    assert by_execution_id == {1: "failed", 2: "succeeded"}


def test_does_not_write_when_there_are_no_rows(spark):
    writer = DeltaRunLogWriter(spark, table_fqn="rl_d.log")

    writer.write([], execution_id=1)

    assert not spark.catalog.tableExists("rl_d.log")


def test_writes_rows_whose_optional_columns_are_all_none(spark):
    # A real run_log_rows() batch: successful tables have no error_type,
    # a fresh table has no rows_written yet, etc. Spark can't infer a type
    # for a column that's None in every row of a plain dict-list unless
    # the writer supplies an explicit schema.
    writer = DeltaRunLogWriter(spark, table_fqn="rl_e.log")
    rows = [
        {
            "_execution_id": 1,
            "table_fqn": "bronze.orders",
            "layer": "bronze",
            "strategy": "scd1",
            "status": "succeeded",
            "attempts": 1,
            "started_at": 1,
            "ended_at": 2,
            "duration_ms": 100,
            "rows_written": None,
            "duplicates_dropped": None,
            "schema_changes": None,
            "error_type": None,
            "error_message": None,
        }
    ]

    writer.write(rows, execution_id=1)

    written = spark.table("rl_e.log").collect()
    assert written[0]["table_fqn"] == "bronze.orders"
    assert written[0]["error_type"] is None
