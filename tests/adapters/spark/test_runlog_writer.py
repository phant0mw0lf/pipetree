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
            "null_keys_dropped": None,
        }
    ]

    writer.write(rows, execution_id=1)

    written = spark.table("rl_e.log").collect()
    assert written[0]["table_fqn"] == "bronze.orders"
    assert written[0]["error_type"] is None


def test_a_run_log_table_without_null_keys_dropped_is_evolved_in_place(spark):
    # Run logs created before the column existed must keep working: the
    # write adds the column, old rows read back NULL.
    fresh_fqn, fqn = "rl_fresh.log", "rl_evolve.log"
    spark.sql("CREATE SCHEMA IF NOT EXISTS rl_evolve")
    spark.sql(
        f"CREATE TABLE {fqn} (_execution_id BIGINT NOT NULL, table_fqn STRING NOT NULL, "
        "layer STRING, strategy STRING, status STRING, attempts BIGINT, started_at BIGINT, "
        "ended_at BIGINT, duration_ms BIGINT, rows_written BIGINT, duplicates_dropped BIGINT, "
        "schema_changes ARRAY<STRING>, error_type STRING, error_message STRING) USING delta"
    )
    spark.sql(
        f"INSERT INTO {fqn} (_execution_id, table_fqn, status, duplicates_dropped) "
        "VALUES (1, 'bronze.old', 'succeeded', 2)"
    )
    new_row = {
        "_execution_id": 2,
        "table_fqn": "bronze.new",
        "status": "succeeded",
        "null_keys_dropped": 7,
    }

    DeltaRunLogWriter(spark, table_fqn=fqn).write([new_row], execution_id=2)
    DeltaRunLogWriter(spark, table_fqn=fresh_fqn).write([new_row], execution_id=2)

    by_fqn = {r["table_fqn"]: r for r in spark.table(fqn).collect()}
    assert by_fqn["bronze.old"]["null_keys_dropped"] is None
    assert by_fqn["bronze.old"]["duplicates_dropped"] == 2
    assert by_fqn["bronze.new"]["null_keys_dropped"] == 7
    assert spark.table(fqn).columns == spark.table(fresh_fqn).columns
