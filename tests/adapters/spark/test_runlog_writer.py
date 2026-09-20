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
