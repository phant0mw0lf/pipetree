import pytest

from pipetree.adapters.spark.adapter import SparkAdapter
from pipetree.model import System, Table

pytestmark = pytest.mark.spark


def make_table(**overrides) -> Table:
    defaults = {
        "name": "customer",
        "layer": "adaptertest",
        "table_schema": "adaptertest",
        "fqn": "adaptertest.customer",
        "strategy": "scd1",
        "business_key": ["id"],
    }
    defaults.update(overrides)
    return Table.model_validate(defaults)


def test_capabilities_declares_atomic_append_retry_support(spark, tmp_path):
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)
    assert adapter.capabilities.supports_delete_by_execution_id is True


def test_run_table_reads_a_source_table_via_read_source_and_merges(spark, tmp_path):
    system = System.model_validate({"type": "csv", "path": "customer.csv"})
    csv_path = tmp_path / "customer.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n")

    table = make_table(
        source={"system": "crm", "object": "customer"}, strategy="scd1", fqn="bronze_a.customer"
    )
    adapter = SparkAdapter(spark, systems={"crm": system}, base_dir=tmp_path)

    result = adapter.run_table(table, execution_id=1)

    assert result is not None
    written = spark.table(table.fqn)
    assert result["rows_written"] == 2
    assert {r["name"] for r in written.collect()} == {"Alice", "Bob"}
    assert {r["_source_system"] for r in written.collect()} == {"crm"}


def test_run_table_raises_a_clear_error_for_an_unsupported_system_type(spark, tmp_path):
    system = System(type="sqlserver")
    table = make_table(
        source={"system": "hr", "object": "employee"}, strategy="scd1", fqn="bronze_b.employee"
    )
    adapter = SparkAdapter(spark, systems={"hr": system}, base_dir=tmp_path)

    with pytest.raises(NotImplementedError, match="sqlserver"):
        adapter.run_table(table, execution_id=1)


def test_run_table_executes_a_sql_logic_file(spark, tmp_path):
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze_c")
    spark.createDataFrame([(1, "Alice")], ["id", "name"]).write.format("delta").mode(
        "overwrite"
    ).saveAsTable("bronze_c.customer")

    logic_path = tmp_path / "silver.sql"
    logic_path.write_text("SELECT id, name FROM bronze_c.customer")
    table = make_table(
        logic="silver.sql", strategy="replace", fqn="silver_c.customer_enriched", source=None
    )
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    result = adapter.run_table(table, execution_id=1)

    assert result is not None
    assert result["rows_written"] == 1
    assert spark.table(table.fqn).collect()[0]["name"] == "Alice"


def test_run_table_executes_a_pyspark_logic_file_assigning_result(spark, tmp_path):
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze_d")
    spark.createDataFrame([(1, "Bob")], ["id", "name"]).write.format("delta").mode(
        "overwrite"
    ).saveAsTable("bronze_d.customer")

    logic_path = tmp_path / "silver.py"
    logic_path.write_text(
        "customer = spark.table('bronze_d.customer')\n"
        "result = customer.selectExpr('id', 'upper(name) AS name')\n"
    )
    table = make_table(
        logic="silver.py", strategy="replace", fqn="silver_d.customer_enriched", source=None
    )
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    adapter.run_table(table, execution_id=1)

    assert spark.table(table.fqn).collect()[0]["name"] == "BOB"


def test_pyspark_logic_file_without_a_result_variable_raises_a_clear_error(spark, tmp_path):
    logic_path = tmp_path / "bad.py"
    logic_path.write_text("x = 1\n")
    table = make_table(logic="bad.py", strategy="replace", fqn="silver_e.x", source=None)
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    with pytest.raises(ValueError, match="result"):
        adapter.run_table(table, execution_id=1)


def test_run_table_seeds_unknown_member_when_requested(spark, tmp_path):
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold_a")
    logic_path = tmp_path / "dim.sql"
    logic_path.write_text("SELECT 1 AS id, 'Alice' AS name")
    table = make_table(
        logic="dim.sql",
        strategy="scd2",
        fqn="gold_a.dim_customer",
        source=None,
        unknown_member=True,
    )
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    adapter.run_table(table, execution_id=1)

    ids = {r["id"] for r in spark.table(table.fqn).collect()}
    assert ids == {1, -1}


def test_delete_by_execution_id_removes_only_rows_from_that_run(spark, tmp_path):
    table = make_table(strategy="append", fqn="applog.events")
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    from pipetree.adapters.spark.merge import merge_append

    merge_append(spark, table, spark.createDataFrame([(1, "a")], ["id", "name"]), 1, "sys")
    merge_append(spark, table, spark.createDataFrame([(2, "b")], ["id", "name"]), 2, "sys")

    adapter.delete_by_execution_id(table, execution_id=1)

    remaining = {r["id"] for r in spark.table(table.fqn).collect()}
    assert remaining == {2}


def test_delete_by_execution_id_is_a_noop_when_the_table_does_not_exist(spark, tmp_path):
    table = make_table(strategy="append", fqn="applog_missing.events")
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    adapter.delete_by_execution_id(table, execution_id=1)  # must not raise
