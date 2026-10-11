from pathlib import Path

import pytest

from pipetree.graph.errors import DependencyParseError
from pipetree.graph.parsers import (
    parse_logic_dependencies,
    parse_pyspark_dependencies,
    parse_sql_dependencies,
)


def test_parse_sql_dependencies_extracts_from_and_join():
    sql = """
    SELECT o.order_id, c.accountid
    FROM silver.orders o
    JOIN gold.dim_customer c ON o.customer_id = c.accountid
    """

    assert parse_sql_dependencies(sql) == {"silver.orders", "gold.dim_customer"}


def test_parse_sql_dependencies_is_case_insensitive_on_keywords():
    sql = "select * from silver.orders o join gold.dim_customer c on true"

    assert parse_sql_dependencies(sql) == {"silver.orders", "gold.dim_customer"}


def test_parse_sql_dependencies_finds_tables_in_subqueries_too():
    sql = "SELECT * FROM (SELECT * FROM bronze.orders) x"

    assert parse_sql_dependencies(sql) == {"bronze.orders"}


def test_parse_pyspark_dependencies_extracts_spark_table_literal():
    code = """
orders = spark.table("silver.orders")
customer = spark.table('gold.dim_customer')
"""

    assert parse_pyspark_dependencies(code) == {"silver.orders", "gold.dim_customer"}


def test_parse_pyspark_dependencies_extracts_from_spark_sql_literal():
    code = '''
result = spark.sql("""
    SELECT * FROM silver.orders
""")
'''

    assert parse_pyspark_dependencies(code) == {"silver.orders"}


def test_parse_pyspark_dependencies_resolves_names_from_module_constants():
    code = """
name = "silver.orders"
result = spark.sql(f"SELECT * FROM {name}")
"""

    assert parse_pyspark_dependencies(code) == {"silver.orders"}


def test_parse_pyspark_dependencies_refuses_names_built_at_run_time():
    code = """
def load(name):
    return spark.sql(f"SELECT * FROM {name}")
"""

    with pytest.raises(DependencyParseError, match="depends_on"):
        parse_pyspark_dependencies(code, file="orders.py")


def test_dependency_parse_error_names_file_expression_and_fix():
    with pytest.raises(DependencyParseError) as excinfo:
        parse_pyspark_dependencies("df = spark.table(get_name())", file="silver/orders.py")

    message = str(excinfo.value)
    assert "silver/orders.py" in message
    assert "spark.table(get_name())" in message
    assert "depends_on" in message


def test_parse_logic_dependencies_dispatches_sql_files(tmp_path: Path):
    path = tmp_path / "orders.sql"
    path.write_text("SELECT * FROM bronze.orders")

    assert parse_logic_dependencies(path) == {"bronze.orders"}


def test_parse_logic_dependencies_dispatches_python_files(tmp_path: Path):
    path = tmp_path / "orders.py"
    path.write_text('orders = spark.table("bronze.orders")')

    assert parse_logic_dependencies(path) == {"bronze.orders"}


def test_parse_logic_dependencies_rejects_unknown_extension(tmp_path: Path):
    path = tmp_path / "orders.txt"
    path.write_text("irrelevant")

    with pytest.raises(ValueError):
        parse_logic_dependencies(path)
