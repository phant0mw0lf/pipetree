"""No JVM needed: SqlServerSource only ever calls `ctx.spark.read...`
through duck typing, so a hand-written fake DataFrameReader is enough to
verify the JDBC url/options it builds - there's no real SQL Server to
connect to here anyway.

FakeSpark/the (fmt, options) tuple `.read()` returns are both typed `Any`
throughout: the real SourceReader protocol declares a `SparkSession` in
and a `DataFrame` out, but this fake stands in for both to keep the test
fast and JVM-free.
"""

from pathlib import Path
from typing import Any

import pytest

from pipetree.model import System, Table
from pipetree.platform.local import LocalPlatform
from pipetree.sources.base import SourceContext
from pipetree.sources.sqlserver import SqlServerSource


class FakeReader:
    def __init__(self):
        self.format_name = None
        self.options: dict[str, str] = {}

    def format(self, name):
        self.format_name = name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self):
        return ("LOADED", self.format_name, dict(self.options))


class FakeSpark:
    def __init__(self):
        self.read = FakeReader()


def make_table(**overrides) -> Table:
    defaults = {
        "name": "employee",
        "layer": "bronze",
        "table_schema": "bronze",
        "fqn": "bronze.employee",
        "strategy": "scd1",
        "source": {"system": "hr", "object": "dbo.Employee"},
    }
    defaults.update(overrides)
    return Table.model_validate(defaults)


def make_ctx(system: System) -> SourceContext:
    spark: Any = FakeSpark()
    return SourceContext(spark, make_table(), system, LocalPlatform(), Path("."))


def read(ctx: SourceContext) -> Any:
    return SqlServerSource().read(ctx)


def test_builds_the_jdbc_url_and_dbtable():
    system = System.model_validate(
        {
            "type": "sqlserver",
            "host": "sql.example.com",
            "database": "hr",
            "auth": {"type": "sql", "user": "svc", "password": "pw"},
        }
    )
    ctx = make_ctx(system)

    _, fmt, options = read(ctx)

    assert fmt == "jdbc"
    assert options["url"] == (
        "jdbc:sqlserver://sql.example.com;databaseName=hr;encrypt=true;trustServerCertificate=false"
    )
    assert options["dbtable"] == "dbo.Employee"
    assert options["user"] == "svc"
    assert options["password"] == "pw"


def test_resolves_secret_connection_properties(monkeypatch):
    monkeypatch.setenv("HR_HOST", "sql.internal")
    monkeypatch.setenv("HR_USER", "svc")
    monkeypatch.setenv("HR_PW", "s3cret")
    system = System.model_validate(
        {
            "type": "sqlserver",
            "host": {"secret": "hr-host"},
            "database": "hr",
            "auth": {
                "type": "sql",
                "user": {"secret": "hr-user"},
                "password": {"secret": "hr-pw"},
            },
        }
    )
    ctx = make_ctx(system)

    _, _, options = read(ctx)

    assert "sql.internal" in options["url"]
    assert options["user"] == "svc"
    assert options["password"] == "s3cret"


def test_passes_through_partitioning_options_when_given():
    system = System.model_validate(
        {
            "type": "sqlserver",
            "host": "sql.example.com",
            "database": "hr",
            "auth": {"type": "sql", "user": "svc", "password": "pw"},
            "partitionColumn": "employee_id",
            "numPartitions": 4,
            "lowerBound": 1,
            "upperBound": 10000,
        }
    )
    ctx = make_ctx(system)

    _, _, options = read(ctx)

    assert options["partitionColumn"] == "employee_id"
    assert options["numPartitions"] == "4"
    assert options["lowerBound"] == "1"
    assert options["upperBound"] == "10000"


def test_omits_partitioning_options_when_not_given():
    system = System.model_validate(
        {
            "type": "sqlserver",
            "host": "sql.example.com",
            "database": "hr",
            "auth": {"type": "sql", "user": "svc", "password": "pw"},
        }
    )
    ctx = make_ctx(system)

    _, _, options = read(ctx)

    assert "partitionColumn" not in options


def test_raises_a_clear_error_when_host_is_missing():
    system = System.model_validate(
        {"type": "sqlserver", "database": "hr", "auth": {"user": "svc", "password": "pw"}}
    )
    ctx = make_ctx(system)

    with pytest.raises(ValueError, match="host"):
        read(ctx)


def test_raises_a_clear_error_when_auth_is_missing():
    system = System.model_validate(
        {"type": "sqlserver", "host": "sql.example.com", "database": "hr"}
    )
    ctx = make_ctx(system)

    with pytest.raises(ValueError, match="auth"):
        read(ctx)
