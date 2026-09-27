"""No JVM needed, same reasoning as test_sqlserver.py - and there's no
real Kusto cluster to connect to here regardless.

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
from pipetree.sources.kusto import KustoSource


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


def make_table() -> Table:
    return Table.model_validate(
        {
            "name": "events",
            "layer": "bronze",
            "table_schema": "bronze",
            "fqn": "bronze.events",
            "strategy": "append",
            "source": {"system": "adx", "object": "EventsTable"},
        }
    )


def make_system() -> System:
    return System.model_validate(
        {
            "type": "kusto",
            "cluster": "https://mycluster.westeurope.kusto.windows.net",
            "database": "mydb",
            "auth": {"appId": "app-id", "appSecret": "app-secret", "authority": "tenant-id"},
        }
    )


def make_ctx(spark: Any, table: Table, system: System) -> SourceContext:
    return SourceContext(spark, table, system, LocalPlatform(), Path("."))


def read(ctx: SourceContext) -> Any:
    return KustoSource().read(ctx)


def test_builds_the_kusto_format_and_options():
    ctx = make_ctx(FakeSpark(), make_table(), make_system())

    _, fmt, options = read(ctx)

    assert fmt == "com.microsoft.kusto.spark.datasource"
    assert options["kustoCluster"] == "https://mycluster.westeurope.kusto.windows.net"
    assert options["kustoDatabase"] == "mydb"
    assert options["kustoTable"] == "EventsTable"
    assert options["kustoAadAppId"] == "app-id"
    assert options["kustoAadAppSecret"] == "app-secret"
    assert options["kustoAadAuthorityID"] == "tenant-id"


def test_resolves_secret_connection_properties(monkeypatch):
    monkeypatch.setenv("ADX_APP_SECRET", "resolved-secret")
    system = System.model_validate(
        {
            "type": "kusto",
            "cluster": "https://mycluster.westeurope.kusto.windows.net",
            "database": "mydb",
            "auth": {
                "appId": "app-id",
                "appSecret": {"secret": "adx-app-secret"},
                "authority": "tenant-id",
            },
        }
    )
    ctx = make_ctx(FakeSpark(), make_table(), system)

    _, _, options = read(ctx)

    assert options["kustoAadAppSecret"] == "resolved-secret"


def test_raises_a_clear_error_when_cluster_is_missing():
    system = System.model_validate({"type": "kusto", "database": "mydb", "auth": {}})
    ctx = make_ctx(FakeSpark(), make_table(), system)

    with pytest.raises(ValueError, match="cluster"):
        read(ctx)


def test_raises_a_clear_error_when_auth_is_missing():
    system = System.model_validate(
        {
            "type": "kusto",
            "cluster": "https://mycluster.westeurope.kusto.windows.net",
            "database": "mydb",
        }
    )
    ctx = make_ctx(FakeSpark(), make_table(), system)

    with pytest.raises(ValueError, match="auth"):
        read(ctx)
