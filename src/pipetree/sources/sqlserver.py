"""JDBC read for SQL Server. Connection properties resolve through the
platform, same as any other system value - a literal or `{secret: name}`.

Only ever calls `ctx.spark.read...` through duck typing, so this needs no
pyspark import at runtime - it works against any object with a
`DataFrameReader`-shaped `.read`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pipetree.platform.base import resolve_value
from pipetree.sources.base import SourceContext

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

_PARTITION_OPTIONS = ("partitionColumn", "numPartitions", "lowerBound", "upperBound")


class SqlServerSource:
    def read(self, ctx: SourceContext) -> DataFrame:
        system = ctx.system
        fqn = ctx.table.fqn

        host = resolve_value(_required(getattr(system, "host", None), "host", fqn), ctx.platform)
        database = resolve_value(
            _required(getattr(system, "database", None), "database", fqn), ctx.platform
        )

        if ctx.table.source is None:
            raise ValueError(f"{fqn}: a sqlserver table needs a 'source' block")
        object_name = ctx.table.source.object

        url = (
            f"jdbc:sqlserver://{host};databaseName={database};"
            "encrypt=true;trustServerCertificate=false"
        )
        reader = ctx.spark.read.format("jdbc").option("url", url).option("dbtable", object_name)

        auth = _required(getattr(system, "auth", None), "auth", fqn)
        user = resolve_value(_required(auth.get("user"), "auth.user", fqn), ctx.platform)
        password = resolve_value(
            _required(auth.get("password"), "auth.password", fqn), ctx.platform
        )
        reader = reader.option("user", user).option("password", password)

        for option_name in _PARTITION_OPTIONS:
            value = getattr(system, option_name, None)
            if value is not None:
                reader = reader.option(option_name, str(value))

        return reader.load()


def _required(value: Any, field_name: str, table_fqn: str) -> Any:
    if value is None:
        raise ValueError(f"{table_fqn}: sqlserver system requires {field_name!r}")
    return value
