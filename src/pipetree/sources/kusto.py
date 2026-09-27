"""Read for Azure Data Explorer / Fabric Eventhouse via the Kusto Spark
connector. The connector jar is a session-building concern, not a
read-time one: whoever builds the SparkSession needs `MAVEN_COORDINATE`
on its classpath (a `--packages` dependency, the same idea as
`configure_spark_with_delta_pip` for Delta) before this reader can work.

**`MAVEN_COORDINATE` is unverified** - pin it to whatever's current for
your Spark/Scala version when you actually wire this up; it's a
plausible-looking default, not a tested one.

**`kustoAccessToken` (the option `auth.mode: aad_token` sets) is a
plausible-looking option name for passing a pre-acquired AAD token
directly, same caveat as `MAVEN_COORDINATE` above - verify it against
the connector version actually pinned before relying on it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pipetree.platform.base import resolve_value
from pipetree.sources.base import SourceContext

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

MAVEN_COORDINATE = "com.microsoft.azure.kusto:kusto-spark_3.0_2.12:5.0.5"
_FORMAT = "com.microsoft.kusto.spark.datasource"


class KustoSource:
    def read(self, ctx: SourceContext) -> DataFrame:
        system = ctx.system
        fqn = ctx.table.fqn

        cluster = resolve_value(
            _required(getattr(system, "cluster", None), "cluster", fqn), ctx.platform
        )
        database = resolve_value(
            _required(getattr(system, "database", None), "database", fqn), ctx.platform
        )

        if ctx.table.source is None:
            raise ValueError(f"{fqn}: a kusto table needs a 'source' block")
        table_name = ctx.table.source.object

        auth = _required(getattr(system, "auth", None), "auth", fqn)
        mode = auth.get("mode", "app_secret")

        reader = (
            ctx.spark.read.format(_FORMAT)
            .option("kustoCluster", cluster)
            .option("kustoDatabase", database)
            .option("kustoTable", table_name)
        )

        if mode == "aad_token":
            resource = _required(auth.get("resource"), "auth.resource", fqn)
            token = ctx.platform.acquire_token(resource)
            reader = reader.option("kustoAccessToken", token)
        elif mode == "app_secret":
            app_id = resolve_value(_required(auth.get("appId"), "auth.appId", fqn), ctx.platform)
            app_secret = resolve_value(
                _required(auth.get("appSecret"), "auth.appSecret", fqn), ctx.platform
            )
            authority = resolve_value(
                _required(auth.get("authority"), "auth.authority", fqn), ctx.platform
            )
            reader = (
                reader.option("kustoAadAppId", app_id)
                .option("kustoAadAppSecret", app_secret)
                .option("kustoAadAuthorityID", authority)
            )
        else:
            raise ValueError(
                f"{fqn}: kusto auth.mode must be 'app_secret' or 'aad_token', got {mode!r}"
            )

        return reader.load()


def _required(value: Any, field_name: str, table_fqn: str) -> Any:
    if value is None:
        raise ValueError(f"{table_fqn}: kusto system requires {field_name!r}")
    return value
