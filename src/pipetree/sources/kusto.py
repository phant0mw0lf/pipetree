"""Read for Azure Data Explorer / Fabric Eventhouse via the Kusto Spark
connector (azure-kusto-spark). The connector jar is a session-building
concern, not a read-time one: whoever builds the SparkSession needs
`MAVEN_COORDINATE` on its classpath (a `--packages` dependency or a
cluster library, the same idea as `configure_spark_with_delta_pip` for
Delta) before this reader can work.

**Option names - checked against the connector source at tag
`v4.0_7.1.4`** (`common/KustoOptions.scala`,
`datasource/KustoSourceOptions.scala`, `datasink/KustoSinkOptions.scala`,
`datasource/DefaultSource.scala`) and its `docs/KustoSource.md` /
`docs/Authentication.md`:

- `kustoCluster`, `kustoDatabase` - mandatory; a full
  `https://<cluster>.<region>.kusto.windows.net` URL is accepted as well
  as an alias.
- `kustoQuery` drives a read; "a flexible Kusto query (can simply be a
  table name)". The table's `source.object` is sent as that query, so a
  read is a whole-table read. `kustoTable` is a *sink* (write) option -
  the read path ignores it and would run an empty query.
- `auth.mode: aad_token` -> `accessToken` (`KUSTO_ACCESS_TOKEN`).
- `auth.mode: app_secret` -> `kustoAadAppId`, `kustoAadAppSecret`,
  `kustoAadAuthorityID` (the tenant id).

**`MAVEN_COORDINATE`** is the Spark 4.0 / Scala 2.13 build, matching
pipetree's pyspark 4 requirement; 7.1.4 is the latest
`kusto-spark_4.0_2.13` release on Maven Central, and its POM declares
`spark-sql_2.13` 4.0.0 and Scala 2.13 as provided. The older
`kusto-spark_3.0_2.12` artifacts can't load on a Scala 2.13 runtime.

**Token lifetime:** with `aad_token`, one token is minted on the driver
(`Platform.acquire_token`) and handed to the connector as a static string;
the connector's docs require it to stay "valid throughout the duration of
the read/write operation", and nothing refreshes it on executors.
Microsoft Entra's default access-token lifetime is a random 60-90 minutes
(the exact lifetime depends on the identity type), so a very long read can
outlive its token. (The connector's `tokenProviderCallbackClasspath`
option, a JVM callback called per request, is the refreshable
alternative; it isn't wired up here.)

Reads need the connector on the classpath, a token audience the cluster
accepts and a Viewer grant for the principal. For large reads: when the connector estimates
a query exceeds Kusto's query limits it switches to distributed mode,
which exports to blob storage - storage from the Kusto ingest service
unless `transientStorage` is set, which the connector's docs advise
against in production. pipetree sets no read-mode or storage options.

`auth.resource` (the `aad_token` audience) resolves through
`resolve_value`, so it can be a literal (e.g.
`https://api.kusto.windows.net`) or a `{secret: name}` reference (e.g. a
per-environment cluster URI).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pipetree.platform.base import resolve_value
from pipetree.sources.base import SourceContext

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

MAVEN_COORDINATE = "com.microsoft.azure.kusto:kusto-spark_4.0_2.13:7.1.4"
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
            .option("kustoQuery", table_name)
        )

        if mode == "aad_token":
            resource = resolve_value(
                _required(auth.get("resource"), "auth.resource", fqn), ctx.platform
            )
            token = ctx.platform.acquire_token(resource)
            reader = reader.option("accessToken", token)
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
