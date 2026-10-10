"""Files landing continuously in a storage account, read as bounded
batches via Structured Streaming with `Trigger.AvailableNow` - so a
streaming source stays a bounded unit of work the ready-queue scheduler
can run once and finish, instead of running forever like an always-on
stream would.

Auto Loader (`cloudFiles`) on Databricks, since it isn't available
elsewhere; plain file-format streaming with schema inference enabled
locally and on Fabric. This is the one source type with durable state
between runs (a checkpoint tracking which files are already processed) -
`ctx.init` clears it, reprocessing everything as if for the first time.

The streaming mechanics are fully encapsulated here: the rest of pipetree
never sees a streaming DataFrame. New files are streamed into a staging
Delta location (cleared before each run, so it only ever holds *this*
run's newly-arrived batch - the checkpoint, untouched between runs, is
what remembers what's already been seen), then read back as an ordinary
batch DataFrame.

On Databricks the stream is read with Auto Loader (`cloudFiles`); elsewhere
with plain file-format streaming.
"""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING, Any

from delta.tables import DeltaTable

from pipetree.platform.base import resolve_value
from pipetree.sources.base import SourceContext

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class StorageStreamSource:
    def read(self, ctx: SourceContext) -> DataFrame:
        system = ctx.system
        fqn = ctx.table.fqn

        raw_path = _required(getattr(system, "path", None), "path", fqn)
        source_path = ctx.platform.resolve_path(resolve_value(raw_path, ctx.platform), ctx.base_dir)
        file_format = _required(getattr(system, "format", None), "format", fqn)

        checkpoint_location = ctx.platform.resolve_path(
            f"_checkpoints/{fqn}/checkpoint", ctx.base_dir
        )
        staging_location = ctx.platform.resolve_path(f"_checkpoints/{fqn}/staging", ctx.base_dir)
        schema_location = ctx.platform.resolve_path(f"_checkpoints/{fqn}/schema", ctx.base_dir)

        if ctx.init:
            for path in (checkpoint_location, staging_location, schema_location):
                shutil.rmtree(path, ignore_errors=True)
        else:
            # Cleared every run regardless: staging must hold only this
            # run's newly-arrived batch, never an accumulation of every
            # batch ever processed. The checkpoint (left alone here) is
            # what actually remembers cross-run progress.
            shutil.rmtree(staging_location, ignore_errors=True)

        reader = ctx.spark.readStream
        if ctx.platform.name == "databricks":
            reader = (
                reader.format("cloudFiles")
                .option("cloudFiles.format", file_format)
                .option("cloudFiles.schemaLocation", schema_location)
            )
        else:
            ctx.spark.conf.set("spark.sql.streaming.schemaInference", "true")
            reader = reader.format(file_format)
            if file_format == "csv":
                reader = reader.option("header", "true")

        stream_df = reader.load(source_path)

        query = (
            stream_df.writeStream.format("delta")
            .option("checkpointLocation", checkpoint_location)
            .trigger(availableNow=True)
            .start(staging_location)
        )
        query.awaitTermination()

        # Trigger.AvailableNow with nothing new to process never
        # initializes the sink at all - no staging table means no rows
        # arrived, not an error.
        if not DeltaTable.isDeltaTable(ctx.spark, staging_location):
            return ctx.spark.createDataFrame([], schema=stream_df.schema)

        return ctx.spark.read.format("delta").load(staging_location)


def _required(value: Any, field_name: str, table_fqn: str) -> Any:
    if value is None:
        raise ValueError(f"{table_fqn}: storage_stream system requires {field_name!r}")
    return value
