"""Reads a local file - csv, json, or parquet. Stands in for a real system
connector so the example project and its demo run don't need one; also
genuinely useful for testing a pipeline against sample data."""

from __future__ import annotations

from pyspark.sql import DataFrame

from pipetree.platform.base import resolve_value
from pipetree.sources.base import SourceContext

FILE_TYPES = ("csv", "json", "parquet")


class LocalFileSource:
    def read(self, ctx: SourceContext) -> DataFrame:
        raw_path = getattr(ctx.system, "path", None)
        if not raw_path:
            raise ValueError(
                f"{ctx.table.fqn}: system type {ctx.system.type!r} requires a 'path' property"
            )

        relative_path = resolve_value(raw_path, ctx.platform)
        resolved_path = ctx.platform.resolve_path(relative_path, ctx.base_dir)

        reader = ctx.spark.read
        if ctx.system.type == "csv":
            reader = reader.option("header", "true").option("inferSchema", "true")
        return reader.format(ctx.system.type).load(resolved_path)
