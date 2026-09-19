"""SparkAdapter: the Adapter protocol, backed by Spark + Delta.

Reading a `source` table's raw input goes through a small, swappable
`read_source` callable rather than the full `SourceReader` registry
(sqlserver, kusto, storage_stream, custom) - that registry is Phase D. The
default here reads a local file (csv/json/parquet) so the example project
and its demo run don't need a real system to talk to; pass a different
`read_source` to plug in something else in the meantime.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from pipetree.adapters.base import Capabilities
from pipetree.adapters.spark.merge import (
    merge_append,
    merge_replace,
    merge_scd1,
    merge_scd2,
    seed_unknown_member,
)
from pipetree.model import System, Table

MergeFn = Callable[[SparkSession, Table, DataFrame, int, "str | None"], dict]
ReadSourceFn = Callable[[Table, System], DataFrame]

_MERGE_FUNCTIONS: dict[str, MergeFn] = {
    "scd1": merge_scd1,
    "scd2": merge_scd2,
    "replace": merge_replace,
    "append": merge_append,
}

_LOCAL_FILE_TYPES = {"csv", "json", "parquet"}


class SparkAdapter:
    capabilities = Capabilities(supports_delete_by_execution_id=True)

    def __init__(
        self,
        spark: SparkSession,
        systems: dict[str, System],
        base_dir: str | Path,
        read_source: ReadSourceFn | None = None,
    ) -> None:
        self._spark = spark
        self._systems = systems
        self._base_dir = Path(base_dir)
        self._read_source = read_source or self._read_local_file_source

    def run_table(self, table: Table, *, execution_id: int) -> dict[str, Any] | None:
        if table.source is not None:
            system = self._systems[table.source.system]
            source_df = self._read_source(table, system)
            source_system = table.source.system
        else:
            source_df = self._run_logic(table)
            source_system = None

        merge_fn = _MERGE_FUNCTIONS[table.strategy]
        result = merge_fn(self._spark, table, source_df, execution_id, source_system)

        if table.unknown_member:
            seed_unknown_member(self._spark, table, execution_id)

        return result

    def delete_by_execution_id(self, table: Table, execution_id: int) -> None:
        if not self._spark.catalog.tableExists(table.fqn):
            return
        DeltaTable.forName(self._spark, table.fqn).delete(f"_execution_id = {execution_id}")

    def _run_logic(self, table: Table) -> DataFrame:
        if table.logic is None:
            raise ValueError(f"{table.fqn}: no 'source' and no 'logic' - nothing to run")

        path = self._base_dir / table.logic
        text = path.read_text()

        if path.suffix == ".sql":
            return self._spark.sql(text)
        if path.suffix == ".py":
            return self._run_pyspark_logic(path, text)
        raise ValueError(f"{path}: don't know how to run a {path.suffix!r} logic file")

    def _run_pyspark_logic(self, path: Path, text: str) -> DataFrame:
        # Convention: the file's final output is whatever it assigns to a
        # module-level `result` - the PySpark analogue of "one output table
        # per file" from the dependency-parsing convention.
        namespace: dict[str, Any] = {"spark": self._spark}
        exec(compile(text, str(path), "exec"), namespace)  # noqa: S102 - trusted repo logic files
        result = namespace.get("result")
        if not isinstance(result, DataFrame):
            raise ValueError(
                f"{path}: a PySpark logic file must assign its output DataFrame to `result`"
            )
        return result

    def _read_local_file_source(self, table: Table, system: System) -> DataFrame:
        if system.type not in _LOCAL_FILE_TYPES:
            raise NotImplementedError(
                f"{table.fqn}: system type {system.type!r} has no reader yet - sqlserver, "
                "kusto, storage_stream and custom sources land in a later phase. Pass a "
                "read_source callable to SparkAdapter to supply one in the meantime."
            )

        path = getattr(system, "path", None)
        if not path:
            raise ValueError(f"{table.fqn}: system type {system.type!r} requires a 'path' property")

        reader = self._spark.read
        if system.type == "csv":
            reader = reader.option("header", "true").option("inferSchema", "true")
        return reader.format(system.type).load(str(self._base_dir / path))
