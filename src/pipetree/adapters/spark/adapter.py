"""SparkAdapter: the Adapter protocol, backed by Spark + Delta.

Reading a `source` table's raw input goes through the `pipetree.sources`
registry, keyed on `systems.<name>.type` - csv/json/parquet, sqlserver,
kusto, storage_stream (plus its d365_export/synapse_link aliases), or a
`custom` dotted-path class. Pass `read_source` to bypass the registry
entirely with your own callable instead (tests use this; so could a
one-off table that doesn't fit the registry).
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
    strip_reserved_columns,
)
from pipetree.model import System, Table
from pipetree.platform.base import Platform
from pipetree.platform.local import LocalPlatform
from pipetree.sources import get_reader
from pipetree.sources.base import SourceContext
from pipetree.sources.custom import load_custom_reader

MergeFn = Callable[[SparkSession, Table, DataFrame, int, "str | None", bool], dict]
ReadSourceFn = Callable[[Table, System], DataFrame]

_MERGE_FUNCTIONS: dict[str, MergeFn] = {
    "scd1": merge_scd1,
    "scd2": merge_scd2,
    "replace": merge_replace,
    "append": merge_append,
}


class SparkAdapter:
    capabilities = Capabilities(supports_delete_by_execution_id=True)

    def __init__(
        self,
        spark: SparkSession,
        systems: dict[str, System],
        base_dir: str | Path,
        read_source: ReadSourceFn | None = None,
        platform: Platform | None = None,
    ) -> None:
        self._spark = spark
        self._systems = systems
        self._base_dir = Path(base_dir)
        self._platform = platform or LocalPlatform()
        self._read_source_override = read_source

    def run_table(
        self, table: Table, *, execution_id: int, init: bool = False
    ) -> dict[str, Any] | None:
        if table.source is not None:
            system = self._systems[table.source.system]
            if self._read_source_override is not None:
                source_df = self._read_source_override(table, system)
            else:
                source_df = self._read_via_registry(table, system, init)
            source_system = table.source.system
        else:
            source_df = self._run_logic(table)
            source_system = None

        # The one choke point every strategy's input passes through, for a
        # source read and a logic file alike: whatever audit columns the
        # input inherited, the merge below stamps this table's own.
        source_df = strip_reserved_columns(source_df, table)

        merge_fn = _MERGE_FUNCTIONS[table.strategy]
        result = merge_fn(self._spark, table, source_df, execution_id, source_system, init)

        if table.unknown_member:
            seed_unknown_member(self._spark, table, execution_id)

        return result

    def delete_by_execution_id(self, table: Table, execution_id: int) -> None:
        if not self._spark.catalog.tableExists(table.fqn):
            return
        DeltaTable.forName(self._spark, table.fqn).delete(f"_execution_id = {execution_id}")

    def _read_via_registry(self, table: Table, system: System, init: bool) -> DataFrame:
        if system.type == "custom":
            class_path = getattr(system, "class", None)
            if not class_path:
                raise ValueError(f"{table.fqn}: system type 'custom' requires a 'class' property")
            reader = load_custom_reader(class_path)
        else:
            reader = get_reader(system.type)

        ctx = SourceContext(self._spark, table, system, self._platform, self._base_dir, init=init)
        return reader.read(ctx)

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
