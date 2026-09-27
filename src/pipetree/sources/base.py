"""The SourceReader registry: how to read *this* system, keyed on
`systems.<name>.type`.

Every reader gets a `SourceContext` bundling everything it might need
(the Spark session, the table and system config, the active Platform for
secret/path resolution, and the config's base directory) rather than a
long, mostly-unused parameter list - a JDBC reader needs the platform for
secrets and nothing else from base_dir, a local file reader is the
reverse.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from pipetree.model import System, Table
from pipetree.platform.base import Platform

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class SourceContext:
    spark: SparkSession
    table: Table
    system: System
    platform: Platform
    base_dir: Path
    init: bool = False


@runtime_checkable
class SourceReader(Protocol):
    def read(self, ctx: SourceContext) -> DataFrame:
        """Read this system's raw data for `ctx.table`."""
        ...


_REGISTRY: dict[str, SourceReader] = {}


def register_reader(type_name: str, reader: SourceReader) -> None:
    """Register (or alias) a reader for a `systems.<name>.type` value."""
    _REGISTRY[type_name] = reader


def get_reader(type_name: str) -> SourceReader:
    try:
        return _REGISTRY[type_name]
    except KeyError:
        raise KeyError(
            f"no source reader registered for system type {type_name!r} "
            f"(known: {sorted(_REGISTRY)})"
        ) from None
