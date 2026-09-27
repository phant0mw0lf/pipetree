"""Structure read from the source, never declared in the YAML - part 1 is
explicit about that. A column's type is whatever the engine that read it
inferred: Spark's own CSV/JSON/JDBC schema inference for a source, or a
Delta table's existing metadata for a target.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass(frozen=True)
class ColumnSchema:
    name: str
    data_type: str
    nullable: bool


def infer_schema(
    df: DataFrame, *, exclude: frozenset[str] = frozenset()
) -> dict[str, ColumnSchema]:
    return {
        field.name: ColumnSchema(field.name, field.dataType.simpleString(), field.nullable)
        for field in df.schema.fields
        if field.name not in exclude
    }
