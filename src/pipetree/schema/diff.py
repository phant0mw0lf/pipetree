"""Classifies the difference between a source's schema and a target
table's existing schema: added, removed, retyped, or a nullability
change. Pure comparison over `ColumnSchema` dicts - no engine needed."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pipetree.schema.infer import ColumnSchema

Kind = Literal["added", "removed", "retyped", "nullability"]


@dataclass(frozen=True)
class SchemaChange:
    kind: Kind
    column: str
    old_type: str | None = None
    new_type: str | None = None
    old_nullable: bool | None = None
    new_nullable: bool | None = None

    def __str__(self) -> str:
        if self.kind == "added":
            return f"added: {self.column} ({self.new_type})"
        if self.kind == "removed":
            return f"removed: {self.column} ({self.old_type})"
        if self.kind == "retyped":
            return f"retyped: {self.column} ({self.old_type} -> {self.new_type})"
        return f"nullability changed: {self.column} ({self.old_nullable} -> {self.new_nullable})"


def diff_schemas(
    source: dict[str, ColumnSchema], target: dict[str, ColumnSchema]
) -> list[SchemaChange]:
    changes: list[SchemaChange] = []

    for name, col in source.items():
        tgt = target.get(name)
        if tgt is None:
            changes.append(SchemaChange("added", name, new_type=col.data_type))
        elif col.data_type != tgt.data_type:
            changes.append(
                SchemaChange("retyped", name, old_type=tgt.data_type, new_type=col.data_type)
            )
        elif col.nullable != tgt.nullable:
            changes.append(
                SchemaChange(
                    "nullability", name, old_nullable=tgt.nullable, new_nullable=col.nullable
                )
            )

    for name, col in target.items():
        if name not in source:
            changes.append(SchemaChange("removed", name, old_type=col.data_type))

    return sorted(changes, key=lambda c: c.column)
