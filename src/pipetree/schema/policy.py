"""What to do about schema drift, per part 1's `schema_policy`: `evolve`
(default), `fail`, or `ignore`.

"You don't declare columns" means drift is the normal case, not an
exceptional one - the question `reconcile()` answers is only how much of
it a given table tolerates silently.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pipetree.schema.diff import SchemaChange, diff_schemas
from pipetree.schema.errors import SchemaError
from pipetree.schema.infer import ColumnSchema

# Numeric widenings safe enough to apply without asking: every pair here
# can hold every value the narrower type could, so no existing row's data
# is at risk from the retype. Spark's own simpleString() names
# ("bigint", "tinyint", ...), not Python's ("long", "byte", ...).
_SAFE_WIDENINGS = {
    ("tinyint", "smallint"),
    ("tinyint", "int"),
    ("tinyint", "bigint"),
    ("tinyint", "float"),
    ("tinyint", "double"),
    ("smallint", "int"),
    ("smallint", "bigint"),
    ("smallint", "float"),
    ("smallint", "double"),
    ("int", "bigint"),
    ("int", "float"),
    ("int", "double"),
    ("bigint", "float"),
    ("bigint", "double"),
    ("float", "double"),
}


def is_safe_widening(change: SchemaChange) -> bool:
    return change.kind == "retyped" and (change.old_type, change.new_type) in _SAFE_WIDENINGS


@dataclass(frozen=True)
class SchemaReconciliation:
    columns: list[str]
    changes: list[SchemaChange] = field(default_factory=list)
    needs_schema_evolution: bool = False
    # The target's own columns, before this write - callers need this to
    # know which of `columns` have no existing target value to compare
    # against yet (a just-added column can't be part of a "did this row
    # actually change" comparison; it has no old value to compare to).
    target_columns: frozenset[str] = frozenset()


def reconcile(
    source_columns: list[str],
    source_schema: dict[str, ColumnSchema],
    target_schema: dict[str, ColumnSchema] | None,
    policy: str,
    table_fqn: str,
) -> SchemaReconciliation:
    if target_schema is None:
        # First write: nothing to reconcile against yet.
        return SchemaReconciliation(columns=source_columns)

    target_columns = frozenset(target_schema)
    changes = diff_schemas(source_schema, target_schema)
    if not changes:
        return SchemaReconciliation(columns=source_columns, target_columns=target_columns)

    if policy == "fail":
        raise SchemaError(table_fqn, changes)

    if policy == "ignore":
        kept = [c for c in source_columns if c in target_schema]
        return SchemaReconciliation(columns=kept, changes=changes, target_columns=target_columns)

    # evolve
    unsafe = [c for c in changes if c.kind == "retyped" and not is_safe_widening(c)]
    if unsafe:
        raise SchemaError(table_fqn, unsafe, reason="unsafe retype under schema_policy: evolve")

    added = any(c.kind == "added" for c in changes)
    return SchemaReconciliation(
        columns=source_columns,
        changes=changes,
        needs_schema_evolution=added,
        target_columns=target_columns,
    )
