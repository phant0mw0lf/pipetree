"""Audit column values, per part 1: `_inserted_at`, `_updated_at`,
`_is_deleted`, `_execution_id`, `_source_system` on every table, plus
`_valid_from`/`_valid_to`/`_is_current` on scd2.

These return column *values* to plug into a merge's INSERT/UPDATE clauses
- not a whole-DataFrame transform - because insert and update touch a
different subset of these columns: an update must never overwrite
`_inserted_at`, and a delete-close on scd2 must never reopen `_valid_from`.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def insert_audit_values(
    execution_id: int, source_system: str | None, now: int
) -> dict[str, Column]:
    return {
        "_inserted_at": F.lit(now),
        "_updated_at": F.lit(now),
        "_is_deleted": F.lit(False),
        "_execution_id": F.lit(execution_id),
        "_source_system": F.lit(source_system),
    }


def update_audit_values(
    execution_id: int, source_system: str | None, now: int
) -> dict[str, Column]:
    return {
        "_updated_at": F.lit(now),
        "_execution_id": F.lit(execution_id),
        "_source_system": F.lit(source_system),
    }


def scd2_insert_audit_values(
    execution_id: int, source_system: str | None, now: int
) -> dict[str, Column]:
    return {
        **insert_audit_values(execution_id, source_system, now),
        "_valid_from": F.lit(now),
        "_valid_to": F.lit(None).cast("long"),
        "_is_current": F.lit(True),
    }


def scd2_close_values(now: int) -> dict[str, Column]:
    """Close out the current version - on a real change (a new version
    replaces it) or a soft delete (nothing replaces it)."""
    return {"_valid_to": F.lit(now), "_is_current": F.lit(False)}
