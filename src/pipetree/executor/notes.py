"""Data-quality notes: what a merge quietly changed.

A merge that drops duplicate rows, drops NULL-key rows or adapts to schema
drift still succeeds - so the fact is easy to miss. `notes_for` turns the
details a table's run produced into short strings every surface can show
(console digest, graph badge, live view) without each re-deriving the
wording. Pure: no Spark, no logging.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def notes_for(details: Mapping[str, Any]) -> list[str]:
    notes: list[str] = []
    if (duplicates := details.get("duplicates_dropped") or 0) > 0:
        notes.append(f"{duplicates} duplicate row(s) dropped")
    if (null_keys := details.get("null_keys_dropped") or 0) > 0:
        notes.append(f"{null_keys} NULL-key row(s) dropped")
    notes.extend(f"schema: {change}" for change in details.get("schema_changes") or ())
    return notes
