"""Pure-Python reference model of the merge strategies.

Written from the merge rules, independent of the Spark implementation. It must
not import pyspark. State rows are dicts of the table columns plus the audit
columns ``_is_deleted``, ``_execution_id`` and (scd2 only) ``_is_current``.
Rows keep insertion order; scd2 versions of a key are oldest first.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

UNKNOWN = "__unknown_member__"


@dataclass(frozen=True)
class ModelTable:
    strategy: str  # replace | append | scd1 | scd2
    key: tuple[str, ...]
    columns: tuple[str, ...]  # every non-audit column, incl. key columns and op
    sequence_by: tuple[str, ...] = ()
    ignore: tuple[str, ...] = ()
    delete_mode: str = "soft"  # soft | hard | ignore
    has_delete: bool = False
    unknown_member: bool = False
    sentinels: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class BatchStats:
    duplicates_dropped: int | None
    null_keys_dropped: int


def _key(table: ModelTable, row: dict) -> tuple:
    return tuple(row[c] for c in table.key)


def is_unknown_member(table: ModelTable, row: dict) -> bool:
    """True for the seeded unknown-member row (sentinel key, not deleted)."""
    return all(row[c] == table.sentinels[c] for c in table.key) and not row["_is_deleted"]


def _is_delete(table: ModelTable, row: dict) -> bool:
    return table.has_delete and row["op"] == "D"


def _seq(table: ModelTable, row: dict) -> tuple:
    # None sorts lowest (desc_nulls_last picks the highest, None last).
    return tuple((row[c] is not None, 0 if row[c] is None else row[c]) for c in table.sequence_by)


def _differs(a: dict, b: dict, cols: list[str]) -> bool:
    return any(a[c] != b[c] for c in cols)


def apply_batch(
    table: ModelTable,
    state: list[dict],
    batch: list[dict],
    execution_id: int,
    *,
    init: bool = False,
) -> tuple[list[dict], BatchStats]:
    n = execution_id
    # init: a full reload is the batch applied to an empty state.
    new_state = [] if init else [dict(r) for r in state]
    rows = [dict(r) for r in batch]

    # Null-key rule (NULL business keys).
    keys = table.key
    if table.unknown_member:
        # NULL business keys: unknown_member tables drop a row with a NULL in ANY key column
        kept = [r for r in rows if all(r[c] is not None for c in keys)]
    elif len(keys) == 1:
        # NULL business keys: single-column key NULL -> dropped
        kept = [r for r in rows if r[keys[0]] is not None]
    else:
        # NULL business keys: composite key, a NULL component is a value; drop only an all-NULL key
        kept = [r for r in rows if any(r[c] is not None for c in keys)]
    null_dropped = len(rows) - len(kept)
    rows = kept

    scd = table.strategy in ("scd1", "scd2")
    dups: int | None = None
    if scd:
        if table.has_delete and table.delete_mode == "ignore":
            # delete_mode ignore: delete rows are removed BEFORE dedupe
            rows = [r for r in rows if r["op"] != "D"]
        # Dedupe: winner per key = greatest sequence_by tuple, else the first row.
        winners: dict[tuple, dict] = {}
        for r in rows:
            k = _key(table, r)
            if k not in winners or table.sequence_by and _seq(table, r) > _seq(table, winners[k]):
                winners[k] = r
        dups = len(rows) - len(winners)
        rows = list(winners.values())

    if table.strategy == "replace":
        new_state = [_audit(r, n, table) for r in rows]
    elif table.strategy == "append":
        new_state += [_audit(r, n, table) for r in rows]
    elif table.strategy == "scd1":
        _apply_scd1(table, new_state, rows, n)
    else:
        _apply_scd2(table, new_state, rows, n)

    if table.unknown_member and not any(is_unknown_member(table, r) for r in new_state):
        # Seeded once, never touched; replace rebuilt the table so it is re-added.
        member: dict[str, Any] = dict.fromkeys(table.columns)
        member.update({c: table.sentinels[c] for c in keys})
        member.update(_is_deleted=False, _execution_id=n)
        if table.strategy == "scd2":
            member["_is_current"] = True
        new_state.append(member)

    return new_state, BatchStats(duplicates_dropped=dups, null_keys_dropped=null_dropped)


def _audit(row: dict, n: int, table: ModelTable) -> dict:
    out = {c: row[c] for c in table.columns}
    out.update(_is_deleted=False, _execution_id=n)
    if table.strategy == "scd2":
        out["_is_current"] = True
    return out


def _apply_scd1(table: ModelTable, state: list[dict], winners: list[dict], n: int) -> None:
    by_key = {_key(table, r): r for r in state}
    tracked_cols = [c for c in table.columns if c not in table.key and c not in table.ignore]
    ignored_cols = [c for c in table.ignore if c not in table.key]
    for new in winners:
        k = _key(table, new)
        existing = by_key.get(k)
        if _is_delete(table, new):
            if existing is None:
                # a delete for an absent key is a no-op: delete for an absent key is a no-op
                continue
            if table.delete_mode == "hard":
                state.remove(existing)
                del by_key[k]
            else:
                existing.update(_is_deleted=True, _execution_id=n)
            continue
        if existing is None:
            # a hard-deleted key that returns is a fresh insert: after a hard delete the key is
            # simply absent -> fresh insert
            row = _audit(new, n, table)
            state.append(row)
            by_key[k] = row
        elif _differs(existing, new, tracked_cols) or existing["_is_deleted"]:
            # a returning soft-deleted key is active again: a soft-deleted key returns active, even
            # with identical values
            # the last batch wins: the last batch wins regardless of sequence_by
            for c in table.columns:
                if c not in table.key:
                    existing[c] = new[c]
            existing.update(_is_deleted=False, _execution_id=n)
        elif _differs(existing, new, ignored_cols):
            # ignore_columns alone update in place: ignore_columns alone update in place, no
            # execution id bump
            for c in ignored_cols:
                existing[c] = new[c]


def _apply_scd2(table: ModelTable, state: list[dict], winners: list[dict], n: int) -> None:
    tracked_cols = [c for c in table.columns if c not in table.key and c not in table.ignore]
    ignored_cols = [c for c in table.ignore if c not in table.key]
    for new in winners:
        k = _key(table, new)
        current = next((r for r in state if r["_is_current"] and _key(table, r) == k), None)
        if _is_delete(table, new):
            if current is not None:
                current.update(_is_current=False, _is_deleted=True, _execution_id=n)
            # a delete for an absent key is a no-op: absent key -> no-op
            continue
        if current is None:
            # new key, or returning after a delete (a returning scd2 key opens a new version): a NEW
            # current version,
            # the closed deleted one stays closed
            state.append(_audit(new, n, table))
        elif _differs(current, new, tracked_cols):
            current.update(_is_current=False, _execution_id=n)
            state.append(_audit(new, n, table))
        elif _differs(current, new, ignored_cols):
            # ignore_columns alone update in place: ignore_columns alone update in place, no new
            # version, no bump
            for c in ignored_cols:
                current[c] = new[c]
