"""Pure-Python reference model of the merge strategies.

Written from the merge rules, independent of the Spark implementation. It must
not import pyspark. State rows are dicts of the table columns plus the audit
columns ``_is_deleted``, ``_execution_id`` and (scd2 only) ``_is_current``.
Rows keep insertion order; scd2 versions of a key are oldest first. List order
carries no meaning except scd2 version order within a key, so a harness must
compare in a canonical order. NaN is not supported (Python ``!=`` differs from
Spark null-safe equality).

Order of operations in ``apply_batch``: (1) the NULL-key drop (scd1/scd2, and
unknown_member tables of every strategy; a NULL-key delete
row is counted in ``null_keys_dropped``), (2) the ``delete_mode: ignore`` filter
removing delete rows, (3) dedupe. ``duplicates_dropped`` counts after (1) and (2).

Generator contract: a source row must never carry the sentinel key of an
``unknown_member`` table (``apply_batch`` raises ``ValueError``), and rows that tie
for the top ``sequence_by`` value of a key must be identical (``AmbiguousDedupe``),
because Spark would pick an arbitrary winner.

``sequence_by`` on scd1 also orders rows across batches: an incoming row (after the in-batch
dedupe) whose sequence tuple is older than the stored row's is ignored, delete rows included
(no value change, no audit bump); equal or newer updates, so the later batch wins a tie.
NULL sorts lowest, as in the dedupe. Without ``sequence_by`` the last batch wins. Known
limit: a hard delete removes the row, so a later older row for that key simply inserts.
scd2, replace and append keep arrival order (the last batch wins).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


class AmbiguousDedupe(ValueError):
    """Tied top rows of one key differ, so the Spark winner would be arbitrary."""


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

    def __post_init__(self) -> None:
        if self.strategy not in ("replace", "append", "scd1", "scd2"):
            raise ValueError(f"unknown strategy {self.strategy!r}")
        if self.delete_mode not in ("soft", "hard", "ignore"):
            raise ValueError(f"unknown delete_mode {self.delete_mode!r}")
        if self.strategy == "scd2" and self.delete_mode == "hard":
            raise ValueError("scd2 does not support delete_mode 'hard'")
        cols = set(self.columns)
        for name in ("key", "sequence_by", "ignore"):
            missing = set(getattr(self, name)) - cols
            if missing:
                raise ValueError(f"{name} columns not in columns: {sorted(missing)}")
        if set(self.key) & set(self.ignore):
            raise ValueError("key columns must not be ignored")


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
        # NULL business keys: unknown_member tables (every strategy) drop a row with a NULL in
        # ANY key column - NULL is the unknown member's string key
        kept = [r for r in rows if all(r[c] is not None for c in keys)]
    elif table.strategy in ("replace", "append"):
        # NULL business keys: the drop is for scd1/scd2 only, where the key drives the
        # merge; replace/append keep every row as it comes (a NULL key is data)
        kept = rows
    elif len(keys) == 1:
        # NULL business keys: single-column key NULL -> dropped
        kept = [r for r in rows if r[keys[0]] is not None]
    else:
        # NULL business keys: composite key, a NULL component is a value; drop only an all-NULL key
        kept = [r for r in rows if any(r[c] is not None for c in keys)]
    null_dropped = len(rows) - len(kept)
    rows = kept

    if table.unknown_member:
        sentinel = tuple(table.sentinels[c] for c in keys)
        if any(_key(table, r) == sentinel for r in rows):
            raise ValueError("source row uses the unknown-member sentinel key")

    scd = table.strategy in ("scd1", "scd2")
    dups: int | None = None
    if scd:
        if table.has_delete and table.delete_mode == "ignore":
            # delete_mode ignore: delete rows are removed BEFORE dedupe
            rows = [r for r in rows if r["op"] != "D"]
        # Dedupe: winner per key = greatest sequence_by tuple, else the first row.
        groups: dict[tuple, list[dict]] = {}
        for r in rows:
            groups.setdefault(_key(table, r), []).append(r)
        winners: dict[tuple, dict] = {}
        for k, grp in groups.items():
            if table.sequence_by:
                top = max(_seq(table, r) for r in grp)
                tied = [r for r in grp if _seq(table, r) == top]
            else:
                tied = grp
            if any(_differs(tied[0], r, list(table.columns)) for r in tied[1:]):
                raise AmbiguousDedupe(f"tied rows differ for key {k}")
            winners[k] = tied[0]
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
        if existing is not None and table.sequence_by and _seq(table, new) < _seq(table, existing):
            # sequence_by across batches: an incoming row older than the stored row (compared on
            # the stored sequence_by columns) is ignored - delete rows and a returning
            # soft-deleted key included. Equal or newer: the later batch wins.
            continue
        if _is_delete(table, new):
            if existing is None:
                # a delete for an absent key is a no-op: delete for an absent key is a no-op
                continue
            if table.delete_mode == "hard":
                state.remove(existing)
                del by_key[k]
            elif not existing["_is_deleted"]:
                # a delete for an absent key is a no-op + idempotence: re-deleting a deleted row is
                # a no-op (no bump).
                # Soft delete keeps the existing non-key values, only the audit changes.
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
            # not older than the stored row (checked above), so the later batch wins
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
