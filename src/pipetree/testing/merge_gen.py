"""Seeded generator of merge-conformance cases: a random but valid table config
plus a short history of source batches built to reach the interesting cases
(duplicates, deletes, resurrection, late sequence values, NULL keys, repeated
and empty batches, an ``init`` midway).

Pure Python, no pyspark import. Deterministic from ``(seed, strategy)``: all
randomness comes from ``random.Random(f"{strategy}:{seed}")``.

The generator honours the model's contract (see ``merge_model``): rows of one
key with an equal ``sequence_by`` tuple (or any two rows of one key without
``sequence_by``) are identical, no source key equals an unknown-member
sentinel (no int key is ever ``-1``), scd2 never uses ``delete_mode: hard``,
``op`` is ``"D"`` for a delete row; any other row has ``"U"``, or, on a table with a delete
flag, ``None`` for a deterministic share of them (the delete expression is then NULL: not a
delete).

Type options (a post-pass, ``_with_types``): after the case above is generated, a SEPARATE
``random.Random(f"types:{strategy}:{seed}")`` decides on four extra options, so the random
stream of the base generator - and every case that uses none of the options - is unchanged:

- ``sequence_by`` columns that are not integers: the integer sequence values of ``seq`` (and of
  ``w`` when ``sequence_by`` is ``(seq, w)``) are mapped, order-preserving and injectively, to
  ``timestamp`` (2024-01-01 + n minutes), ``string`` (``f"{n:06d}"``, binary order), ``decimal``
  (``n / 4``, ``decimal(10,2)``) or ``double`` (``n / 2``); NULL stays NULL. The Spark column
  types are in ``Case.column_types``. NaN is NOT generated: the model's ``_seq`` and its row
  comparison (Python ``!=``) do not support NaN (Spark orders NaN highest and equal to itself).
- a composite ``sequence_by`` of two columns of different types (``seq`` and ``w`` drawn
  independently).
- ``seq`` also listed in ``ignore_columns`` (scd1/scd2): the sequence column is ignored for
  change detection but still orders the rows.
- ``surrogate_key`` without ``unknown_member`` (scd1/scd2).
"""

from __future__ import annotations

import random
import zlib
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from pipetree.testing.merge_model import ModelTable, apply_batch, is_unknown_member

STRATEGIES = ("replace", "append", "scd1", "scd2")

COVERAGE_NAMES = (
    "duplicate_rows",
    "duplicate_with_equal_sequence",
    "delete_tie_with_data_row",
    "delete_row",
    "delete_for_absent_key",
    "resurrection",
    "ignored_only_change",
    "tracked_change",
    "empty_batch",
    "repeated_batch",
    "null_key",
    "partial_null_key",
    "late_older_sequence",
    "init_midway",
    "sequence_timestamp",
    "sequence_string",
    "sequence_decimal",
    "sequence_double",
    "sequence_in_ignore_columns",
    "surrogate_key_without_unknown_member",
    "composite_sequence_mixed_types",
)

DELETE_WHEN = "op = 'D'"
SURROGATE_KEY = "sid"

_INJECT = 0.15  # probability of each deliberate injection per batch


@dataclass(frozen=True)
class Case:
    seed: int  # -1 for a hand-built case
    table: ModelTable
    delete_when: str | None
    surrogate_key: str | None
    batches: list[list[dict]]
    inits: frozenset[int]
    # Spark DDL type per column that is not the default (int/string); empty for a plain case.
    column_types: dict[str, str] = field(default_factory=dict)

    def repr_line(self) -> str:
        t = self.table
        origin = (
            f"generate_case({self.seed}, {t.strategy!r})"
            if self.seed >= 0
            else f"hand-built {t.strategy} case"
        )
        delete = f"{t.delete_mode}" if t.has_delete else "none"
        types = f" column_types={self.column_types}" if self.column_types else ""
        return (
            f"{origin}: key={list(t.key)} sequence_by={list(t.sequence_by)} "
            f"ignore={list(t.ignore)} delete={delete} unknown_member={t.unknown_member} "
            f"surrogate_key={self.surrogate_key} batches={len(self.batches)} "
            f"inits={sorted(self.inits)}{types}"
        )


def generate_case(seed: int, strategy: str) -> Case:
    if strategy not in STRATEGIES:
        raise ValueError(f"unknown strategy {strategy!r}")
    rng = random.Random(f"{strategy}:{seed}")
    scd = strategy in ("scd1", "scd2")

    # ---- table config
    key_kind = rng.choice(("int", "str", "composite"))
    key: tuple[str, ...] = ("a", "b") if key_kind == "composite" else ("id",)
    ignore: tuple[str, ...] = ("seen",) if rng.random() < 0.5 else ()
    columns = key + ("v", "w", "seq", "op") + ignore
    sequence_by: tuple[str, ...] = rng.choice(((), ("seq",), ("seq", "w")))
    has_delete = scd and rng.random() < 0.6
    delete_mode = "soft"
    if has_delete:
        delete_mode = rng.choice(
            ("soft", "hard", "ignore") if strategy == "scd1" else ("soft", "ignore")
        )
    unknown_member = rng.random() < 0.25
    sentinels: dict[str, Any] = {}
    if unknown_member:
        sentinels = dict.fromkeys(key, None if key_kind == "str" else -1)
    surrogate_key = SURROGATE_KEY if scd and unknown_member and rng.random() < 0.5 else None
    table = ModelTable(
        strategy=strategy,
        key=key,
        columns=columns,
        sequence_by=sequence_by,
        ignore=ignore,
        delete_mode=delete_mode,
        has_delete=has_delete,
        unknown_member=unknown_member,
        sentinels=sentinels,
    )

    # ---- key space: 3-8 keys, never the int sentinel -1
    n_keys = rng.randint(3, 8)
    if key_kind == "int":
        pool: list[tuple] = [(k,) for k in rng.sample(range(20), n_keys)]
        absent = [(50 + i,) for i in range(10)]
    elif key_kind == "str":
        pool = [(f"k{k}",) for k in rng.sample(range(20), n_keys)]
        absent = [(f"x{i}",) for i in range(10)]
    else:
        pool = rng.sample([(a, b) for a in range(4) for b in range(4)], n_keys)
        absent = [(9, i) for i in range(10)]

    # ---- history
    seq_counter = 0
    recent: list[tuple] = []

    def make_row(k: tuple, op: str | None = None, *, fresh: bool = False) -> dict:
        nonlocal seq_counter
        if fresh or rng.random() < 0.6:
            seq_counter += 1
            seq = seq_counter
        else:  # late/older (or missing) sequence value
            seq = rng.choice([None, *range(seq_counter + 1)])
        row: dict[str, Any] = dict(zip(key, k, strict=True))
        row.update(
            v=rng.choice(("a", "b", "c")),
            w=rng.choice((None, 0, 1, 2)),
            seq=seq,
            op=op or ("D" if has_delete and rng.random() < 0.2 else "U"),
        )
        if ignore:
            row["seen"] = rng.randint(0, 3)
        return row

    n_batches = rng.randint(2, 6)
    batches: list[list[dict]] = []
    for i in range(n_batches):
        if i > 0 and rng.random() < _INJECT:  # a verbatim repeated batch
            batches.append([dict(r) for r in batches[-1]])
            continue
        if rng.random() < _INJECT:  # an empty batch
            batches.append([])
            continue
        rows: list[dict] = []
        for _ in range(rng.randint(1, 6)):
            k = rng.choice(recent) if recent and rng.random() < 0.7 else rng.choice(pool)
            rows.append(make_row(k))
            recent.append(k)
        if rng.random() < _INJECT:  # a duplicate of a row
            src = rng.choice(rows)
            if sequence_by and rng.random() < 0.5:
                dup = dict(src)  # same key, other content, DIFFERENT sequence
                dup["v"] = rng.choice([v for v in ("a", "b", "c") if v != src["v"]])
                dup["seq"] = (src["seq"] if src["seq"] is not None else 0) + rng.randint(1, 3)
            else:
                dup = dict(src)  # identical (an equal-sequence tie)
            rows.insert(rng.randint(0, len(rows)), dup)
        if has_delete and rng.random() < _INJECT:  # a delete for an absent key
            rows.append(make_row(rng.choice(absent), op="D"))
        if rng.random() < _INJECT:  # a NULL key row (all-NULL, or partial when composite)
            if len(key) > 1 and rng.random() < 0.5:
                nk: tuple = (
                    (None, rng.choice(pool)[1])
                    if rng.random() < 0.5
                    else (rng.choice(pool)[0], None)
                )
            else:
                nk = (None,) * len(key)
            rows.append(make_row(nk))
        if has_delete and recent and rng.random() < _INJECT:
            # a delete of a known key that wins its key's dedupe (fresh, highest sequence)
            rows.append(make_row(rng.choice(recent), op="D", fresh=True))
        deleted = [r for b in batches for r in b if r["op"] == "D"]
        if deleted and rng.random() < 2 * _INJECT:
            # a resurrection: a key deleted in an earlier batch comes back
            rows.append(make_row(_key(table, rng.choice(deleted)), op="U", fresh=True))
        if ignore and batches and batches[-1] and rng.random() < _INJECT:
            # an ignored-column-only change: a previous row re-sent with a new `seen`
            again = dict(rng.choice(batches[-1]))
            again["seen"] = (again["seen"] + 1) % 4
            rows.append(again)
        rng.shuffle(rows)
        batch = _resolve_ties(rows[:8], table)
        if has_delete:
            _null_some_flags(batch, table, seed)
        batches.append(batch)

    inits: frozenset[int] = frozenset()
    if rng.random() < 0.10:
        inits = frozenset({rng.randint(1, n_batches - 1)})

    if has_delete and delete_mode != "ignore":
        batches = _add_delete_ties(batches, table, random.Random(f"deltie:{strategy}:{seed}"))

    case = Case(
        seed=seed,
        table=table,
        delete_when=DELETE_WHEN if has_delete else None,
        surrogate_key=surrogate_key,
        batches=batches,
        inits=inits,
    )
    return _with_types(case, strategy, seed)


_BASE = datetime(2024, 1, 1)

# type name -> (Spark DDL type, int -> value); each mapping is injective and order preserving.
_SEQ_TYPES: dict[str, tuple[str, Any]] = {
    "int": ("int", lambda n: n),
    "timestamp": ("timestamp", lambda n: _BASE + timedelta(minutes=n)),
    "string": ("string", lambda n: f"{n:06d}"),
    "decimal": ("decimal(10,2)", lambda n: Decimal(n) / 4),
    "double": ("double", lambda n: n / 2),
}
_SEQ_TYPE_NAMES = tuple(_SEQ_TYPES)


def _with_types(case: Case, strategy: str, seed: int) -> Case:
    """The type options post-pass (see the module docstring). All draws are made
    unconditionally, in a fixed order, so one option never shifts another."""
    rng = random.Random(f"types:{strategy}:{seed}")
    seq_type = rng.choice(_SEQ_TYPE_NAMES)
    w_type = rng.choice(_SEQ_TYPE_NAMES)
    ignore_seq = rng.random() < 0.25
    add_sid = rng.random() < 0.4

    t = case.table
    scd = t.strategy in ("scd1", "scd2")
    surrogate_key = case.surrogate_key
    if scd and surrogate_key is None and not t.unknown_member and add_sid:
        surrogate_key = SURROGATE_KEY
    if scd and "seq" in t.sequence_by and ignore_seq and "seq" not in t.ignore:
        t = replace(t, ignore=(*t.ignore, "seq"))

    typed = {c: n for c, n in (("seq", seq_type), ("w", w_type)) if c in t.sequence_by}
    typed = {c: n for c, n in typed.items() if n != "int"}
    batches = case.batches
    if typed:
        batches = [
            [
                {
                    **r,
                    **{
                        c: None if r[c] is None else _SEQ_TYPES[n][1](r[c])
                        for c, n in typed.items()
                    },
                }
                for r in batch
            ]
            for batch in case.batches
        ]
    return replace(
        case,
        table=t,
        surrogate_key=surrogate_key,
        batches=batches,
        column_types={c: _SEQ_TYPES[n][0] for c, n in typed.items()},
    )


def _null_some_flags(rows: list[dict], table: ModelTable, seed: int) -> None:
    """Turn ``op`` into NULL (a NULL delete expression) for about one update row in seven.

    The choice depends only on (seed, key, sequence tuple), so rows that tie are still
    identical, and it draws nothing from the history's random stream: the histories of
    every seed stay what they were apart from these values."""
    for r in rows:
        if r["op"] != "U":
            continue
        tie = (tuple(r[c] for c in table.key), tuple(r[c] for c in table.sequence_by))
        if zlib.crc32(f"{seed}:{tie}".encode()) % 7 == 0:
            r["op"] = None


def _resolve_ties(rows: list[dict], table: ModelTable) -> list[dict]:
    """Rows of one key with an equal ``sequence_by`` tuple (every row of a key
    without ``sequence_by``) become copies of the first such row: Spark would
    pick an arbitrary winner among different tied rows."""
    first: dict[tuple, dict] = {}
    out = []
    for r in rows:
        tie = (tuple(r[c] for c in table.key), tuple(r[c] for c in table.sequence_by))
        if tie in first:
            out.append(dict(first[tie]))
        else:
            first[tie] = r
            out.append(r)
    return out


def _add_delete_ties(batches: list[list[dict]], table: ModelTable, rng: random.Random):
    """A deterministic post-pass on its own random stream (the history of a seed
    is otherwise unchanged): some batches get a delete row tied with a data row of
    one key (equal ``sequence_by`` tuple; any two rows without ``sequence_by``).
    The delete wins that tie; tied rows of one kind stay identical."""
    out = []
    for batch in batches:
        batch = list(batch)
        if rng.random() < _INJECT:
            data = [r for r in batch if r["op"] != "D" and all(r[c] is not None for c in table.key)]
            if data and len(batch) < 8:
                src = rng.choice(data)
                tie = (_key(table, src), tuple(src[c] for c in table.sequence_by))
                has_delete = any(
                    r["op"] == "D"
                    and (_key(table, r), tuple(r[c] for c in table.sequence_by)) == tie
                    for r in batch
                )
                if not has_delete:
                    batch.insert(rng.randint(0, len(batch)), {**src, "op": "D"})
        out.append(batch)
    return out


# --------------------------------------------------------------------- coverage


def _key(table: ModelTable, row: dict) -> tuple:
    return tuple(row[c] for c in table.key)


def _rows_by_key(table: ModelTable, state: list[dict]) -> tuple[dict, dict]:
    """(present, active): the key's row (scd1) / current row (scd2), excluding the
    unknown member; active = present and not deleted."""
    present: dict[tuple, dict] = {}
    for r in state:
        if table.unknown_member and is_unknown_member(table, r):
            continue
        if table.strategy == "scd2" and not r["_is_current"]:
            continue
        present[_key(table, r)] = r
    active = {k: r for k, r in present.items() if not r["_is_deleted"]}
    return present, active


def _seq_tuple(table: ModelTable, row: dict) -> tuple | None:
    vals = tuple(row[c] for c in table.sequence_by)
    return None if not vals or any(v is None for v in vals) else vals


def coverage(case: Case) -> set[str]:
    """The interesting situations ``case`` contains. Batch-shape names are read
    off the batches; state-relative names come from replaying the model:

    - duplicate_rows: a batch has >= 2 rows of one fully non-NULL key.
    - duplicate_with_equal_sequence: such rows share their ``sequence_by`` tuple
      (``sequence_by`` set).
    - delete_tie_with_data_row: such a tie (delete_mode not ignore) holds a delete row
      and a data row; the delete wins.
    - delete_row: a batch has a delete row (``has_delete``, ``op == "D"``).
    - delete_for_absent_key: a delete row, the only row of its key in the batch,
      on a table whose ``delete_mode`` is not ignore, for a key with no row
      (scd1) / no current row (scd2) in the model state before the batch.
    - resurrection (scd): a key deleted before the batch (scd1: soft-deleted row
      or hard-deleted = absent but present in an earlier state since the last
      init; scd2: versions but no current one) is active after it.
    - tracked_change / ignored_only_change (scd): a key active before and after
      the batch whose (current) row changed in a tracked column / changed only
      in ignored columns.
    - empty_batch: a batch without rows. repeated_batch: a non-empty batch equal
      to the previous one.
    - null_key: a row whose key columns are all NULL. partial_null_key: some but
      not all key columns NULL (composite keys).
    - late_older_sequence (``sequence_by`` set): a row whose sequence tuple is
      lower than that of its key's active row before the batch (no NULLs).
    - init_midway: ``init`` on a batch after the first.
    - sequence_timestamp / _string / _decimal / _double: a ``sequence_by`` column of that type.
    - sequence_in_ignore_columns: a ``sequence_by`` column that is also in ``ignore_columns``.
    - surrogate_key_without_unknown_member: a surrogate key on a table without unknown member.
    - composite_sequence_mixed_types: ``sequence_by`` of two columns of different types.
    """
    t = case.table
    got: set[str] = set()
    seq_kinds = {c: case.column_types.get(c, "int").split("(")[0] for c in t.sequence_by}
    for kind in ("timestamp", "string", "decimal", "double"):
        if kind in seq_kinds.values():
            got.add(f"sequence_{kind}")
    if set(t.sequence_by) & set(t.ignore):
        got.add("sequence_in_ignore_columns")
    if case.surrogate_key is not None and not t.unknown_member:
        got.add("surrogate_key_without_unknown_member")
    if len(set(seq_kinds.values())) > 1:
        got.add("composite_sequence_mixed_types")
    scd = t.strategy in ("scd1", "scd2")
    tracked = [c for c in t.columns if c not in t.key and c not in t.ignore]
    state: list[dict] = []
    ever: set[tuple] = set()  # keys with a row in some state since the last init
    for i, batch in enumerate(case.batches):
        init = i in case.inits
        if init and i > 0:
            got.add("init_midway")
        if not batch:
            got.add("empty_batch")
        if i > 0 and batch and batch == case.batches[i - 1]:
            got.add("repeated_batch")

        groups: dict[tuple, list[dict]] = {}
        for r in batch:
            groups.setdefault(_key(t, r), []).append(r)
        for k, grp in groups.items():
            nulls = sum(v is None for v in k)
            if nulls == len(k):
                got.add("null_key")
            elif nulls:
                got.add("partial_null_key")
            if nulls == 0 and len(grp) >= 2:
                got.add("duplicate_rows")
                if t.sequence_by:
                    seqs = [tuple(r[c] for c in t.sequence_by) for r in grp]
                    if len(set(seqs)) < len(seqs):
                        got.add("duplicate_with_equal_sequence")
                if t.has_delete and t.delete_mode != "ignore":
                    by_seq: dict[tuple, set[bool]] = {}
                    for r in grp:
                        tie = tuple(r[c] for c in t.sequence_by)
                        by_seq.setdefault(tie, set()).add(r["op"] == "D")
                    if any(len(kinds) == 2 for kinds in by_seq.values()):
                        got.add("delete_tie_with_data_row")

        base = [] if init else state
        if init:
            ever = set()
        present_before, active_before = _rows_by_key(t, base)
        new_state, _ = apply_batch(t, state, batch, i + 1, init=init)
        _, active_after = _rows_by_key(t, new_state)

        for r in batch:
            k = _key(t, r)
            if t.has_delete and r["op"] == "D":
                got.add("delete_row")
                effective = t.delete_mode != "ignore" and len(groups[k]) == 1
                if effective and k not in present_before and all(v is not None for v in k):
                    got.add("delete_for_absent_key")
            elif t.sequence_by and k in active_before:
                new_seq, old_seq = _seq_tuple(t, r), _seq_tuple(t, active_before[k])
                if new_seq is not None and old_seq is not None and new_seq < old_seq:
                    got.add("late_older_sequence")

        if scd:
            for k, after in active_after.items():
                was_deleted = (k in present_before and k not in active_before) or (
                    k not in present_before and k in ever
                )
                if t.strategy == "scd2":
                    was_deleted = k not in active_before and any(
                        _key(t, r) == k and not r["_is_current"] for r in base
                    )
                if was_deleted:
                    got.add("resurrection")
                before = active_before.get(k)
                if before is None:
                    continue
                if any(before[c] != after[c] for c in tracked):
                    got.add("tracked_change")
                elif any(before[c] != after[c] for c in t.ignore):
                    got.add("ignored_only_change")

        state = new_state
        ever |= set(_rows_by_key(t, state)[0])
    return got
