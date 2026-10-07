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
``op`` is ``"D"`` for a delete row and ``"U"`` otherwise.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any

from pipetree.testing.merge_model import ModelTable, apply_batch, is_unknown_member

STRATEGIES = ("replace", "append", "scd1", "scd2")

COVERAGE_NAMES = (
    "duplicate_rows",
    "duplicate_with_equal_sequence",
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

    def repr_line(self) -> str:
        t = self.table
        origin = (
            f"generate_case({self.seed}, {t.strategy!r})"
            if self.seed >= 0
            else f"hand-built {t.strategy} case"
        )
        delete = f"{t.delete_mode}" if t.has_delete else "none"
        return (
            f"{origin}: key={list(t.key)} sequence_by={list(t.sequence_by)} "
            f"ignore={list(t.ignore)} delete={delete} unknown_member={t.unknown_member} "
            f"surrogate_key={self.surrogate_key} batches={len(self.batches)} "
            f"inits={sorted(self.inits)}"
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
        batches.append(_resolve_ties(rows[:8], table))

    inits: frozenset[int] = frozenset()
    if rng.random() < 0.10:
        inits = frozenset({rng.randint(1, n_batches - 1)})

    return Case(
        seed=seed,
        table=table,
        delete_when=DELETE_WHEN if has_delete else None,
        surrogate_key=surrogate_key,
        batches=batches,
        inits=inits,
    )


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
    """
    t = case.table
    got: set[str] = set()
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
