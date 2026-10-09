"""`QuirkModel`: the reference model with one switch per KNOWN pipetree deviation
(the deviations in `known_divergences.py`), used to ATTRIBUTE divergences.

`run_case(spark, case, model=QuirkModel({"Fn", "Fm"}))` compares the real
table with "the model as if today's quirks Fn and Fm were the rules". A seed
listed under deviations S must match `QuirkModel(S)` over its whole history, and
every flag in S must matter; otherwise the divergence is unexplained.

This is deliberately a transcription of today's pipetree behaviour, one flag
per deviation, each citing the code that causes it (src/pipetree/adapters/spark,
at commit 911c072). It currently has NO flags: all deviations are fixed, so
`QuirkModel()` is the reference model. It lives in tests/, never in the oracle: when a deviation is
fixed in pipetree, delete its flag (and its `tolerates` entry) and regenerate
the lists (`uv run python -m tests.merge_conformance.regen --seeds 300 --write`).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from pipetree.testing.merge_model import (
    AmbiguousDedupe,
    BatchStats,
    ModelTable,
    _apply_scd2_late,
    _audit,
    _differs,
    _is_delete,
    _key,
    _seq,
    is_unknown_member,
)

# One entry per OPEN deviation: id -> one-line description, with a comment citing the
# code that causes it. EMPTY today: every deviation is fixed (null-key-kept/composite-null-key were
# the last).
# To add a deviation, see the docstring of known_divergences.py.
FLAGS: dict[str, str] = {}

# Model-independent invariants a quirk breaks (see merge_harness.INVARIANTS):
# flag -> the invariant names it makes expected to fail.
_TOLERATES: dict[str, frozenset[str]] = {}


class QuirkModel:
    """The reference model with the given quirk flags switched on."""

    def __init__(self, flags: Iterable[str] = ()) -> None:
        self.flags = frozenset(flags)
        unknown = self.flags - set(FLAGS)
        if unknown:
            raise ValueError(f"unknown quirk flags {sorted(unknown)}")

    def __repr__(self) -> str:
        return f"QuirkModel({sorted(self.flags)})"

    def tolerates(self, invariant: str) -> bool:
        return any(invariant in _TOLERATES.get(f, ()) for f in self.flags)

    def apply_batch(
        self,
        table: ModelTable,
        state: list[dict],
        batch: list[dict],
        execution_id: int,
        *,
        init=False,
    ) -> tuple[list[dict], BatchStats]:
        n = execution_id
        new_state = [] if init else [dict(r) for r in state]
        rows = [dict(r) for r in batch]
        keys = table.key
        scd = table.strategy in ("scd1", "scd2")
        if table.unknown_member:
            kept = [r for r in rows if all(r[c] is not None for c in keys)]
        elif not scd:
            kept = rows
        elif len(keys) == 1:
            kept = [r for r in rows if r[keys[0]] is not None]
        else:
            kept = [r for r in rows if any(r[c] is not None for c in keys)]
        null_dropped = len(rows) - len(kept)
        rows = kept

        ignore_mode = table.has_delete and table.delete_mode == "ignore"

        def is_del(r: dict) -> bool:
            return _is_delete(table, r)

        dups = None
        if scd:
            if ignore_mode:
                rows = [r for r in rows if r["op"] != "D"]
            groups: dict[tuple, list[dict]] = {}
            for r in rows:
                groups.setdefault(_key(table, r), []).append(r)
            winners = {}
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
            self._scd1(table, new_state, rows, n, is_del)
        else:
            self._scd2(table, new_state, rows, n, is_del)

        if table.unknown_member and not any(is_unknown_member(table, r) for r in new_state):
            member: dict[str, Any] = dict.fromkeys(table.columns)
            member.update({c: table.sentinels[c] for c in keys})
            member.update(_is_deleted=False, _execution_id=n)
            if table.strategy == "scd2":
                member["_is_current"] = True
            new_state.append(member)
        return new_state, BatchStats(duplicates_dropped=dups, null_keys_dropped=null_dropped)

    def _scd1(self, table, state, winners, n, is_del) -> None:
        tracked = [c for c in table.columns if c not in table.key and c not in table.ignore]
        ignored = [c for c in table.ignore if c not in table.key]
        for new in winners:
            k = _key(table, new)
            existing = next((r for r in state if _key(table, r) == k), None)
            if (
                existing is not None
                and table.sequence_by
                and _seq(table, new) < _seq(table, existing)
            ):
                continue  # older than the stored row: ignored (sequence_by across batches)
            if is_del(new):
                if existing is None:
                    continue
                if table.delete_mode == "hard":
                    state.remove(existing)
                elif not existing["_is_deleted"]:
                    existing.update(_is_deleted=True, _execution_id=n)
                continue
            if existing is None:
                state.append(_audit(new, n, table))
            elif _differs(existing, new, tracked) or existing["_is_deleted"]:
                for c in table.columns:
                    if c not in table.key:
                        existing[c] = new[c]
                existing.update(_is_deleted=False, _execution_id=n)
            elif _differs(existing, new, ignored):
                for c in ignored:
                    existing[c] = new[c]

    def _scd2(self, table, state, winners, n, is_del) -> None:
        tracked = [c for c in table.columns if c not in table.key and c not in table.ignore]
        ignored = [c for c in table.ignore if c not in table.key]
        for new in winners:
            k = _key(table, new)
            versions = [r for r in state if _key(table, r) == k]
            if table.sequence_by and versions and _seq(table, new) < _seq(table, versions[-1]):
                # sequence_by: a late row becomes an earlier version (shared with the reference)
                _apply_scd2_late(table, state, versions, new, n, tracked)
                continue
            current = next((r for r in state if r["_is_current"] and _key(table, r) == k), None)
            if is_del(new):
                if current is not None:
                    current.update(_is_current=False, _is_deleted=True, _execution_id=n)
                continue
            if current is None:
                state.append(_audit(new, n, table))
            elif _differs(current, new, tracked):
                current.update(_is_current=False, _execution_id=n)
                state.append(_audit(new, n, table))
            elif _differs(current, new, ignored):
                for c in ignored:
                    current[c] = new[c]


def quirk_problems(case: Any, flags: Iterable[str]) -> list[str]:
    """Pure-Python half of "the case is explained by exactly `flags`": the quirk
    model must differ from the reference model on this history (so the real run
    diverges once the table matches the quirk model), and every flag must matter
    (dropping it changes what the quirk model predicts)."""
    from pipetree.testing.merge_harness import model_trajectory

    flags = frozenset(flags)
    full = model_trajectory(case, QuirkModel(flags))
    problems = []
    if full == model_trajectory(case):
        problems.append(f"{sorted(flags)} predict exactly the reference model for this case")
    for f in sorted(flags):
        if model_trajectory(case, QuirkModel(flags - {f})) == full:
            problems.append(f"flag {f} does not change the prediction for this case")
    return problems
