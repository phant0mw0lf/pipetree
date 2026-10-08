"""`QuirkModel`: the reference model with one switch per KNOWN pipetree deviation
(the deviations in `known_divergences.py`), used to ATTRIBUTE divergences.

`run_case(spark, case, model=QuirkModel({"null-key-kept", "ignore-mode-delete-rows"}))` compares the
real
table with "the model as if today's quirks null-key-kept and ignore-mode-delete-rows were the
rules". A seed
listed under deviations S must match `QuirkModel(S)` over its whole history, and
every flag in S must matter; otherwise the divergence is unexplained.

This is deliberately a transcription of today's pipetree behaviour, one flag
per deviation, each citing the code that causes it (src/pipetree/adapters/spark,
at commit 911c072). It lives in tests/, never in the oracle: when a deviation is
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
    _audit,
    _differs,
    _is_delete,
    _key,
    _seq,
    is_unknown_member,
)

FLAGS: dict[str, str] = {
    # merge.py:191 `whenMatchedUpdate(... AND ({change_cond}), set=update_values)` and
    # audit.py:29 `update_audit_values` (no `_is_deleted`): a soft-deleted row that
    # returns is updated only on a tracked change, and stays `_is_deleted = true`.
    "revive-after-delete": "scd1 soft-deleted key returning stays deleted",
    # merge.py:355 `drop_null_business_keys` only acts on unknown_member tables, and
    # merge.py:558 `_key_condition` matches with `=`, so a whole-NULL key on an
    # scd1/scd2 table is kept and never matches: inserted again on every run.
    "null-key-kept": "NULL key on scd1/scd2 without unknown_member kept and re-inserted",
    # merge.py:558 `_key_condition`: `target.k = source.k` is not null-safe, so a
    # composite key with one NULL component never matches.
    "composite-null-key": "composite key with a NULL component never matches",
    # merge.py:552 `_prepare_source`: delete_mode ignore marks no row as a delete, and
    # merge.py:152/214 `dedupe_for_merge` runs before, so delete rows are upserts.
    "ignore-mode-delete-rows": "delete_mode ignore treats delete rows as upserts",
    # merge.py:565 `_comparable_columns` drops ignore_columns from change detection
    # and nothing else writes them: a change in ignored columns alone is lost.
    "ignored-column-stale": "ignore_columns change alone not written",
    # merge.py:184 `whenMatchedUpdate(condition="source.is_delete = true", ...)`
    # without a target `_is_deleted` check: re-deleting bumps the audit columns.
    "redelete-bump": "scd1 re-delete of a soft-deleted row bumps _execution_id (re-run = no-op)",
}

# Model-independent invariants a quirk breaks (see merge_harness.INVARIANTS).
_TOLERATES: dict[str, frozenset[str]] = {
    # the NULL key piles up: several rows / current versions of one key, and a
    # re-run inserts it once more
    "null-key-kept": frozenset(
        {"identity_unique", "scd2_one_current", "scd2_current_not_latest", "idempotence"}
    ),
    "composite-null-key": frozenset(
        {"identity_unique", "scd2_one_current", "scd2_current_not_latest", "idempotence"}
    ),
    "redelete-bump": frozenset({"idempotence"}),
}


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
        q = self.flags
        n = execution_id
        new_state = [] if init else [dict(r) for r in state]
        rows = [dict(r) for r in batch]
        keys = table.key
        scd = table.strategy in ("scd1", "scd2")
        if table.unknown_member:
            kept = [r for r in rows if all(r[c] is not None for c in keys)]
        elif not scd or "null-key-kept" in q:
            kept = rows
        elif len(keys) == 1:
            kept = [r for r in rows if r[keys[0]] is not None]
        else:
            kept = [r for r in rows if any(r[c] is not None for c in keys)]
        null_dropped = len(rows) - len(kept)
        rows = kept

        ignore_mode = table.has_delete and table.delete_mode == "ignore"

        def is_del(r: dict) -> bool:
            return _is_delete(table, r) and not (ignore_mode and "ignore-mode-delete-rows" in q)

        dups = None
        if scd:
            if ignore_mode and "ignore-mode-delete-rows" not in q:
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

    def _never_matches(self, k: tuple) -> bool:
        nulls = sum(v is None for v in k)
        if nulls == len(k):
            return "null-key-kept" in self.flags
        return nulls > 0 and "composite-null-key" in self.flags

    def _scd1(self, table, state, winners, n, is_del) -> None:
        q = self.flags
        tracked = [c for c in table.columns if c not in table.key and c not in table.ignore]
        ignored = [c for c in table.ignore if c not in table.key]
        for new in winners:
            k = _key(table, new)
            existing = None
            if not self._never_matches(k):
                existing = next((r for r in state if _key(table, r) == k), None)
            if is_del(new):
                if existing is None:
                    continue
                if table.delete_mode == "hard":
                    state.remove(existing)
                elif not existing["_is_deleted"] or "redelete-bump" in q:
                    existing.update(_is_deleted=True, _execution_id=n)
                continue
            if existing is None:
                state.append(_audit(new, n, table))
            elif "revive-after-delete" in q and existing["_is_deleted"]:
                if _differs(existing, new, tracked):
                    for c in table.columns:
                        if c not in table.key:
                            existing[c] = new[c]
                    existing["_execution_id"] = n  # _is_deleted stays True
                elif _differs(existing, new, ignored) and "ignored-column-stale" not in q:
                    for c in ignored:
                        existing[c] = new[c]
            elif _differs(existing, new, tracked) or existing["_is_deleted"]:
                for c in table.columns:
                    if c not in table.key:
                        existing[c] = new[c]
                existing.update(_is_deleted=False, _execution_id=n)
            elif _differs(existing, new, ignored) and "ignored-column-stale" not in q:
                for c in ignored:
                    existing[c] = new[c]

    def _scd2(self, table, state, winners, n, is_del) -> None:
        tracked = [c for c in table.columns if c not in table.key and c not in table.ignore]
        ignored = [c for c in table.ignore if c not in table.key]
        for new in winners:
            k = _key(table, new)
            current = None
            if not self._never_matches(k):
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
            elif _differs(current, new, ignored) and "ignored-column-stale" not in self.flags:
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
