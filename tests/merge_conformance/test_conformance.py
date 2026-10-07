"""Conformance: pipetree's merge strategies against the reference model.

Every generated (strategy, seed) case runs through the real
`SparkAdapter.run_table` and is compared with `merge_model` after every
batch, plus model-independent invariants. Known divergences (deviations, see
`known_divergences.py`) are `xfail(strict=True)`: a fixed bug turns its xfail
into a failure, forcing the entry's removal.

`PIPETREE_CONFORMANCE_SEEDS` (default 25) sets the number of seeds per strategy.
"""

from __future__ import annotations

import os

import pytest

from pipetree.testing import merge_harness
from pipetree.testing.merge_gen import Case, generate_case
from pipetree.testing.merge_harness import run_case
from pipetree.testing.merge_model import ModelTable, apply_batch
from tests.merge_conformance.known_divergences import (
    HAND_REPROS,
    KNOWN_DIVERGENCES,
    deviations_for,
)

pytestmark = pytest.mark.spark

STRATEGIES = ("replace", "append", "scd1", "scd2")
SEEDS = int(os.environ.get("PIPETREE_CONFORMANCE_SEEDS", "25"))


def _params():
    for strategy in STRATEGIES:
        for seed in range(SEEDS):
            ids = deviations_for(strategy, seed)
            marks = []
            if ids:
                reason = "; ".join(f"{i}: {KNOWN_DIVERGENCES[i][0]}" for i in ids)
                marks.append(pytest.mark.xfail(strict=True, raises=AssertionError, reason=reason))
            yield pytest.param(strategy, seed, id=f"{strategy}-{seed}", marks=marks)


@pytest.mark.parametrize(("strategy", "seed"), list(_params()))
def test_generated_case_conforms(spark, strategy, seed):
    divergences = run_case(spark, generate_case(seed, strategy))
    assert divergences == [], "\n\n".join(str(d) for d in divergences)


# ------------------------------------------------------- hand-built smoke cases

COLS = ("id", "v", "w", "seq", "op")


def _row(id, v="a", *, w=None, seq=None, op="U"):
    return {"id": id, "v": v, "w": w, "seq": seq, "op": op}


def _case(strategy, batches, *, delete_mode="soft", has_delete=False, inits=()):
    table = ModelTable(
        strategy=strategy,
        key=("id",),
        columns=COLS,
        sequence_by=("seq",),
        delete_mode=delete_mode,
        has_delete=has_delete,
    )
    return Case(
        seed=-1,
        table=table,
        delete_when="op = 'D'" if has_delete else None,
        surrogate_key=None,
        batches=batches,
        inits=frozenset(inits),
    )


SMOKE = {
    "replace": _case("replace", [[_row(1), _row(2)], [_row(3, "c")], [_row(3, "c")]]),
    "append": _case("append", [[_row(1)], [_row(1), _row(2, "b")], [_row(3)]], inits=[2]),
    # insert, update, untouched, soft delete (the return of a deleted key is a deviation)
    "scd1": _case(
        "scd1",
        [
            [_row(1, "a", seq=1), _row(2, "b", seq=1)],
            [_row(1, "x", seq=2), _row(2, "b", seq=1)],
            [_row(2, "b", seq=3, op="D"), _row(3, "c", seq=3)],
            [
                _row(1, "x", seq=2),
                _row(3, "c", seq=3),
            ],  # untouched (re-run safe, see redelete-bump)
        ],
        has_delete=True,
    ),
    # change, delete, return (a returning scd2 key opens a new version: a new current version)
    "scd2": _case(
        "scd2",
        [
            [_row(1, "a", seq=1), _row(2, "b", seq=1)],
            [_row(1, "x", seq=2)],
            [_row(1, "x", seq=3, op="D")],
            [_row(1, "y", seq=4), _row(2, "b", seq=1)],
        ],
        has_delete=True,
    ),
}


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_hand_built_smoke_case_conforms(spark, strategy):
    divergences = run_case(spark, SMOKE[strategy])
    assert divergences == [], "\n\n".join(str(d) for d in divergences)


@pytest.mark.parametrize(
    "deviation",
    [
        pytest.param(
            f,
            marks=pytest.mark.xfail(
                strict=True,
                raises=AssertionError,
                reason=f"{f}: {KNOWN_DIVERGENCES[f.split('-')[0]][0]}",
            ),
            id=f,
        )
        for f in HAND_REPROS
    ],
)
def test_deviation_minimal_repro(spark, deviation):
    """The hand-minimised reproduction of every deviation still diverges."""
    divergences = run_case(spark, HAND_REPROS[deviation])
    assert divergences == [], "\n\n".join(str(d) for d in divergences)


def test_harness_is_live_a_wrong_model_is_reported(spark, monkeypatch):
    """If the oracle says something else, the harness must say so (no vacuous pass)."""

    def wrong_apply_batch(table, state, batch, execution_id, *, init=False):
        new_state, stats = apply_batch(table, state, batch, execution_id, init=init)
        for row in new_state:
            if row.get("v") is not None:
                row["v"] = row["v"] + "-WRONG"
        return new_state, stats

    monkeypatch.setattr(merge_harness, "apply_batch", wrong_apply_batch)
    case = SMOKE["scd1"]
    divergences = run_case(spark, case)

    assert len(divergences) >= 1
    first = divergences[0]
    assert first.kind == "rows"
    assert first.batch_index == 0
    text = str(first)
    assert case.repr_line() in text
    assert "-WRONG" in text  # the model rows are shown
    assert "model" in text and "table" in text


def test_harness_reports_a_wrong_stat(spark, monkeypatch):
    def wrong_stats(table, state, batch, execution_id, *, init=False):
        new_state, stats = apply_batch(table, state, batch, execution_id, init=init)
        stats.null_keys_dropped += 7
        return new_state, stats

    monkeypatch.setattr(merge_harness, "apply_batch", wrong_stats)
    divergences = run_case(spark, SMOKE["scd2"])
    assert [d.kind for d in divergences] == ["stats"]
    assert divergences[0].batch_index == 0
