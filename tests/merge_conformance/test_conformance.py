"""Conformance: pipetree's merge strategies against the reference model.

Every generated (strategy, seed) case runs through the real
`SparkAdapter.run_table` and is compared with `merge_model` after every
batch, plus model-independent invariants. A case listed in
`known_divergences.py` under deviations S must instead be explained by exactly
S: the table matches `QuirkModel(S)` over the whole history, every flag of S
matters and the reference model differs; then the test is reported as xfailed
(a known pipetree bug). Anything else - a listed case that conforms (fixed), or
one that diverges in an unexplained way (a hidden or new deviation) - fails, and
the lists must be regenerated (see `known_divergences.py`).

`PIPETREE_CONFORMANCE_SEEDS` (default 25) sets the number of seeds per
strategy; 0 turns the generated cases off. The lists are generated for seeds
0-299: a diverging seed >= 300 fails as unattributed until the lists are
regenerated with `--seeds N`. The slow tests carry the `conformance` marker
(`-m "not conformance"` skips them).
"""

from __future__ import annotations

import os

import pytest

from pipetree.testing import merge_harness
from pipetree.testing.merge_gen import Case, generate_case
from pipetree.testing.merge_harness import run_case
from pipetree.testing.merge_model import ModelTable, apply_batch
from tests.merge_conformance.known_divergences import (
    DEVIATIONS,
    HAND_REPROS,
    deviation_of,
    deviations_for,
)
from tests.merge_conformance.quirks import QuirkModel, quirk_problems

pytestmark = pytest.mark.spark

STRATEGIES = ("replace", "append", "scd1", "scd2")
ATTRIBUTED_SEEDS = 300  # the seed range the generated lists cover


def _seed_count() -> tuple[int, str | None]:
    raw = os.environ.get("PIPETREE_CONFORMANCE_SEEDS", "25")
    try:
        n = int(raw)
    except ValueError:
        n = -1
    if n < 0:
        return (
            0,
            f"PIPETREE_CONFORMANCE_SEEDS must be a non-negative integer (0 = off), got {raw!r}",
        )
    return n, None


SEEDS, SEEDS_ERROR = _seed_count()


def test_conformance_seed_setting_is_valid():
    if SEEDS_ERROR:
        pytest.fail(SEEDS_ERROR)


def _check(spark, case: Case, deviations: list[str], *, note: str = "") -> None:
    """Conforms (no deviations), or is explained by exactly `deviations` -> xfail."""
    if not deviations:
        divergences = run_case(spark, case)
        assert divergences == [], note + "\n\n".join(str(d) for d in divergences)
        return
    problems = quirk_problems(case, deviations)
    divergences = run_case(spark, case, model=QuirkModel(deviations), attribution=True)
    if problems or divergences:
        pytest.fail(
            f"listed under {deviations} but NOT explained by QuirkModel({deviations}) - fixed, "
            "or a hidden/new deviation; regenerate the lists (see known_divergences.py):\n"
            + "\n".join(problems)
            + "\n\n"
            + "\n\n".join(str(d) for d in divergences)
        )
    pytest.xfail("; ".join(f"{f}: {DEVIATIONS[f]}" for f in deviations))


def _params():
    for strategy in STRATEGIES:
        for seed in range(SEEDS):
            ids = deviations_for(strategy, seed)
            label = f"{strategy}-{seed}" + (f"-{'+'.join(ids)}" if ids else "")
            yield pytest.param(strategy, seed, id=label)


@pytest.mark.conformance
@pytest.mark.parametrize(("strategy", "seed"), list(_params()))
def test_generated_case_conforms(spark, strategy, seed):
    note = ""
    if seed >= ATTRIBUTED_SEEDS:
        note = (
            f"seed {seed} is outside the attributed range 0-{ATTRIBUTED_SEEDS - 1}: run "
            f"`python -m tests.merge_conformance.regen --seeds {seed + 1}` to attribute it\n"
        )
    _check(spark, generate_case(seed, strategy), deviations_for(strategy, seed), note=note)


@pytest.mark.conformance
@pytest.mark.parametrize("repro", list(HAND_REPROS))
def test_deviation_minimal_repro(spark, repro):
    """The hand-minimised reproduction of every deviation is explained by its flag."""
    _check(spark, HAND_REPROS[repro], [deviation_of(repro)])


# ------------------------------------------------------- hand-built smoke cases

COLS = ("id", "v", "w", "seq", "op")


def _row(id, v="a", *, w=None, seq=None, op="U", **extra):
    return {"id": id, "v": v, "w": w, "seq": seq, "op": op, **extra}


def _case(strategy, batches, *, inits=(), extra=(), **kw):
    has_delete = kw.get("has_delete", False)
    kw.setdefault("sequence_by", ("seq",))
    table = ModelTable(strategy=strategy, key=("id",), columns=COLS + extra, **kw)
    return Case(
        seed=-1,
        table=table,
        delete_when="op = 'D'" if has_delete else None,
        surrogate_key=None,
        batches=batches,
        inits=frozenset(inits),
    )


# name -> (case, deviations that explain it today)
SMOKE: dict[str, tuple[Case, list[str]]] = {
    "replace": (_case("replace", [[_row(1), _row(2)], [_row(3, "c")], [_row(3, "c")]]), []),
    "append": (_case("append", [[_row(1)], [_row(1), _row(2, "b")], [_row(3)]], inits=[2]), []),
    # insert, update, untouched, soft delete
    "scd1": (
        _case(
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
        [],
    ),
    # ... and the deleted key returns (a returning soft-deleted key is active again): today's
    # revive-after-delete
    "scd1-resurrect": (
        _case(
            "scd1",
            [
                [_row(1, "a", seq=1), _row(2, "b", seq=1)],
                [_row(1, "x", seq=2), _row(2, "b", seq=1)],
                [_row(2, "b", seq=3, op="D"), _row(3, "c", seq=3)],
                [_row(2, "B", seq=4)],
            ],
            has_delete=True,
        ),
        ["revive-after-delete"],
    ),
    # change, delete, return (a returning scd2 key opens a new version: a new current version)
    "scd2": (
        _case(
            "scd2",
            [
                [_row(1, "a", seq=1), _row(2, "b", seq=1)],
                [_row(1, "x", seq=2)],
                [_row(1, "x", seq=3, op="D")],
                [_row(1, "y", seq=4), _row(2, "b", seq=1)],
            ],
            has_delete=True,
        ),
        [],
    ),
}


@pytest.mark.parametrize("name", list(SMOKE))
def test_hand_built_smoke_case(spark, name):
    case, deviations = SMOKE[name]
    _check(spark, case, deviations)


# ---------------------------------------------------------- harness behaviour


def test_harness_is_live_a_wrong_model_is_reported(spark, monkeypatch):
    """If the oracle says something else, the harness must say so (no vacuous pass)."""

    def wrong_apply_batch(table, state, batch, execution_id, *, init=False):
        new_state, stats = apply_batch(table, state, batch, execution_id, init=init)
        for row in new_state:
            if row.get("v") is not None:
                row["v"] = row["v"] + "-WRONG"
        return new_state, stats

    monkeypatch.setattr(merge_harness, "apply_batch", wrong_apply_batch)
    case = SMOKE["scd1"][0]
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
    divergences = run_case(spark, SMOKE["scd2"][0])
    assert [d.kind for d in divergences] == ["stats"]
    assert divergences[0].batch_index == 0


def test_attribution_mode_reports_independent_later_divergences(spark):
    # ignored-column-stale at batch 1 (ignored column alone), ignore-mode-delete-rows at batch 2
    # (ignore-mode delete row).
    case = _case(
        "scd1",
        [[_row(1, seen=0)], [_row(1, seen=1)], [_row(2, op="D", seen=0)]],
        extra=("seen",),
        ignore=("seen",),
        has_delete=True,
        delete_mode="ignore",
        sequence_by=(),
    )
    first_only = run_case(spark, case)
    assert {d.batch_index for d in first_only} == {1}

    every = run_case(spark, case, attribution=True)
    assert [(d.batch_index, d.kind) for d in every] == [(1, "rows"), (2, "rows")]
    assert "'seen': 1" in every[0].detail  # model: the ignored column is updated
    assert "'op': 'D'" in every[1].detail  # table: the delete row was written

    assert (
        run_case(
            spark,
            case,
            model=QuirkModel({"ignore-mode-delete-rows", "ignored-column-stale"}),
            attribution=True,
        )
        == []
    )
    assert run_case(spark, case, model=QuirkModel({"ignored-column-stale"}), attribution=True) != []
