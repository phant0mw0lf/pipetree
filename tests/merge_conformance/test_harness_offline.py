"""No-Spark tests of the harness pieces: invariants on crafted snapshots, the
offline comparison of a recording (attribution mode, resync) and the quirk model."""

import pytest

from pipetree.testing.merge_gen import STRATEGIES, Case, generate_case
from pipetree.testing.merge_harness import (
    INVARIANTS,
    Recording,
    check_invariants,
    compare_recording,
    model_trajectory,
)
from pipetree.testing.merge_model import ModelTable
from tests.merge_conformance.known_divergences import HAND_REPROS, deviation_of
from tests.merge_conformance.quirks import FLAGS, QuirkModel, quirk_problems

COLS = ("id", "v", "w", "seq", "op")


def _case(strategy, *, surrogate_key=None, inits=(), n_batches=3, **kw):
    t = ModelTable(strategy=strategy, key=("id",), columns=COLS, **kw)
    return Case(-1, t, None, surrogate_key, [[] for _ in range(n_batches)], frozenset(inits))


def V(id, vf, vt, *, cur, deleted=False, v="a", ins=None, upd=None, sid=None):
    """A raw scd2 table row (timestamps are plain ints here)."""
    row = {"id": id, "v": v, "w": None, "seq": None, "op": "U"}
    row.update(
        _inserted_at=ins if ins is not None else vf,
        _updated_at=upd if upd is not None else (vt or vf),
        _is_deleted=deleted,
        _execution_id=1,
        _valid_from=vf,
        _valid_to=vt,
        _is_current=cur,
    )
    if sid is not None:
        row["sid"] = sid
    return row


def names(problems):
    return {name for name, _ in problems}


def test_a_consistent_scd2_history_has_no_problem():
    raw = [
        V(1, 10, 20, cur=False),
        V(1, 20, 30, cur=False, deleted=True),
        # returned after a delete: a gap is fine (a returning scd2 key opens a new version)
        V(1, 40, None, cur=True),
    ]
    assert check_invariants(_case("scd2"), 1, None, raw, {}) == []


def test_overlapping_versions_are_reported():
    raw = [V(1, 10, 25, cur=False), V(1, 20, None, cur=True)]
    assert "scd2_overlap" in names(check_invariants(_case("scd2"), 0, None, raw, {}))
    raw = [V(1, 10, 25, cur=False, deleted=True), V(1, 20, None, cur=True)]
    assert "scd2_overlap" in names(check_invariants(_case("scd2"), 0, None, raw, {}))


def test_a_gap_after_a_change_is_reported():
    raw = [V(1, 10, 20, cur=False), V(1, 30, None, cur=True)]
    assert names(check_invariants(_case("scd2"), 0, None, raw, {})) == {"scd2_gap"}


def test_two_current_versions_are_reported():
    raw = [V(1, 10, None, cur=True), V(1, 20, None, cur=True)]
    got = names(check_invariants(_case("scd2"), 0, None, raw, {}))
    assert {"scd2_one_current", "scd2_current_not_latest"} <= got


def test_a_changed_closed_version_is_reported():
    prev = [V(1, 10, 20, cur=False), V(1, 20, None, cur=True)]
    now = [V(1, 10, 20, cur=False, v="changed"), V(1, 20, None, cur=True)]
    got = names(check_invariants(_case("scd2"), 1, prev, now, {}))
    assert got == {"scd2_closed_changed"}
    # ... except on an init batch, which rebuilds the table
    assert check_invariants(_case("scd2", inits=[1]), 1, prev, now, {}) == []


def test_closed_without_delete_and_valid_to_rules():
    raw = [V(1, 10, 20, cur=False)]
    assert names(check_invariants(_case("scd2"), 0, None, raw, {})) == {"scd2_latest_closed"}
    raw = [V(1, 10, 20, cur=True)]
    assert "scd2_valid_to_iff_current" in names(check_invariants(_case("scd2"), 0, None, raw, {}))


def test_scd1_inserted_at_and_duplicate_keys():
    def R(id, ins, upd=None):
        row = {"id": id, "v": "a", "w": None, "seq": None, "op": "U"}
        row.update(_inserted_at=ins, _updated_at=upd or ins, _is_deleted=False, _execution_id=1)
        return row

    case = _case("scd1")
    assert names(check_invariants(case, 1, [R(1, 10)], [R(1, 11, 12)], {})) == {
        "inserted_at_changed"
    }
    assert names(check_invariants(case, 0, None, [R(1, 10), R(1, 11)], {})) == {"identity_unique"}
    assert names(check_invariants(case, 0, None, [R(1, 10, 9)], {})) == {"updated_before_inserted"}


def test_a_surrogate_key_moving_to_another_row_is_reported_even_on_init():
    case = _case("scd2", surrogate_key="sid", inits=[1])
    owner: dict = {}
    first = [V(1, 10, None, cur=True, sid=1), V(2, 10, None, cur=True, sid=2)]
    assert check_invariants(case, 0, None, first, owner) == []
    # init rebuilds the table: new versions must get NEW sids, not reuse 1 and 2
    reused = [V(1, 50, None, cur=True, sid=2), V(2, 50, None, cur=True, sid=3)]
    assert "sid_reused" in names(check_invariants(case, 1, first, reused, owner))


def test_sid_stability_and_uniqueness():
    case = _case("scd2", surrogate_key="sid")
    prev = [V(1, 10, None, cur=True, sid=1)]
    now = [V(1, 10, None, cur=True, sid=5)]
    assert names(check_invariants(case, 1, prev, now, {1: ((1,), 10)})) == {"sid_changed"}
    dup = [V(1, 10, None, cur=True, sid=1), V(2, 10, None, cur=True, sid=1)]
    assert "sid_null_or_duplicate" in names(check_invariants(case, 0, None, dup, {}))


def test_every_reported_invariant_name_is_declared():
    # the names above are a subset of the declared list the quirk model refers to
    assert {"scd2_overlap", "scd2_gap", "sid_reused", "idempotence"} <= set(INVARIANTS)


# ------------------------------------------------- offline comparison, resync


def _snap(rows):
    out = []
    for id_, v, seen, e in rows:
        out.append(
            {"id": id_, "v": v, "w": None, "seq": None, "op": "U", "seen": seen}
            | {"_inserted_at": 1, "_updated_at": 1, "_is_deleted": False, "_execution_id": e}
        )
    return out


def test_attribution_mode_resyncs_and_keeps_comparing():
    t = ModelTable(strategy="scd1", key=("id",), columns=(*COLS, "seen"), ignore=("seen",))

    def r(id_, v="a", seen=0):
        return {"id": id_, "v": v, "w": None, "seq": None, "op": "U", "seen": seen}

    case = Case(-1, t, None, None, [[r(1)], [r(1, seen=1)], [r(2, "b")]], frozenset())
    # A fabricated table that (a) loses the ignored change in batch 1 and (b) also
    # writes v="X" for key 2 in batch 2: two independent divergences.
    rec = Recording(
        snapshots=[
            _snap([(1, "a", 0, 1)]),
            _snap([(1, "a", 0, 1)]),
            _snap([(1, "a", 0, 1), (2, "X", 0, 3)]),
        ],
        results=[{"duplicates_dropped": 0}] * 3,
        rerun=_snap([(1, "a", 0, 1), (2, "X", 0, 3)]),
    )
    first = compare_recording(case, rec)
    assert [d.batch_index for d in first] == [1]
    every = compare_recording(case, rec, attribution=True)
    # without the resync, batch 2 would also report the old `seen` difference
    assert [(d.batch_index, d.kind, d.rerun) for d in every] == [
        (1, "rows", False),
        (2, "rows", False),
        (2, "rows", True),  # the re-run compares against the model too
    ]
    only_model = every[1].detail.split("only in model:")[1].split("only in table:")[0]
    assert "'id': 2" in only_model and "'id': 1" not in only_model


# ---------------------------------------------------------------- quirk model


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_quirk_model_without_flags_is_the_reference_model(strategy):
    for seed in range(100):
        case = generate_case(seed, strategy)
        assert model_trajectory(case, QuirkModel()) == model_trajectory(case), seed


@pytest.mark.parametrize("repro", list(HAND_REPROS))
def test_each_deviation_flag_explains_its_repro_and_matters(repro):
    assert quirk_problems(HAND_REPROS[repro], [deviation_of(repro)]) == []


def test_every_flag_is_a_deviation_and_unknown_flags_are_rejected():
    from tests.merge_conformance.known_divergences import DEVIATIONS

    assert set(FLAGS) == set(DEVIATIONS)
    with pytest.raises(ValueError, match="unknown quirk"):
        QuirkModel({"F99"})


def test_scd2_version_order_is_total_for_late_versions_of_equal_sequence():
    # two late versions of one key: equal sequence and equal _valid_from (both sit at the next
    # later version's _valid_from); the later batch (_execution_id) is the later version
    from pipetree.testing.merge_harness import _normalise_raw

    t = ModelTable(strategy="scd2", key=("id",), columns=COLS, sequence_by=("seq",))

    def row(v, seq, e, ins, vf, vt, cur):
        r = V(1, vf, vt, cur=cur, v=v, ins=ins)
        r.update(seq=seq, _execution_id=e)
        return r

    a = row("a", 1, 3, 30, 100, 100, False)
    b = row("b", 1, 5, 50, 100, 100, False)
    c = row("c", 2, 2, 20, 100, None, True)
    for raw in ([c, b, a], [b, c, a], [a, c, b], [b, a, c]):
        assert [r["v"] for r in _normalise_raw(list(raw), t)] == ["a", "b", "c"]
