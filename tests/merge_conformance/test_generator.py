"""Pure-Python tests of the conformance case generator (no Spark)."""

import pytest

from pipetree.testing.merge_gen import COVERAGE_NAMES, Case, coverage, generate_case
from pipetree.testing.merge_model import apply_batch

STRATEGIES = ("replace", "append", "scd1", "scd2")


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_generation_is_deterministic(strategy):
    for seed in range(20):
        assert generate_case(seed, strategy) == generate_case(seed, strategy)


def test_different_seeds_give_different_cases():
    assert len({generate_case(s, "scd1").repr_line() for s in range(30)}) > 20


def _check_valid(case: Case, strategy: str) -> None:
    t = case.table
    assert t.strategy == strategy
    cols = set(t.columns)
    assert set(t.key) <= cols
    assert set(t.sequence_by) <= cols and not set(t.sequence_by) & set(t.key)
    assert set(t.ignore) <= cols and not set(t.ignore) & set(t.key)
    assert not (t.strategy == "scd2" and t.delete_mode == "hard")
    if not t.has_delete:
        assert t.delete_mode == "soft"  # the default: delete_mode only set with a delete
        assert case.delete_when is None
    else:
        assert t.strategy in ("scd1", "scd2")
        assert case.delete_when == "op = 'D'"
    if case.surrogate_key is not None:
        assert t.strategy in ("scd1", "scd2") and t.unknown_member
    assert 2 <= len(case.batches) <= 6
    assert all(0 <= i < len(case.batches) for i in case.inits)
    keys_seen = set()
    for batch in case.batches:
        assert 0 <= len(batch) <= 8
        groups: dict[tuple, list[dict]] = {}
        for row in batch:
            assert set(row) == cols
            assert row["op"] in ("U", "D")
            assert row["op"] == "U" or t.has_delete
            k = tuple(row[c] for c in t.key)
            if row["op"] == "U" and all(v is not None for v in k):
                keys_seen.add(k)
            for c in t.key:
                if isinstance(row[c], int):
                    assert row[c] != -1  # never the int unknown-member sentinel
            groups.setdefault(k, []).append(row)
        # Equal sequence (or no sequence_by) for one key -> identical rows only.
        for grp in groups.values():
            for i, a in enumerate(grp):
                for b in grp[i + 1 :]:
                    same_seq = all(a[c] == b[c] for c in t.sequence_by)
                    assert not same_seq or a == b, (case.repr_line(), a, b)
    assert len(keys_seen) <= 8  # upserts draw from a key space of 3-8 keys


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_generated_cases_are_valid(strategy):
    for seed in range(200):
        _check_valid(generate_case(seed, strategy), strategy)


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_the_model_accepts_every_generated_case(strategy):
    # No AmbiguousDedupe, no sentinel key: the generator honours the model's contract.
    for seed in range(200):
        case = generate_case(seed, strategy)
        state: list[dict] = []
        for i, batch in enumerate(case.batches):
            state, _ = apply_batch(case.table, state, batch, i + 1, init=i in case.inits)


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_coverage_reaches_every_interesting_case(strategy):
    reached: set[str] = set()
    for seed in range(200):
        reached |= coverage(generate_case(seed, strategy))
    assert reached == set(COVERAGE_NAMES), sorted(set(COVERAGE_NAMES) - reached)


def test_coverage_names_are_the_documented_ones():
    assert set(COVERAGE_NAMES) == {
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
    }


def test_coverage_detects_each_property_on_hand_built_cases():
    from pipetree.testing.merge_model import ModelTable

    cols = ("a", "b", "v", "w", "seq", "op", "seen")
    t = ModelTable(
        strategy="scd1",
        key=("a", "b"),
        columns=cols,
        sequence_by=("seq",),
        ignore=("seen",),
        has_delete=True,
    )

    def r(a, b, v="x", seq=1, op="U", seen=0):
        return {"a": a, "b": b, "v": v, "w": None, "seq": seq, "op": op, "seen": seen}

    batches = [
        [r(1, 1, seq=5), r(1, 1, seq=5), r(2, 2, seq=1), r(2, 2, "y", seq=2)],
        [r(1, 1, seq=5, seen=9), r(2, 2, "z", seq=1), r(3, 3, op="D")],
        [r(1, 1, op="D", seq=6), r(None, None), r(None, 4)],
        [r(1, 1, seq=7)],
        [r(1, 1, seq=7)],
        [],
    ]
    case = Case(
        seed=-1,
        table=t,
        delete_when="op = 'D'",
        surrogate_key=None,
        batches=batches,
        inits=frozenset({5}),
    )
    assert coverage(case) == set(COVERAGE_NAMES)

    # A case without any of it covers nothing.
    plain = Case(
        seed=-1,
        table=t,
        delete_when="op = 'D'",
        surrogate_key=None,
        batches=[[r(1, 1)], [r(2, 2, seq=2)]],
        inits=frozenset(),
    )
    assert coverage(plain) == set()
