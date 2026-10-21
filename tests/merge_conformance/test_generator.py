"""Pure-Python tests of the conformance case generator (no Spark)."""

from datetime import datetime
from decimal import Decimal

import pytest

from pipetree.testing.merge_gen import COVERAGE_NAMES, Case, coverage, generate_case
from pipetree.testing.merge_model import apply_batch

STRATEGIES = ("replace", "append", "scd1", "scd2")
TYPE_OPTION_NAMES = {
    "sequence_timestamp",
    "sequence_string",
    "sequence_decimal",
    "sequence_double",
    "sequence_in_ignore_columns",
    "surrogate_key_without_unknown_member",
    "composite_sequence_mixed_types",
    "sequence_nan",
}
SNAPSHOT_NAMES = {"snapshot_missing_key", "snapshot_return"}


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
        assert t.strategy in ("scd1", "scd2")  # with or without unknown_member
    assert set(case.column_types) <= set(t.sequence_by)  # only sequence columns are retyped
    assert not set(t.ignore) & set(t.key)
    if "seq" in t.ignore:
        assert t.strategy in ("scd1", "scd2") and "seq" in t.sequence_by
    py_types = {
        "int": int,
        "timestamp": datetime,
        "string": str,
        "decimal(10,2)": Decimal,
        "double": float,
    }
    for c in t.sequence_by:
        py = py_types[case.column_types.get(c, "int")]
        for batch in case.batches:
            for row in batch:
                assert row[c] is None or type(row[c]) is py, (case.repr_line(), c, row[c])
                # NaN only where the case types the column double (the model compares it like Spark)
                assert row[c] is None or row[c] == row[c] or case.column_types.get(c) == "double"
    if t.snapshot:
        assert t.strategy in ("scd1", "scd2")
        assert t.snapshot_max_missing_ratio == 1.0 and t.snapshot_allow_empty
        for batch in case.batches:  # a snapshot batch always carries at least one real key
            assert any(all(r[c] is not None for c in t.key) for r in batch), case.repr_line()
    else:
        assert t.snapshot_max_missing_ratio == 0.5 and not t.snapshot_allow_empty
    assert 2 <= len(case.batches) <= 6
    assert all(0 <= i < len(case.batches) for i in case.inits)
    keys_seen = set()
    for batch in case.batches:
        assert 0 <= len(batch) <= 8
        groups: dict[tuple, list[dict]] = {}
        for row in batch:
            assert set(row) == cols
            assert row["op"] in ("U", "D", None)
            assert row["op"] == "U" or t.has_delete
            k = tuple(row[c] for c in t.key)
            if row["op"] == "U" and all(v is not None for v in k):
                keys_seen.add(k)
            for c in t.key:
                if isinstance(row[c], int):
                    assert row[c] != -1  # never the int unknown-member sentinel
            groups.setdefault(k, []).append(row)
        # Equal sequence (or no sequence_by) for one key -> identical rows only, except
        # that a delete row and a data row may tie (the delete wins).
        for grp in groups.values():
            for i, a in enumerate(grp):
                for b in grp[i + 1 :]:
                    same_seq = all(a[c] == b[c] for c in t.sequence_by)
                    assert not same_seq or a == b or a["op"] != b["op"], (case.repr_line(), a, b)
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
        "sequence_nan",
        "snapshot_missing_key",
        "snapshot_return",
    }


def test_snapshot_cases_are_a_share_of_scd_cases_only():
    for strategy in ("replace", "append"):
        assert not any(generate_case(s, strategy).table.snapshot for s in range(100))
    for strategy in ("scd1", "scd2"):
        cases = [generate_case(s, strategy) for s in range(200)]
        share = sum(c.table.snapshot for c in cases) / len(cases)
        assert 0.2 < share < 0.5, (strategy, share)
        modes = {c.table.delete_mode for c in cases if c.table.snapshot and c.table.has_delete}
        assert "ignore" in modes and "soft" in modes
        assert any(c.table.snapshot and c.table.unknown_member for c in cases)


def test_snapshot_batches_are_subsets_of_one_key_universe_and_vary():
    sizes: set[int] = set()
    for seed in range(100):
        case = generate_case(seed, "scd1")
        t = case.table
        if not t.snapshot:
            continue
        keysets = [
            frozenset(tuple(r[c] for c in t.key) for r in b if all(r[c] is not None for c in t.key))
            for b in case.batches
        ]
        sizes |= {len(k) for k in keysets}
        assert len(frozenset().union(*keysets)) <= 8
    assert len(sizes) > 2


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
        [r(1, 1, seq=5, seen=9), r(2, 2, "z", seq=3), r(3, 3, op="D")],
        [r(1, 1, op="D", seq=6), r(2, 2, "w", seq=1), r(None, None), r(None, 4)],
        [r(1, 1, seq=7)],
        [r(1, 1, seq=7)],
        [],
        [r(4, 4, seq=1, op="D"), r(4, 4, "y", seq=1)],
    ]
    case = Case(
        seed=-1,
        table=t,
        delete_when="op = 'D'",
        surrogate_key=None,
        batches=batches,
        inits=frozenset({5}),
    )
    assert coverage(case) == set(COVERAGE_NAMES) - TYPE_OPTION_NAMES - SNAPSHOT_NAMES

    # The type options are detected too.
    typed = Case(
        seed=-1,
        table=ModelTable(
            strategy="scd1",
            key=("a", "b"),
            columns=cols,
            sequence_by=("seq", "w"),
            ignore=("seen", "seq"),
        ),
        delete_when=None,
        surrogate_key="sid",
        batches=[],
        inits=frozenset(),
        column_types={"seq": "timestamp", "w": "double"},
    )
    assert coverage(typed) == {
        "sequence_timestamp",
        "sequence_double",
        "sequence_in_ignore_columns",
        "surrogate_key_without_unknown_member",
        "composite_sequence_mixed_types",
    }

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


def test_cases_without_a_type_option_keep_integer_sequences():
    plain = 0
    for strategy in STRATEGIES:
        for seed in range(100):
            case = generate_case(seed, strategy)
            if case.column_types:
                continue
            plain += 1
            assert all(type(r["seq"]) in (int, type(None)) for b in case.batches for r in b)
    assert plain > 100


def test_every_typed_sequence_value_is_a_python_value_of_its_column_type():
    seen: set[str] = set()
    for seed in range(100):
        for strategy in ("scd1", "scd2"):
            case = generate_case(seed, strategy)
            seen |= set(case.column_types.values())
    assert seen == {"timestamp", "string", "decimal(10,2)", "double"}


def test_coverage_detects_snapshot_missing_key_and_return():
    from dataclasses import replace

    from pipetree.testing.merge_model import ModelTable

    t = ModelTable(
        strategy="scd1",
        key=("id",),
        columns=("id", "v", "w", "seq", "op"),
        snapshot=True,
        snapshot_max_missing_ratio=1.0,
        snapshot_allow_empty=True,
    )

    def r(i):
        return {"id": i, "v": "x", "w": None, "seq": None, "op": "U"}

    def case(batches, table=t):
        return Case(
            seed=-1,
            table=table,
            delete_when=None,
            surrogate_key=None,
            batches=batches,
            inits=frozenset(),
        )

    only_missing = coverage(case([[r(1), r(2)], [r(2)]]))
    assert {"snapshot_missing_key", "snapshot_return"} & only_missing == {"snapshot_missing_key"}
    both = coverage(case([[r(1), r(2)], [r(2)], [r(1), r(2)]]))
    assert both >= SNAPSHOT_NAMES
    # a table without snapshot, or in ignore mode, never reports them
    assert not SNAPSHOT_NAMES & coverage(
        case([[r(1), r(2)], [r(2)], [r(1), r(2)]], replace(t, snapshot=False))
    )
    ign = replace(t, has_delete=True, delete_mode="ignore")
    assert not SNAPSHOT_NAMES & coverage(case([[r(1), r(2)], [r(2)], [r(1), r(2)]], ign))


def test_an_unknown_strategy_is_rejected():
    with pytest.raises(ValueError, match="unknown strategy"):
        generate_case(0, "merge")
