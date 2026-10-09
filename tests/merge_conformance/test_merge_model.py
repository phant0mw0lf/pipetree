"""Pure-Python tests of the merge reference model (no Spark)."""

import copy

import pytest

from pipetree.testing.merge_model import (
    AmbiguousDedupe,
    BatchStats,
    ModelTable,
    apply_batch,
    is_unknown_member,
)

COLS = ("id", "v", "w", "seq", "op")


def T(strategy, **kw):
    kw.setdefault("key", ("id",))
    kw.setdefault("columns", COLS)
    return ModelTable(strategy=strategy, **kw)


def B(*rows):
    out = []
    for r in rows:
        row = dict.fromkeys(COLS)
        row["op"] = "U"
        row.update(r)
        out.append(row)
    return out


def R(id, v=None, *, w=None, seq=None, op: str | None = "U", e=1, d=False, cur=None):
    row = {"id": id, "v": v, "w": w, "seq": seq, "op": op, "_is_deleted": d, "_execution_id": e}
    if cur is not None:
        row["_is_current"] = cur
    return row


def run(table, *batches) -> tuple[list[dict], BatchStats]:
    """Apply batches 1..n in order; return (state, stats of the last batch)."""
    state: list[dict] = []
    stats: BatchStats | None = None
    for n, batch in enumerate(batches, start=1):
        state, stats = apply_batch(table, state, batch, n)
    assert stats is not None, "run() needs at least one batch"
    return state, stats


def test_replace_is_the_last_batch():
    t = T("replace")
    state, stats = run(t, B({"id": 1, "v": "a"}, {"id": 2, "v": "b"}), B({"id": 3, "v": "c"}))
    assert state == [R(3, "c", e=2)]
    assert stats.duplicates_dropped is None
    assert stats.null_keys_dropped == 0


def test_append_accumulates_and_init_resets():
    t = T("append")
    state, stats = run(t, B({"id": 1, "v": "a"}), B({"id": 1, "v": "a"}, {"id": 2, "v": "b"}))
    assert state == [R(1, "a", e=1), R(1, "a", e=2), R(2, "b", e=2)]
    assert stats.duplicates_dropped is None
    state, _ = apply_batch(t, state, B({"id": 9, "v": "z"}), 3, init=True)
    assert state == [R(9, "z", e=3)]


def test_scd1_insert_update_untouched():
    t = T("scd1")
    s1, _ = run(t, B({"id": 1, "v": "a"}))
    assert s1 == [R(1, "a", e=1)]
    s2, _ = run(t, B({"id": 1, "v": "a"}), B({"id": 1, "v": "b"}))
    assert s2 == [R(1, "b", e=2)]
    s3, _ = run(t, B({"id": 1, "v": "a"}), B({"id": 1, "v": "b"}), B({"id": 1, "v": "b"}))
    assert s3 == [R(1, "b", e=2)]


def test_scd1_dedupe_picks_highest_sequence_and_counts():
    t = T("scd1", sequence_by=("seq",))
    state, stats = run(t, B({"id": 1, "v": "old", "seq": 1}, {"id": 1, "v": "new", "seq": 2}))
    assert state == [R(1, "new", seq=2, e=1)]
    assert stats.duplicates_dropped == 1
    state, stats = run(t, B({"id": 1, "v": "none", "seq": None}, {"id": 1, "v": "one", "seq": 1}))
    assert state == [R(1, "one", seq=1, e=1)]
    assert stats.duplicates_dropped == 1
    state, stats = run(
        t,
        B(
            {"id": 1, "v": "a", "seq": 2},
            {"id": 1, "v": "b", "seq": 3},
            {"id": 1, "v": "c", "seq": 1},
            {"id": 2, "v": "x", "seq": 1},
        ),
    )
    assert state == [R(1, "b", seq=3, e=1), R(2, "x", seq=1, e=1)]
    assert stats.duplicates_dropped == 2


def test_dedupe_without_sequence_by_takes_the_first_of_identical_rows():
    t = T("scd1")
    state, stats = run(t, B({"id": 1, "v": "a"}, {"id": 1, "v": "a"}, {"id": 2, "v": "c"}))
    assert state == [R(1, "a", e=1), R(2, "c", e=1)]
    assert stats.duplicates_dropped == 1


def test_ambiguous_ties_raise():
    t = T("scd1", sequence_by=("seq",))
    # identical tied rows are fine
    state, stats = run(t, B({"id": 1, "v": "a", "seq": 1}, {"id": 1, "v": "a", "seq": 1}))
    assert state == [R(1, "a", seq=1, e=1)]
    assert stats.duplicates_dropped == 1
    # tied rows differing in a non-key column raise
    with pytest.raises(AmbiguousDedupe):
        run(t, B({"id": 1, "v": "a", "seq": 1}, {"id": 1, "v": "b", "seq": 1}))
    # a lower, differing row is not part of the tie
    state, _ = run(
        t,
        B(
            {"id": 1, "v": "a", "seq": 2},
            {"id": 1, "v": "a", "seq": 2},
            {"id": 1, "v": "b", "seq": 1},
        ),
    )
    assert state == [R(1, "a", seq=2, e=1)]
    # different sequence values never raise
    run(t, B({"id": 1, "v": "a", "seq": 1}, {"id": 1, "v": "b", "seq": 2}))
    # without sequence_by, differing duplicates raise
    with pytest.raises(AmbiguousDedupe):
        run(T("scd2"), B({"id": 1, "v": "a"}, {"id": 1, "v": "b"}))


def test_multi_column_sequence_by():
    t = T("scd1", sequence_by=("seq", "w"))
    state, _ = run(
        t, B({"id": 1, "v": "a", "seq": 1, "w": "a"}, {"id": 1, "v": "b", "seq": 1, "w": "b"})
    )
    assert state == [R(1, "b", w="b", seq=1, e=1)]
    # a None in the second column loses against a value
    state, _ = run(
        t, B({"id": 1, "v": "a", "seq": 1, "w": None}, {"id": 1, "v": "b", "seq": 1, "w": "a"})
    )
    assert state == [R(1, "b", w="a", seq=1, e=1)]
    # the first column dominates
    state, _ = run(
        t, B({"id": 1, "v": "a", "seq": 2, "w": "a"}, {"id": 1, "v": "b", "seq": 1, "w": "z"})
    )
    assert state == [R(1, "a", w="a", seq=2, e=1)]


def test_table_validation():
    with pytest.raises(ValueError):
        T("scd2", has_delete=True, delete_mode="hard")
    with pytest.raises(ValueError):
        T("scd1", delete_mode="drop")
    with pytest.raises(ValueError):
        T("merge")
    with pytest.raises(ValueError):
        T("scd1", ignore=("nope",))
    with pytest.raises(ValueError):
        T("scd1", sequence_by=("nope",))
    with pytest.raises(ValueError):
        T("scd1", key=("nope",))
    with pytest.raises(ValueError):
        T("scd1", ignore=("id",))


def test_source_row_with_the_sentinel_key_raises_only_for_unknown_member_tables():
    t = T("scd1", unknown_member=True, sentinels={"id": -1})
    with pytest.raises(ValueError, match="sentinel"):
        run(t, B({"id": -1, "v": "a"}))
    with pytest.raises(ValueError, match="sentinel"):
        run(t, B({"id": 1, "v": "a"}), B(D(-1)))
    plain = T("scd1")
    state, _ = run(plain, B({"id": -1, "v": "a"}))
    assert state == [R(-1, "a", e=1)]


def test_soft_delete_applied_twice_is_idempotent():
    # a delete for an absent key is a no-op + idempotence: a re-delete does not bump _execution_id
    for strategy in ("scd1", "scd2"):
        t = T(strategy, has_delete=True)
        once, _ = run(t, B({"id": 1, "v": "a"}), B(D(1)))
        twice, _ = apply_batch(t, once, B(D(1)), 3)
        assert twice == once
        assert twice[0]["_execution_id"] == 2


def test_delete_and_normal_row_in_one_batch_resolve_by_sequence():
    for mode in ("soft", "hard"):
        t = T("scd1", sequence_by=("seq",), has_delete=True, delete_mode=mode)
        first = B({"id": 1, "v": "a"})
        delete_wins = B(D(1, seq=5), {"id": 1, "v": "b", "seq": 1})
        state, _ = apply_batch(t, apply_batch(t, [], first, 1)[0], delete_wins, 2)
        assert state == ([R(1, "a", e=2, d=True)] if mode == "soft" else [])
        normal_wins = B(D(1, seq=1), {"id": 1, "v": "b", "seq": 5})
        state, _ = apply_batch(t, apply_batch(t, [], first, 1)[0], normal_wins, 2)
        assert state == [R(1, "b", seq=5, e=2)]


def test_unknown_member_is_one_row_after_every_batch_and_reseeded_by_init():
    for strategy in ("replace", "append", "scd1", "scd2"):
        t = T(strategy, unknown_member=True, sentinels={"id": -1})
        state, ids = [], []
        for n in (1, 2, 3):
            state, _ = apply_batch(t, state, B({"id": n, "v": "a"}), n)
            members = [r for r in state if is_unknown_member(t, r)]
            assert len(members) == 1, (strategy, n)
            ids.append(members[0]["_execution_id"])
        assert ids == ([1, 2, 3] if strategy == "replace" else [1, 1, 1]), strategy
        state, _ = apply_batch(t, state, B({"id": 9, "v": "z"}), 4, init=True)
        members = [r for r in state if is_unknown_member(t, r)]
        assert [m["_execution_id"] for m in members] == [4], strategy
        assert len(state) == 2, strategy


def D(id, **kw):
    return {"id": id, "op": "D", **kw}


def test_scd1_soft_delete_marks_and_keeps_row():
    t = T("scd1", has_delete=True)
    state, _ = run(t, B({"id": 1, "v": "a"}, {"id": 2, "v": "b"}), B(D(1)))
    assert state == [R(1, "a", e=2, d=True), R(2, "b", e=1)]


def test_scd1_hard_delete_removes_row():
    t = T("scd1", has_delete=True, delete_mode="hard")
    state, _ = run(t, B({"id": 1, "v": "a"}, {"id": 2, "v": "b"}), B(D(1)))
    assert state == [R(2, "b", e=1)]


def test_delete_for_absent_key_is_a_noop():
    # a delete for an absent key is a no-op
    for strategy, mode in (("scd1", "soft"), ("scd1", "hard"), ("scd2", "soft")):
        t = T(strategy, has_delete=True, delete_mode=mode)
        cur = True if strategy == "scd2" else None
        state, _ = run(t, B({"id": 1, "v": "a"}), B(D(7)))
        assert state == [R(1, "a", e=1, cur=cur)], (strategy, mode)


def test_scd1_soft_deleted_key_returns_active_again_even_with_identical_values():
    # a returning soft-deleted key is active again
    t = T("scd1", has_delete=True)
    state, _ = run(t, B({"id": 1, "v": "a"}), B(D(1)), B({"id": 1, "v": "a"}))
    assert state == [R(1, "a", e=3, d=False)]


def test_scd1_hard_delete_then_return_is_a_fresh_insert():
    # a hard-deleted key that returns is a fresh insert
    t = T("scd1", has_delete=True, delete_mode="hard")
    state, _ = run(t, B({"id": 1, "v": "a"}), B(D(1)), B({"id": 1, "v": "a"}))
    assert state == [R(1, "a", e=3)]
    assert len(state) == 1


def test_delete_mode_ignore_removes_delete_rows_before_dedupe():
    # delete_mode ignore
    t = T("scd1", sequence_by=("seq",), has_delete=True, delete_mode="ignore")
    batch = B(
        D(1, seq=5),
        {"id": 1, "v": "keep", "seq": 1},
        D(2, seq=9),
        {"id": 3, "v": "x", "seq": 1},
        {"id": 3, "v": "y", "seq": 2},
    )
    state, stats = run(t, batch)
    assert state == [R(1, "keep", seq=1, e=1), R(3, "y", seq=2, e=1)]
    assert stats.duplicates_dropped == 1


def test_scd2_change_closes_and_opens():
    t = T("scd2")
    state, _ = run(t, B({"id": 1, "v": "a"}), B({"id": 1, "v": "b"}))
    assert state == [R(1, "a", e=2, cur=False), R(1, "b", e=2, cur=True)]
    state, _ = run(t, B({"id": 1, "v": "a"}), B({"id": 1, "v": "b"}), B({"id": 1, "v": "c"}))
    assert state == [
        R(1, "a", e=2, cur=False),
        R(1, "b", e=3, cur=False),
        R(1, "c", e=3, cur=True),
    ]


def test_scd2_unchanged_row_untouched():
    t = T("scd2")
    state, _ = run(t, B({"id": 1, "v": "a"}), B({"id": 1, "v": "a"}))
    assert state == [R(1, "a", e=1, cur=True)]


def test_scd2_soft_delete_closes_without_new_version():
    t = T("scd2", has_delete=True)
    state, _ = run(t, B({"id": 1, "v": "a"}), B(D(1)))
    assert state == [R(1, "a", e=2, d=True, cur=False)]
    assert not any(r["_is_current"] for r in state)


def test_scd2_deleted_key_returns_as_new_current_version():
    # a returning scd2 key opens a new version
    t = T("scd2", has_delete=True)
    state, _ = run(t, B({"id": 1, "v": "a"}), B(D(1)), B({"id": 1, "v": "a"}))
    assert state == [R(1, "a", e=2, d=True, cur=False), R(1, "a", e=3, d=False, cur=True)]


def test_ignore_columns_alone_update_in_place_without_bump():
    # ignore_columns alone update in place
    t1 = T("scd1", ignore=("w",))
    state, _ = run(t1, B({"id": 1, "v": "a", "w": "x"}), B({"id": 1, "v": "a", "w": "y"}))
    assert state == [R(1, "a", w="y", e=1)]
    t2 = T("scd2", ignore=("w",))
    state, _ = run(t2, B({"id": 1, "v": "a", "w": "x"}), B({"id": 1, "v": "a", "w": "y"}))
    assert state == [R(1, "a", w="y", e=1, cur=True)]
    state, _ = run(t1, B({"id": 1, "v": "a", "w": "x"}), B({"id": 1, "v": "b", "w": "y"}))
    assert state == [R(1, "b", w="y", e=2)]
    state, _ = run(t2, B({"id": 1, "v": "a", "w": "x"}), B({"id": 1, "v": "b", "w": "y"}))
    assert state == [R(1, "a", w="x", e=2, cur=False), R(1, "b", w="y", e=2, cur=True)]


def test_scd2_without_sequence_by_keeps_arrival_order():
    t = T("scd2")
    state, _ = run(t, B({"id": 1, "v": "new", "seq": 9}), B({"id": 1, "v": "old", "seq": 1}))
    assert [(r["v"], r["_is_current"]) for r in state] == [("new", False), ("old", True)]


def test_scd2_late_row_becomes_an_earlier_version():
    t = T("scd2", sequence_by=("seq",))
    state, _ = run(
        t,
        B({"id": 1, "v": "a", "seq": 5}),
        B({"id": 1, "v": "b", "seq": 9}),
        B({"id": 1, "v": "c", "seq": 7}),
    )
    # history by sequence: a(5) < c(7) < b(9); no existing version changes
    assert state == [
        R(1, "a", seq=5, e=2, cur=False),
        R(1, "c", seq=7, e=3, cur=False),
        R(1, "b", seq=9, e=2, cur=True),
    ]


def test_scd2_replayed_old_row_is_a_noop():
    # "equal" compares the tracked columns, sequence_by columns included (as for any row): a
    # replay of a row a stored version already carries changes nothing
    t = T("scd2", sequence_by=("seq",))
    first = [B({"id": 1, "v": "a", "seq": 5}), B({"id": 1, "v": "b", "seq": 9})]
    before, _ = run(t, *first)
    state, _ = run(t, *first, B({"id": 1, "v": "a", "seq": 5}))
    assert state == before
    state, _ = run(t, *first, *first)
    assert state == before


def test_scd2_late_row_older_than_every_version_becomes_the_first_one():
    t = T("scd2", sequence_by=("seq",))
    state, _ = run(t, B({"id": 1, "v": "b", "seq": 9}), B({"id": 1, "v": "a", "seq": 2}))
    assert state == [R(1, "a", seq=2, e=2, cur=False), R(1, "b", seq=9, e=1, cur=True)]


def test_scd2_late_row_equal_to_the_next_version_still_inserts():
    # only the version valid at the late row's sequence is compared
    t = T("scd2", sequence_by=("seq",))
    state, _ = run(
        t,
        B({"id": 1, "v": "a", "seq": 5}),
        B({"id": 1, "v": "b", "seq": 9}),
        B({"id": 1, "v": "b", "seq": 7}),
    )
    assert [(r["v"], r["seq"], r["_is_current"]) for r in state] == [
        ("a", 5, False),
        ("b", 7, False),
        ("b", 9, True),
    ]


def test_scd2_equal_sequence_across_batches_the_later_batch_wins():
    t = T("scd2", sequence_by=("seq",))
    state, _ = run(t, B({"id": 1, "v": "a", "seq": 5}), B({"id": 1, "v": "b", "seq": 5}))
    assert state == [R(1, "a", seq=5, e=2, cur=False), R(1, "b", seq=5, e=2, cur=True)]


def test_scd2_newer_sequence_closes_and_opens_as_before():
    t = T("scd2", sequence_by=("seq",))
    state, _ = run(t, B({"id": 1, "v": "a", "seq": 5}), B({"id": 1, "v": "b", "seq": 6}))
    assert state == [R(1, "a", seq=5, e=2, cur=False), R(1, "b", seq=6, e=2, cur=True)]


def test_scd2_late_row_only_differing_in_an_ignored_column_is_a_noop():
    t = T("scd2", sequence_by=("seq",), ignore=("w",))
    first = [B({"id": 1, "v": "a", "w": "x", "seq": 5}), B({"id": 1, "v": "b", "seq": 9})]
    before, _ = run(t, *first)
    state, _ = run(t, *first, B({"id": 1, "v": "a", "w": "y", "seq": 5}))
    assert state == before


def test_scd2_older_soft_delete_is_ignored_newer_closes():
    t = T("scd2", sequence_by=("seq",), has_delete=True)
    first = [B({"id": 1, "v": "a", "seq": 5})]
    before, _ = run(t, *first)
    state, _ = run(t, *first, B(D(1, seq=2)))
    assert state == before
    state, _ = run(t, *first, B(D(1, seq=8)))
    assert state == [R(1, "a", seq=5, e=2, d=True, cur=False)]


def test_scd2_late_row_before_a_deleted_last_version_is_inserted_earlier():
    t = T("scd2", sequence_by=("seq",), has_delete=True)
    state, _ = run(
        t,
        B({"id": 1, "v": "a", "seq": 5}),
        B(D(1, seq=6)),
        B({"id": 1, "v": "z", "seq": 2}),
    )
    assert state == [
        R(1, "z", seq=2, e=3, cur=False),
        R(1, "a", seq=5, e=2, d=True, cur=False),
    ]
    assert not any(r["_is_current"] for r in state)


def test_scd2_row_after_a_deleted_last_version_opens_a_new_current_one():
    t = T("scd2", sequence_by=("seq",), has_delete=True)
    state, _ = run(
        t,
        B({"id": 1, "v": "a", "seq": 5}),
        B(D(1, seq=6)),
        B({"id": 1, "v": "a", "seq": 7}),
    )
    assert state == [
        R(1, "a", seq=5, e=2, d=True, cur=False),
        R(1, "a", seq=7, e=3, cur=True),
    ]


def test_scd2_late_row_after_a_deleted_version_leaves_that_version_alone():
    t = T("scd2", sequence_by=("seq",), has_delete=True)
    state, _ = run(
        t,
        B({"id": 1, "v": "a", "seq": 1}),
        B(D(1, seq=2)),
        B({"id": 1, "v": "c", "seq": 8}),
        B({"id": 1, "v": "d", "seq": 4}),
    )
    assert state == [
        R(1, "a", seq=1, e=2, d=True, cur=False),
        R(1, "d", seq=4, e=4, cur=False),
        R(1, "c", seq=8, e=3, cur=True),
    ]


def test_scd1_older_sequence_is_ignored_across_batches():
    t = T("scd1", sequence_by=("seq",))
    state, _ = run(t, B({"id": 1, "v": "new", "seq": 9}), B({"id": 1, "v": "old", "seq": 1}))
    assert state == [R(1, "new", seq=9, e=1)]


def test_scd1_newer_and_equal_sequence_update():
    t = T("scd1", sequence_by=("seq",))
    state, _ = run(t, B({"id": 1, "v": "a", "seq": 1}), B({"id": 1, "v": "b", "seq": 2}))
    assert state == [R(1, "b", seq=2, e=2)]
    state, _ = run(t, B({"id": 1, "v": "a", "seq": 2}), B({"id": 1, "v": "b", "seq": 2}))
    assert state == [R(1, "b", seq=2, e=2)]


def test_scd1_without_sequence_by_last_batch_wins():
    t = T("scd1")
    state, _ = run(t, B({"id": 1, "v": "a", "seq": 9}), B({"id": 1, "v": "b", "seq": 1}))
    assert state == [R(1, "b", seq=1, e=2)]


def test_scd1_older_sequence_ignores_ignored_column_change_too():
    t = T("scd1", sequence_by=("seq",), ignore=("w",))
    state, _ = run(
        t, B({"id": 1, "v": "a", "w": "x", "seq": 9}), B({"id": 1, "v": "a", "w": "y", "seq": 1})
    )
    assert state == [R(1, "a", w="x", seq=9, e=1)]


def test_scd1_older_soft_delete_is_ignored_newer_applies():
    t = T("scd1", sequence_by=("seq",), has_delete=True)
    state, _ = run(t, B({"id": 1, "v": "a", "seq": 5}), B({"id": 1, "op": "D", "seq": 2}))
    assert state == [R(1, "a", seq=5, e=1)]
    state, _ = run(t, B({"id": 1, "v": "a", "seq": 5}), B({"id": 1, "op": "D", "seq": 7}))
    assert state == [R(1, "a", seq=5, e=2, d=True)]


def test_scd1_returning_soft_deleted_key_compares_against_stored_sequence():
    t = T("scd1", sequence_by=("seq",), has_delete=True)
    deleted = B({"id": 1, "op": "D", "seq": 7})
    first = B({"id": 1, "v": "a", "seq": 5})
    state, _ = run(t, first, deleted, B({"id": 1, "v": "old", "seq": 3}))
    assert state == [R(1, "a", seq=5, e=2, d=True)]
    state, _ = run(t, first, deleted, B({"id": 1, "v": "back", "seq": 6}))
    assert state == [R(1, "back", seq=6, e=3)]


def test_scd1_null_sequence_sorts_lowest():
    t = T("scd1", sequence_by=("seq",))
    state, _ = run(t, B({"id": 1, "v": "a", "seq": 1}), B({"id": 1, "v": "b", "seq": None}))
    assert state == [R(1, "a", seq=1, e=1)]
    state, _ = run(t, B({"id": 1, "v": "a", "seq": None}), B({"id": 1, "v": "b", "seq": 1}))
    assert state == [R(1, "b", seq=1, e=2)]
    state, _ = run(t, B({"id": 1, "v": "a", "seq": None}), B({"id": 1, "v": "b", "seq": None}))
    assert state == [R(1, "b", seq=None, e=2)]


def test_scd1_multi_column_sequence_compares_lexicographically():
    t = T("scd1", sequence_by=("seq", "w"))
    state, _ = run(
        t, B({"id": 1, "v": "a", "seq": 2, "w": 1}), B({"id": 1, "v": "b", "seq": 1, "w": 9})
    )
    assert state == [R(1, "a", seq=2, w=1, e=1)]
    state, _ = run(
        t, B({"id": 1, "v": "a", "seq": 2, "w": 1}), B({"id": 1, "v": "b", "seq": 2, "w": 2})
    )
    assert state == [R(1, "b", seq=2, w=2, e=2)]


def test_scd1_hard_delete_then_older_row_inserts_known_limit():
    t = T("scd1", sequence_by=("seq",), has_delete=True, delete_mode="hard")
    state, _ = run(
        t,
        B({"id": 1, "v": "a", "seq": 5}),
        B({"id": 1, "op": "D", "seq": 6}),
        B({"id": 1, "v": "old", "seq": 1}),
    )
    assert state == [R(1, "old", seq=1, e=3)]


def test_single_key_null_row_is_dropped_and_counted():
    # NULL business keys
    t = T("scd1")
    state, stats = run(t, B({"id": None, "v": "a"}, {"id": 1, "v": "b"}))
    assert state == [R(1, "b", e=1)]
    assert stats.null_keys_dropped == 1
    assert stats.duplicates_dropped == 0


def test_composite_key_with_one_null_component_is_a_value():
    # NULL business keys
    t = T("scd1", key=("id", "v"))
    state, stats = run(t, B({"id": 1, "v": None, "w": "p"}))
    assert state == [R(1, None, w="p", e=1)]
    assert stats.null_keys_dropped == 0
    state, _ = run(t, B({"id": 1, "v": None, "w": "p"}), B({"id": 1, "v": None, "w": "q"}))
    assert state == [R(1, None, w="q", e=2)]
    state, _ = run(t, B({"id": 1, "v": None, "w": "p"}), B({"id": 1, "v": None, "w": "p"}))
    assert state == [R(1, None, w="p", e=1)]


def test_composite_key_all_null_row_is_dropped():
    t = T("scd1", key=("id", "v"))
    state, stats = run(t, B({"id": None, "v": None, "w": "p"}, {"id": 1, "v": None}))
    assert state == [R(1, None, e=1)]
    assert stats.null_keys_dropped == 1


def test_replace_and_append_keep_null_key_rows():
    # NULL business keys: the NULL-key drop applies to scd1/scd2 only, where
    # the key drives the merge; replace/append keep every row as it comes.
    for strategy in ("replace", "append"):
        t = T(strategy)
        state, stats = run(t, B({"id": None, "v": "a"}, {"id": 1, "v": "b"}))
        assert state == [R(None, "a", e=1), R(1, "b", e=1)], strategy
        assert stats.null_keys_dropped == 0
        tc = T(strategy, key=("id", "v"))
        state, stats = run(tc, B({"id": None, "v": None, "w": "p"}))
        assert state == [R(None, None, w="p", e=1)], strategy
        assert stats.null_keys_dropped == 0


def test_unknown_member_replace_and_append_still_drop_any_null_key_column():
    # NULL business keys: unknown_member tables drop on every strategy (NULL is the member's key)
    for strategy in ("replace", "append"):
        t = T(strategy, unknown_member=True, sentinels={"id": -1})
        state, stats = run(t, B({"id": None, "v": "a"}, {"id": 1, "v": "b"}))
        assert stats.null_keys_dropped == 1, strategy
        assert [r["id"] for r in state] == [1, -1], strategy


def test_unknown_member_table_drops_a_row_with_any_null_key_column():
    # NULL business keys
    t = T("scd1", key=("id", "v"), unknown_member=True, sentinels={"id": -1, "v": None})
    state, stats = run(t, B({"id": 1, "v": None}, {"id": 2, "v": "k"}))
    assert stats.null_keys_dropped == 1
    assert [r["id"] for r in state] == [2, -1]


def test_unknown_member_row_is_seeded_once_and_never_touched():
    unk = R(-1, None, op=None, e=1)
    t = T("scd1", unknown_member=True, sentinels={"id": -1})
    state, _ = run(t, B({"id": 1, "v": "a"}), B({"id": 1, "v": "b"}))
    assert state == [R(1, "b", e=2), unk]
    assert [r["_execution_id"] for r in state if is_unknown_member(t, r)] == [1]
    assert sum(is_unknown_member(t, r) for r in state) == 1

    ts = T("scd1", unknown_member=True, sentinels={"id": None})
    state, stats = run(ts, B({"id": "a", "v": "x"}, {"id": None, "v": "bad"}))
    assert stats.null_keys_dropped == 1
    assert [r["id"] for r in state] == ["a", None]
    assert sum(is_unknown_member(ts, r) for r in state) == 1

    t2 = T("scd2", unknown_member=True, sentinels={"id": -1})
    state, _ = run(t2, B({"id": 1, "v": "a"}), B({"id": 1, "v": "b"}))
    members = [r for r in state if is_unknown_member(t2, r)]
    assert len(members) == 1
    assert members[0]["_is_current"] is True
    assert members[0]["_execution_id"] == 1

    tr = T("replace", unknown_member=True, sentinels={"id": -1})
    state, _ = run(tr, B({"id": 1, "v": "a"}), B({"id": 2, "v": "b"}))
    assert [(r["id"], r["_execution_id"]) for r in state] == [(2, 2), (-1, 2)]
    assert sum(is_unknown_member(tr, r) for r in state) == 1


def test_init_rebuilds_from_the_batch_only():
    for strategy in ("replace", "append", "scd1", "scd2"):
        scd = strategy.startswith("scd")
        t = T(strategy, has_delete=scd)
        cur = True if strategy == "scd2" else None
        first, _ = run(t, B({"id": 1, "v": "a"}), B({"id": 1, "v": "b"}))
        # delete rows only exist for the scd strategies; in init they produce no rows
        batch = B({"id": 2, "v": "n"}, *([D(3)] if scd else []))
        state, _ = apply_batch(t, first, batch, 5, init=True)
        assert state == [R(2, "n", e=5, cur=cur)], strategy


def test_apply_batch_does_not_mutate_its_input():
    for strategy in ("replace", "append", "scd1", "scd2"):
        t = T(strategy, has_delete=True, unknown_member=True, sentinels={"id": -1})
        state, _ = run(t, B({"id": 1, "v": "a"}, {"id": 2, "v": "b"}))
        batch = B({"id": 1, "v": "z"}, D(2), {"id": 3, "v": "n"})
        state_before, batch_before = copy.deepcopy(state), copy.deepcopy(batch)
        apply_batch(t, state, batch, 2)
        apply_batch(t, state, batch, 2, init=True)
        assert state == state_before
        assert batch == batch_before


def test_scd2_two_late_rows_of_equal_sequence_the_later_batch_is_the_later_version():
    t = T("scd2", sequence_by=("seq",))
    state, _ = run(
        t,
        B({"id": 1, "v": "c", "seq": 2}),
        B({"id": 1, "v": "a", "seq": 1}),
        B({"id": 1, "v": "b", "seq": 1}),
    )
    assert state == [
        R(1, "a", seq=1, e=2, cur=False),
        R(1, "b", seq=1, e=3, cur=False),
        R(1, "c", seq=2, e=1, cur=True),
    ]
