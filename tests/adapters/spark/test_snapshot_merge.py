"""`merge.snapshot`: the batch is the complete current state of the source, so an active key of
the table that is not in the batch is handled like a soft or hard delete row for it."""

from typing import Any

import pytest

from pipetree.adapters.spark.merge import (
    SnapshotGuardError,
    merge_scd1,
    merge_scd2,
    seed_unknown_member,
)
from pipetree.model import Table

pytestmark = pytest.mark.spark

_SCHEMA = "id int, name string, op string"
_SEQ_SCHEMA = "id int, name string, op string, seq int"
_MERGE = {"scd1": merge_scd1, "scd2": merge_scd2}


def _table(name: str, strategy: str, merge: dict[str, Any] | None = None, **extra: Any) -> Table:
    return Table.model_validate(
        {
            "name": name,
            "layer": "test",
            "table_schema": f"snap_{strategy}",
            "fqn": f"snap_{strategy}.{name}",
            "strategy": strategy,
            "business_key": ["id"],
            "merge": {"snapshot": True, "delete_when": "op = 'D'", **(merge or {})},
            **extra,
        }
    )


def _run(spark, table: Table, data, execution_id: int, *, init=False, schema=_SCHEMA) -> dict:
    df = spark.createDataFrame(data, schema)
    return _MERGE[table.strategy](spark, table, df, execution_id, "crm", init)


def _people(*ids: int) -> list[tuple]:
    return [(i, f"n{i}", None) for i in ids]


def _all(spark, table: Table) -> list[dict]:
    return [r.asDict() for r in spark.table(table.fqn).collect()]


def _by_id(spark, table: Table) -> dict:
    return {r["id"]: r for r in _all(spark, table)}


def _versions(spark, table: Table, key: int) -> list[dict]:
    return sorted((r for r in _all(spark, table) if r["id"] == key), key=lambda r: r["_valid_from"])


def _sorted_rows(spark, table: Table) -> list[tuple]:
    return sorted(
        (tuple(sorted(r.items(), key=lambda kv: kv[0])) for r in _all(spark, table)), key=repr
    )


# ------------------------------------------------------------------ scd1


def test_scd1_soft_a_missing_key_is_soft_deleted_like_a_delete_row(spark):
    table = _table("soft", "scd1")
    _run(spark, table, _people(1, 2, 3, 4), 1)
    before = _by_id(spark, table)

    result = _run(spark, table, _people(1, 2, 3), 2)

    after = _by_id(spark, table)
    assert result["snapshot_deleted"] == 1
    assert after[4]["_is_deleted"] is True
    assert after[4]["_execution_id"] == 2
    assert after[4]["_updated_at"] >= before[4]["_updated_at"]
    assert after[4]["_inserted_at"] == before[4]["_inserted_at"]
    assert after[4]["_source_system"] == "crm"
    assert after[4]["name"] == "n4"
    for key in (1, 2, 3):  # unchanged keys are untouched: no audit bump
        assert after[key] == before[key]


def test_scd1_hard_a_missing_key_is_removed(spark):
    table = _table("hard", "scd1", {"delete_mode": "hard"})
    _run(spark, table, _people(1, 2, 3, 4), 1)
    before = _by_id(spark, table)

    result = _run(spark, table, _people(1, 2, 3), 2)

    after = _by_id(spark, table)
    assert result["snapshot_deleted"] == 1
    assert set(after) == {1, 2, 3}
    assert all(after[k] == before[k] for k in (1, 2, 3))


def test_scd1_a_missing_key_that_returns_is_active_again(spark):
    table = _table("revive1", "scd1")
    _run(spark, table, _people(1, 2, 3, 4), 1)
    _run(spark, table, _people(1, 2, 3), 2)

    result = _run(spark, table, _people(1, 2, 3, 4), 3)

    after = _by_id(spark, table)
    assert result["snapshot_deleted"] == 0
    assert after[4]["_is_deleted"] is False
    assert after[4]["_execution_id"] == 3


def test_scd1_hard_a_missing_key_that_returns_is_a_fresh_insert(spark):
    table = _table("revive_hard", "scd1", {"delete_mode": "hard"})
    _run(spark, table, _people(1, 2, 3, 4), 1)
    _run(spark, table, _people(1, 2, 3), 2)

    _run(spark, table, _people(1, 2, 3, 4), 3)

    after = _by_id(spark, table)
    assert after[4]["_is_deleted"] is False
    assert after[4]["_inserted_at"] == after[4]["_updated_at"]
    assert after[4]["_execution_id"] == 3


def test_scd1_an_already_deleted_row_is_not_deleted_again(spark):
    table = _table("redelete", "scd1")
    _run(spark, table, _people(1, 2, 3, 4), 1)
    _run(spark, table, _people(1, 2, 3), 2)
    first = _by_id(spark, table)[4]

    result = _run(spark, table, _people(1, 2, 3), 3)

    assert result["snapshot_deleted"] == 0
    assert _by_id(spark, table)[4] == first


def test_scd1_sequence_a_missing_key_is_deleted_even_with_a_high_sequence(spark):
    table = _table("seq1", "scd1", {"sequence_by": ["seq"]})
    _run(spark, table, [(1, "a", None, 1), (2, "b", None, 1), (3, "c", None, 1),
                        (4, "d", None, 99)], 1, schema=_SEQ_SCHEMA)  # fmt: skip

    result = _run(
        spark, table, [(1, "a", None, 1), (2, "b", None, 1), (3, "c", None, 1)], 2,
        schema=_SEQ_SCHEMA,
    )  # fmt: skip

    after = _by_id(spark, table)
    assert result["snapshot_deleted"] == 1
    assert after[4]["_is_deleted"] is True
    assert after[4]["seq"] == 99  # the stored sequence stays as it was


def test_scd1_sequence_a_returning_key_lower_than_the_stored_sequence_stays_deleted(spark):
    table = _table("seq2", "scd1", {"sequence_by": ["seq"]})
    base = [(1, "a", None, 1), (2, "b", None, 1), (3, "c", None, 1)]
    _run(spark, table, [*base, (4, "d", None, 10)], 1, schema=_SEQ_SCHEMA)
    _run(spark, table, base, 2, schema=_SEQ_SCHEMA)

    _run(spark, table, [*base, (4, "old", None, 5)], 3, schema=_SEQ_SCHEMA)
    assert _by_id(spark, table)[4]["_is_deleted"] is True

    _run(spark, table, [*base, (4, "d", None, 10)], 4, schema=_SEQ_SCHEMA)
    assert _by_id(spark, table)[4]["_is_deleted"] is False  # equal sequence: the later batch


def test_scd1_composite_key_with_a_null_part_is_matched_null_safely(spark):
    table = Table.model_validate(
        {
            "name": "composite",
            "layer": "test",
            "table_schema": "snap_scd1",
            "fqn": "snap_scd1.composite",
            "strategy": "scd1",
            "business_key": ["a", "b"],
            "merge": {"snapshot": True},
        }
    )
    schema = "a int, b string, name string"
    data = [(1, None, "x"), (2, "k", "y"), (3, "k", "z")]
    merge_scd1(spark, table, spark.createDataFrame(data, schema), 1, "crm")

    result = merge_scd1(spark, table, spark.createDataFrame(data[:2], schema), 2, "crm")

    deleted = {(r["a"], r["b"]): r["_is_deleted"] for r in spark.table(table.fqn).collect()}
    assert result["snapshot_deleted"] == 1
    assert deleted == {(1, None): False, (2, "k"): False, (3, "k"): True}


# ------------------------------------------------------------------ scd2


def test_scd2_soft_a_missing_key_closes_its_current_version_like_a_delete_row(spark):
    table = _table("soft", "scd2")
    _run(spark, table, _people(1, 2, 3, 4), 1)
    before = _by_id(spark, table)

    result = _run(spark, table, _people(1, 2, 3), 2)

    assert result["snapshot_deleted"] == 1
    versions = _versions(spark, table, 4)
    assert len(versions) == 1  # closed, no new version
    closed = versions[0]
    assert closed["_is_current"] is False
    assert closed["_is_deleted"] is True
    assert closed["_valid_to"] is not None
    assert closed["_valid_from"] == before[4]["_valid_from"]
    assert closed["_execution_id"] == 2
    after = _by_id(spark, table)
    for key in (1, 2, 3):
        assert after[key] == before[key]


def test_scd2_a_missing_key_that_returns_opens_a_new_version(spark):
    table = _table("revive2", "scd2")
    _run(spark, table, _people(1, 2, 3, 4), 1)
    _run(spark, table, _people(1, 2, 3), 2)

    result = _run(spark, table, _people(1, 2, 3, 4), 3)

    versions = _versions(spark, table, 4)
    assert result["snapshot_deleted"] == 0
    assert [(v["_is_current"], v["_is_deleted"]) for v in versions] == [
        (False, True),
        (True, False),
    ]


def test_scd2_sequence_a_missing_key_is_closed_even_with_a_high_sequence(spark):
    table = _table("seq1", "scd2", {"sequence_by": ["seq"]})
    base = [(1, "a", None, 1), (2, "b", None, 1), (3, "c", None, 1)]
    _run(spark, table, [*base, (4, "d", None, 99)], 1, schema=_SEQ_SCHEMA)

    result = _run(spark, table, base, 2, schema=_SEQ_SCHEMA)

    assert result["snapshot_deleted"] == 1
    [closed] = _versions(spark, table, 4)
    assert (closed["_is_current"], closed["_is_deleted"]) == (False, True)


def test_scd2_a_key_present_only_as_a_late_row_is_not_missing(spark):
    table = _table("late", "scd2", {"sequence_by": ["seq"]})
    base = [(1, "a", None, 5), (2, "b", None, 5), (3, "c", None, 5)]
    _run(spark, table, [*base, (4, "d", None, 5)], 1, schema=_SEQ_SCHEMA)

    result = _run(spark, table, [*base, (4, "late", None, 1)], 2, schema=_SEQ_SCHEMA)

    assert result["snapshot_deleted"] == 0
    current = [v for v in _versions(spark, table, 4) if v["_is_current"]]
    assert [(v["name"], v["_is_deleted"]) for v in current] == [("d", False)]


# ------------------------------------------------------------ both strategies


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_the_unknown_member_is_never_missing_and_never_touched(spark, strategy):
    table = _table("unknown", strategy, unknown_member=True)
    _run(spark, table, _people(1, 2), 1)
    seed_unknown_member(spark, table, execution_id=1)
    unknown_before = _by_id(spark, table)[-1]

    result = _run(spark, table, _people(1), 2)

    assert result["snapshot_deleted"] == 1  # key 2 of the active keys {1, 2}
    assert _by_id(spark, table)[-1] == unknown_before


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_an_explicit_delete_row_and_a_missing_key_in_one_batch(spark, strategy):
    table = _table("explicit", strategy)
    _run(spark, table, _people(1, 2, 3, 4), 1)

    result = _run(spark, table, [(1, "n1", None), (2, "n2", "D"), (3, "n3", None)], 2)

    assert result["snapshot_deleted"] == 1  # key 2 is in the batch: an ordinary delete
    live = {r["id"] for r in _all(spark, table) if not r["_is_deleted"]}
    deleted = {r["id"] for r in _all(spark, table) if r["_is_deleted"]}
    assert live == {1, 3}
    assert deleted == {2, 4}
    assert all(r["_execution_id"] == 2 for r in _all(spark, table) if r["id"] in (2, 4))


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_delete_mode_ignore_keeps_missing_keys_active_without_a_guard(spark, strategy):
    table = _table("ignore", strategy, {"delete_mode": "ignore"})
    _run(spark, table, _people(1, 2, 3, 4), 1)
    before = _sorted_rows(spark, table)

    result = _run(spark, table, _people(1), 2)  # 3 of 4 missing: over the 0.5 default
    empty = _run(spark, table, [], 3)

    assert result["snapshot_deleted"] == 0
    assert empty["snapshot_deleted"] == 0
    assert _sorted_rows(spark, table) == before


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_snapshot_off_keeps_missing_keys_active_and_reports_zero(spark, strategy):
    table = _table("off", strategy, {"snapshot": False})
    _run(spark, table, _people(1, 2, 3, 4), 1)

    result = _run(spark, table, _people(1), 2)

    assert result["snapshot_deleted"] == 0
    assert not any(r["_is_deleted"] for r in _all(spark, table))


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_first_load_and_init_delete_nothing_and_skip_the_guard(spark, strategy):
    table = _table("init", strategy)

    first = _run(spark, table, [], 1)  # an empty first load is no error
    assert first["snapshot_deleted"] == 0
    _run(spark, table, _people(1, 2, 3, 4), 2)

    result = _run(spark, table, _people(1), 3, init=True)

    assert result["snapshot_deleted"] == 0
    assert [(r["id"], r["_is_deleted"]) for r in _all(spark, table)] == [(1, False)]


# ------------------------------------------------------------------ guard


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_guard_a_missing_ratio_over_the_limit_fails_and_writes_nothing(spark, strategy):
    table = _table("guard_ratio", strategy)
    _run(spark, table, _people(1, 2, 3, 4), 1)
    before = _sorted_rows(spark, table)

    with pytest.raises(SnapshotGuardError) as info:
        # 3 of 4 missing (0.75 > 0.5); key 1 changes and key 9 is new, neither is written
        _run(spark, table, [(1, "changed", None), (9, "new", None)], 2)

    message = str(info.value)
    assert table.fqn in message
    assert "3" in message and "4" in message
    assert "75" in message
    assert "merge.snapshot_max_missing_ratio" in message
    assert _sorted_rows(spark, table) == before


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_guard_a_ratio_exactly_at_the_limit_passes(spark, strategy):
    table = _table("guard_equal", strategy)
    _run(spark, table, _people(1, 2, 3, 4), 1)

    result = _run(spark, table, _people(1, 2), 2)

    assert result["snapshot_deleted"] == 2


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_guard_a_custom_ratio_is_honoured(spark, strategy):
    table = _table("guard_custom", strategy, {"snapshot_max_missing_ratio": 0.2})
    _run(spark, table, _people(1, 2, 3, 4, 5), 1)
    _run(spark, table, _people(1, 2, 3, 4), 2)  # 1 of 5 = 0.2: at the limit

    with pytest.raises(SnapshotGuardError):
        _run(spark, table, _people(1, 2, 3), 3)  # 1 of 4 = 0.25


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_guard_an_empty_batch_fails_unless_allowed(spark, strategy):
    table = _table("guard_empty", strategy, {"snapshot_max_missing_ratio": 1.0})
    _run(spark, table, _people(1, 2), 1)
    before = _sorted_rows(spark, table)

    with pytest.raises(SnapshotGuardError) as info:
        _run(spark, table, [], 2)

    assert "merge.snapshot_allow_empty" in str(info.value)
    assert table.fqn in str(info.value)
    assert _sorted_rows(spark, table) == before


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_guard_allow_empty_with_ratio_one_deletes_every_key(spark, strategy):
    table = _table(
        "guard_allow",
        strategy,
        {"snapshot_allow_empty": True, "snapshot_max_missing_ratio": 1.0},
    )
    _run(spark, table, _people(1, 2), 1)

    result = _run(spark, table, [], 2)

    assert result["snapshot_deleted"] == 2
    assert all(r["_is_deleted"] for r in _all(spark, table))


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_guard_allow_empty_still_obeys_the_ratio(spark, strategy):
    table = _table("guard_allow_ratio", strategy, {"snapshot_allow_empty": True})
    _run(spark, table, _people(1, 2), 1)

    with pytest.raises(SnapshotGuardError):
        _run(spark, table, [], 2)


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_an_empty_batch_against_a_table_without_active_keys_is_no_error(spark, strategy):
    table = _table("guard_none_active", strategy, {"snapshot_max_missing_ratio": 1.0})
    _run(spark, table, [(1, "n1", None), (2, "n2", None)], 1)
    _run(spark, table, [(1, "n1", "D"), (2, "n2", "D")], 2)  # every key deleted explicitly

    result = _run(spark, table, [], 3)

    assert result["snapshot_deleted"] == 0
