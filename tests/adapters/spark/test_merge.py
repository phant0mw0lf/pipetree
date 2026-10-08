import datetime
import decimal
import logging
from typing import Any

import pytest

from pipetree.adapters.spark.merge import (
    drop_null_business_keys,
    merge_append,
    merge_replace,
    merge_scd1,
    merge_scd2,
    seed_unknown_member,
    strip_reserved_columns,
)
from pipetree.model import Table

pytestmark = pytest.mark.spark

DATE_2026 = datetime.date(2026, 1, 1)


def make_table(name: str, strategy: str, **overrides: Any) -> Table:
    # model_validate (rather than the Table(...) constructor) accepts raw
    # dicts for nested fields like `merge`, matching how config really
    # builds a Table from parsed YAML.
    raw: dict[str, Any] = {
        "name": name,
        "layer": "test",
        "table_schema": f"test_{strategy}",
        "fqn": f"test_{strategy}.{name}",
        "strategy": strategy,
        "business_key": ["id"],
        **overrides,
    }
    return Table.model_validate(raw)


def rows(df, key="id"):
    return {r[key]: r.asDict() for r in df.collect()}


# ---------------------------------------------------------------- replace


def test_replace_creates_the_table_on_first_run(spark):
    table = make_table("customer", "replace")
    source = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])

    result = merge_replace(spark, table, source, execution_id=1, source_system="crm")

    written = spark.table(table.fqn)
    assert result["rows_written"] == 2
    assert {r["name"] for r in written.collect()} == {"a", "b"}
    assert {r["_execution_id"] for r in written.collect()} == {1}


def test_replace_fully_overwrites_on_the_next_run(spark):
    table = make_table("orders", "replace")
    merge_replace(spark, table, spark.createDataFrame([(1, "a")], ["id", "name"]), 1, "crm")

    merge_replace(spark, table, spark.createDataFrame([(2, "b")], ["id", "name"]), 2, "crm")

    written = rows(spark.table(table.fqn))
    assert set(written) == {2}


# ----------------------------------------------------------------- append


def test_append_creates_the_table_on_first_run(spark):
    table = make_table("events", "append")
    source = spark.createDataFrame([(1, "click")], ["id", "kind"])

    result = merge_append(spark, table, source, execution_id=1, source_system="web")

    assert result["rows_written"] == 1
    written = spark.table(table.fqn).collect()[0]
    assert written["_inserted_at"] == written["_updated_at"]


def test_append_adds_rows_without_removing_existing_ones(spark):
    table = make_table("events2", "append")
    merge_append(spark, table, spark.createDataFrame([(1, "click")], ["id", "kind"]), 1, "web")

    merge_append(spark, table, spark.createDataFrame([(2, "view")], ["id", "kind"]), 2, "web")

    assert set(rows(spark.table(table.fqn))) == {1, 2}


# ------------------------------------------------------------------ scd1


def test_scd1_seeds_the_table_on_first_run(spark):
    table = make_table("customer1", "scd1")
    source = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

    result = merge_scd1(spark, table, source, execution_id=1, source_system="crm")

    written = rows(spark.table(table.fqn))
    assert result["rows_written"] == 2
    assert written[1]["name"] == "Alice"
    assert written[1]["_is_deleted"] is False


def test_scd1_updates_a_changed_row_and_inserts_a_new_one(spark):
    table = make_table("customer2", "scd1")
    merge_scd1(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 1, "crm")

    merge_scd1(
        spark,
        table,
        spark.createDataFrame([(1, "Alice Renamed"), (2, "Bob")], ["id", "name"]),
        2,
        "crm",
    )

    written = rows(spark.table(table.fqn))
    assert written[1]["name"] == "Alice Renamed"
    assert written[1]["_execution_id"] == 2
    assert written[2]["name"] == "Bob"


def test_scd1_does_not_touch_updated_at_when_nothing_changed(spark):
    table = make_table("customer3", "scd1")
    merge_scd1(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 1, "crm")
    first_updated_at = rows(spark.table(table.fqn))[1]["_updated_at"]

    merge_scd1(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 2, "crm")

    written = rows(spark.table(table.fqn))[1]
    assert written["_updated_at"] == first_updated_at
    assert written["_execution_id"] == 1  # untouched: still stamped by the original run


def test_scd1_ignore_columns_are_excluded_from_change_detection(spark):
    table = make_table("customer4", "scd1", merge={"ignore_columns": ["last_seen_at"]})
    merge_scd1(
        spark,
        table,
        spark.createDataFrame([(1, "Alice", 100)], ["id", "name", "last_seen_at"]),
        1,
        "crm",
    )

    # only last_seen_at changed - ignored, so no update should happen
    merge_scd1(
        spark,
        table,
        spark.createDataFrame([(1, "Alice", 200)], ["id", "name", "last_seen_at"]),
        2,
        "crm",
    )

    written = rows(spark.table(table.fqn))[1]
    assert written["_execution_id"] == 1
    assert written["last_seen_at"] == 100


def test_scd1_soft_delete_marks_is_deleted_and_keeps_the_row(spark):
    table = make_table(
        "customer5",
        "scd1",
        merge={"delete_when": "is_deleted_flag = true", "delete_mode": "soft"},
    )
    merge_scd1(
        spark,
        table,
        spark.createDataFrame([(1, "Alice", False)], ["id", "name", "is_deleted_flag"]),
        1,
        "crm",
    )

    merge_scd1(
        spark,
        table,
        spark.createDataFrame([(1, "Alice", True)], ["id", "name", "is_deleted_flag"]),
        2,
        "crm",
    )

    written = rows(spark.table(table.fqn))[1]
    assert written["_is_deleted"] is True


_SOFT_COLS = ["id", "name", "is_deleted_flag"]


def _soft_table(name: str, **overrides: Any) -> Table:
    return make_table(
        name,
        "scd1",
        merge={"delete_when": "is_deleted_flag = true", "delete_mode": "soft"},
        **overrides,
    )


def _soft_run(spark, table, data, execution_id):
    merge_scd1(spark, table, spark.createDataFrame(data, _SOFT_COLS), execution_id, "crm")


def test_scd1_soft_deleted_key_that_returns_with_identical_values_is_active_again(spark):
    table = _soft_table("revive_same")
    _soft_run(spark, table, [(1, "Alice", False)], 1)
    _soft_run(spark, table, [(1, "Alice", True)], 2)

    _soft_run(spark, table, [(1, "Alice", False)], 3)

    written = rows(spark.table(table.fqn))[1]
    assert written["_is_deleted"] is False
    assert written["name"] == "Alice"
    assert written["_execution_id"] == 3


def test_scd1_soft_deleted_key_that_returns_with_changed_values_is_active_with_new_values(spark):
    table = _soft_table("revive_changed")
    _soft_run(spark, table, [(1, "Alice", False)], 1)
    _soft_run(spark, table, [(1, "Alice", True)], 2)

    _soft_run(spark, table, [(1, "Alicia", False)], 3)

    written = rows(spark.table(table.fqn))[1]
    assert written["_is_deleted"] is False
    assert written["name"] == "Alicia"
    assert written["_execution_id"] == 3


def test_scd1_revived_row_keeps_its_surrogate_key_and_inserted_at(spark):
    table = _soft_table("revive_sk", surrogate_key="sid")
    _soft_run(spark, table, [(1, "Alice", False), (2, "Bob", False)], 1)
    before = rows(spark.table(table.fqn))
    _soft_run(spark, table, [(1, "Alice", True)], 2)

    _soft_run(spark, table, [(1, "Alice", False)], 3)

    after = rows(spark.table(table.fqn))
    assert after[1]["_is_deleted"] is False
    assert after[1]["sid"] == before[1]["sid"]
    assert after[1]["_inserted_at"] == before[1]["_inserted_at"]
    assert after[2]["sid"] == before[2]["sid"]
    assert spark.table(table.fqn).count() == 2


def test_scd1_deleting_an_already_soft_deleted_row_is_a_no_op(spark):
    table = _soft_table("redelete")
    _soft_run(spark, table, [(1, "Alice", False)], 1)
    _soft_run(spark, table, [(1, "Alice", True)], 2)
    first = rows(spark.table(table.fqn))[1]

    _soft_run(spark, table, [(1, "Alice", True)], 3)

    second = rows(spark.table(table.fqn))[1]
    assert second == first
    assert second["_is_deleted"] is True
    assert second["_execution_id"] == 2


def test_scd1_deleting_a_live_row_soft_deletes_and_bumps_the_audit_columns(spark):
    table = _soft_table("delete_live")
    _soft_run(spark, table, [(1, "Alice", False)], 1)

    _soft_run(spark, table, [(1, "Alice", True)], 2)

    written = rows(spark.table(table.fqn))[1]
    assert written["_is_deleted"] is True
    assert written["_execution_id"] == 2


def test_scd1_deleting_an_absent_key_is_a_no_op(spark):
    table = _soft_table("delete_absent")
    _soft_run(spark, table, [(1, "Alice", False)], 1)

    _soft_run(spark, table, [(9, "Ghost", True)], 2)

    written = rows(spark.table(table.fqn))
    assert set(written) == {1}
    assert written[1]["_execution_id"] == 1


def test_scd1_hard_delete_physically_removes_the_row(spark):
    table = make_table(
        "customer6",
        "scd1",
        merge={"delete_when": "is_deleted_flag = true", "delete_mode": "hard"},
    )
    merge_scd1(
        spark,
        table,
        spark.createDataFrame([(1, "Alice", False)], ["id", "name", "is_deleted_flag"]),
        1,
        "crm",
    )

    merge_scd1(
        spark,
        table,
        spark.createDataFrame([(1, "Alice", True)], ["id", "name", "is_deleted_flag"]),
        2,
        "crm",
    )

    assert 1 not in rows(spark.table(table.fqn))


def test_scd1_deduplicates_the_source_batch_and_reports_it(spark):
    table = make_table("customer7", "scd1", merge={"sequence_by": ["version"]})
    source = spark.createDataFrame(
        [(1, "Alice v1", 1), (1, "Alice v2", 2)], ["id", "name", "version"]
    )

    result = merge_scd1(spark, table, source, execution_id=1, source_system="crm")

    assert result["duplicates_dropped"] == 1
    assert rows(spark.table(table.fqn))[1]["name"] == "Alice v2"


# ------------------------------------------------------------------ scd2


def test_scd2_seeds_the_table_with_an_open_current_version(spark):
    table = make_table("dim1", "scd2")
    source = spark.createDataFrame([(1, "Alice")], ["id", "name"])

    merge_scd2(spark, table, source, execution_id=1, source_system="crm")

    written = rows(spark.table(table.fqn))[1]
    assert written["_is_current"] is True
    assert written["_valid_to"] is None


def test_scd2_change_closes_old_version_and_opens_a_new_one(spark):
    table = make_table("dim2", "scd2")
    merge_scd2(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 1, "crm")

    merge_scd2(
        spark, table, spark.createDataFrame([(1, "Alice Renamed")], ["id", "name"]), 2, "crm"
    )

    all_rows = spark.table(table.fqn).filter("id = 1").collect()
    assert len(all_rows) == 2

    closed = [r for r in all_rows if r["_is_current"] is False]
    current = [r for r in all_rows if r["_is_current"] is True]
    assert len(closed) == 1 and closed[0]["name"] == "Alice"
    assert closed[0]["_valid_to"] is not None
    assert len(current) == 1 and current[0]["name"] == "Alice Renamed"
    assert current[0]["_valid_to"] is None


def test_scd2_unchanged_row_stays_as_a_single_current_version(spark):
    table = make_table("dim3", "scd2")
    merge_scd2(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 1, "crm")

    merge_scd2(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 2, "crm")

    all_rows = spark.table(table.fqn).filter("id = 1").collect()
    assert len(all_rows) == 1
    assert all_rows[0]["_is_current"] is True


def test_scd2_soft_delete_closes_current_version_without_opening_a_new_one(spark):
    table = make_table(
        "dim4",
        "scd2",
        merge={"delete_when": "is_deleted_flag = true", "delete_mode": "soft"},
    )
    merge_scd2(
        spark,
        table,
        spark.createDataFrame([(1, "Alice", False)], ["id", "name", "is_deleted_flag"]),
        1,
        "crm",
    )

    merge_scd2(
        spark,
        table,
        spark.createDataFrame([(1, "Alice", True)], ["id", "name", "is_deleted_flag"]),
        2,
        "crm",
    )

    all_rows = spark.table(table.fqn).filter("id = 1").collect()
    assert len(all_rows) == 1
    assert all_rows[0]["_is_current"] is False
    assert all_rows[0]["_is_deleted"] is True


def test_scd2_new_key_in_a_later_batch_is_inserted_as_current(spark):
    table = make_table("dim5", "scd2")
    merge_scd2(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 1, "crm")

    merge_scd2(spark, table, spark.createDataFrame([(2, "Bob")], ["id", "name"]), 2, "crm")

    written = rows(spark.table(table.fqn))
    assert set(written) == {1, 2}
    assert written[2]["_is_current"] is True


# ------------------------------------------------------------ unknown_member


def test_seed_unknown_member_inserts_a_minus_one_row(spark):
    table = make_table("dim6", "scd2", unknown_member=True)
    merge_scd2(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 1, "crm")

    seed_unknown_member(spark, table, execution_id=1)

    written = rows(spark.table(table.fqn))
    assert -1 in written
    assert written[-1]["name"] is None


def test_seed_unknown_member_is_idempotent(spark):
    table = make_table("dim7", "scd2", unknown_member=True)
    merge_scd2(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 1, "crm")

    seed_unknown_member(spark, table, execution_id=1)
    seed_unknown_member(spark, table, execution_id=2)

    assert len(spark.table(table.fqn).filter("id = -1").collect()) == 1


@pytest.mark.parametrize(
    ("name", "ddl_type", "real_value", "expected_sentinel"),
    [
        ("dim_int", "INT", 1, -1),
        ("dim_bigint", "BIGINT", 1, -1),
        ("dim_dec", "DECIMAL(10,0)", decimal.Decimal(1), decimal.Decimal(-1)),
        ("dim_dbl", "DOUBLE", 1.0, -1.0),
        ("dim_str", "STRING", "DE", None),
        ("dim_date", "DATE", datetime.date(2026, 1, 1), datetime.date(1900, 1, 1)),
        (
            "dim_ts",
            "TIMESTAMP",
            datetime.datetime(2026, 1, 1),
            datetime.datetime(1900, 1, 1),
        ),
    ],
)
def test_seed_unknown_member_uses_a_sentinel_of_the_business_keys_type(
    spark, name, ddl_type, real_value, expected_sentinel
):
    table = make_table(name, "scd1", unknown_member=True)
    source = spark.createDataFrame([(real_value, "x")], f"id {ddl_type}, name STRING")
    merge_scd1(spark, table, source, 1, "crm")

    seed_unknown_member(spark, table, execution_id=1)
    seed_unknown_member(spark, table, execution_id=2)  # still idempotent

    keys = [r["id"] for r in spark.table(table.fqn).collect()]
    assert sorted(keys, key=str) == sorted([real_value, expected_sentinel], key=str)


def _unknown_rows(spark, fqn, key_sql):
    return spark.table(fqn).filter(key_sql).collect()


def test_string_unknown_member_is_null_and_seeding_twice_keeps_one_row(spark):
    table = make_table("dim_null1", "scd1", unknown_member=True)
    merge_scd1(spark, table, spark.createDataFrame([("DE", "x")], "id STRING, name STRING"), 1, "c")

    seed_unknown_member(spark, table, execution_id=1)
    seed_unknown_member(spark, table, execution_id=2)
    seed_unknown_member(spark, table, execution_id=3)

    unknown = _unknown_rows(spark, table.fqn, "id IS NULL")
    assert len(unknown) == 1
    assert unknown[0]["_execution_id"] == 1
    assert spark.table(table.fqn).count() == 2


def test_composite_key_unknown_member_uses_a_sentinel_per_column(spark):
    table = make_table("dim_comp1", "scd1", unknown_member=True, business_key=["id", "code"])
    merge_scd1(
        spark,
        table,
        spark.createDataFrame(
            [(1, "DE", DATE_2026, "x")], "id INT, code STRING, d DATE, name STRING"
        ),
        1,
        "c",
    )

    seed_unknown_member(spark, table, execution_id=1)
    seed_unknown_member(spark, table, execution_id=2)

    unknown = _unknown_rows(spark, table.fqn, "id = -1")
    assert len(unknown) == 1
    assert unknown[0]["code"] is None
    assert unknown[0]["d"] is None  # not a key column: left NULL


def test_composite_key_unknown_member_with_a_date_column(spark):
    table = make_table("dim_comp2", "scd1", unknown_member=True, business_key=["code", "d"])
    merge_scd1(
        spark,
        table,
        spark.createDataFrame([("DE", DATE_2026, "x")], "code STRING, d DATE, name STRING"),
        1,
        "c",
    )

    seed_unknown_member(spark, table, execution_id=1)
    seed_unknown_member(spark, table, execution_id=2)

    unknown = _unknown_rows(spark, table.fqn, "code IS NULL")
    assert len(unknown) == 1
    assert unknown[0]["d"] == datetime.date(1900, 1, 1)


def test_a_legacy_composite_unknown_member_is_recognised_as_already_seeded(spark):
    # Before per-column sentinels, a composite key got -1 in its first
    # column and NULL in the rest. Upgrading must not seed a second row.
    table = make_table("dim_comp3", "scd1", unknown_member=True, business_key=["id", "sub"])
    merge_scd1(
        spark, table, spark.createDataFrame([(1, 1, "x")], "id INT, sub INT, name STRING"), 1, "c"
    )
    spark.sql(
        f"INSERT INTO {table.fqn} "
        "SELECT -1, CAST(NULL AS INT), CAST(NULL AS STRING), 0L, 0L, false, 1L, "
        "CAST(NULL AS STRING)"
    )

    seed_unknown_member(spark, table, execution_id=2)

    assert len(_unknown_rows(spark, table.fqn, "id = -1")) == 1


def test_scd1_remerge_leaves_a_null_keyed_unknown_member_alone(spark):
    table = make_table("dim_null2", "scd1", unknown_member=True)
    merge_scd1(spark, table, spark.createDataFrame([("DE", "x")], "id STRING, name STRING"), 1, "c")
    seed_unknown_member(spark, table, execution_id=1)

    merge_scd1(spark, table, spark.createDataFrame([("DE", "y")], "id STRING, name STRING"), 2, "c")
    seed_unknown_member(spark, table, execution_id=2)

    written = spark.table(table.fqn).collect()
    assert len(written) == 2
    unknown = [r for r in written if r["id"] is None]
    assert len(unknown) == 1
    assert unknown[0]["_execution_id"] == 1  # not touched by the second merge
    assert [r["name"] for r in written if r["id"] == "DE"] == ["y"]


def test_scd2_remerge_does_not_version_a_null_keyed_unknown_member(spark):
    table = make_table("dim_null3", "scd2", unknown_member=True)
    merge_scd2(spark, table, spark.createDataFrame([("DE", "x")], "id STRING, name STRING"), 1, "c")
    seed_unknown_member(spark, table, execution_id=1)

    merge_scd2(spark, table, spark.createDataFrame([("DE", "y")], "id STRING, name STRING"), 2, "c")
    seed_unknown_member(spark, table, execution_id=2)

    unknown = _unknown_rows(spark, table.fqn, "id IS NULL")
    assert len(unknown) == 1
    assert unknown[0]["_is_current"] is True
    assert unknown[0]["_valid_to"] is None
    real = _unknown_rows(spark, table.fqn, "id = 'DE'")
    assert len(real) == 2  # the real row was versioned, as before


def test_seed_unknown_member_rejects_a_key_type_with_no_sentinel(spark):
    table = make_table("dim_bool", "scd1", unknown_member=True)
    merge_scd1(
        spark, table, spark.createDataFrame([(True, "x")], "id BOOLEAN, name STRING"), 1, "c"
    )

    with pytest.raises(ValueError, match="unknown_member.*boolean"):
        seed_unknown_member(spark, table, execution_id=1)


# ------------------------------------------------------------------- init


def test_scd1_init_replaces_the_table_instead_of_merging(spark):
    table = make_table("customer8", "scd1")
    merge_scd1(
        spark, table, spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"]), 1, "crm"
    )

    merge_scd1(
        spark, table, spark.createDataFrame([(3, "Carol")], ["id", "name"]), 2, "crm", init=True
    )

    assert set(rows(spark.table(table.fqn))) == {3}


def test_scd2_init_replaces_the_table_instead_of_merging(spark):
    table = make_table("dim8", "scd2")
    merge_scd2(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 1, "crm")

    merge_scd2(
        spark, table, spark.createDataFrame([(2, "Bob")], ["id", "name"]), 2, "crm", init=True
    )

    written = rows(spark.table(table.fqn))
    assert set(written) == {2}
    assert written[2]["_is_current"] is True


def test_append_init_overwrites_instead_of_appending(spark):
    table = make_table("events3", "append")
    merge_append(spark, table, spark.createDataFrame([(1, "click")], ["id", "kind"]), 1, "web")

    merge_append(
        spark, table, spark.createDataFrame([(2, "view")], ["id", "kind"]), 2, "web", init=True
    )

    assert set(rows(spark.table(table.fqn))) == {2}


def test_replace_accepts_the_init_flag_without_changing_behaviour(spark):
    table = make_table("orders2", "replace")

    result = merge_replace(
        spark, table, spark.createDataFrame([(1, "a")], ["id", "name"]), 1, "crm", init=True
    )

    assert result["rows_written"] == 1


# --------------------------------------------------------------- schema drift


def test_scd1_evolve_adds_a_new_column_and_records_the_drift(spark):
    table = make_table("customer9", "scd1")  # schema_policy defaults to evolve
    merge_scd1(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 1, "crm")

    result = merge_scd1(
        spark,
        table,
        spark.createDataFrame([(2, "Bob", "bob@example.com")], ["id", "name", "email"]),
        2,
        "crm",
    )

    written = rows(spark.table(table.fqn))
    assert written[2]["email"] == "bob@example.com"
    assert written[1]["email"] is None  # the existing row, untouched, gets a null
    assert any("added: email" in c for c in result["schema_changes"])


def test_scd2_evolve_adds_a_new_column_and_records_the_drift(spark):
    table = make_table("dim9", "scd2")
    merge_scd2(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 1, "crm")

    result = merge_scd2(
        spark,
        table,
        spark.createDataFrame([(2, "Bob", "bob@example.com")], ["id", "name", "email"]),
        2,
        "crm",
    )

    written = rows(spark.table(table.fqn))
    assert written[2]["email"] == "bob@example.com"
    assert any("added: email" in c for c in result["schema_changes"])


def test_fail_policy_raises_and_never_writes_on_any_drift(spark):
    table = make_table("customer10", "scd1", schema_policy="fail")
    merge_scd1(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 1, "crm")

    from pipetree.schema.errors import SchemaError

    with pytest.raises(SchemaError, match="added: email"):
        merge_scd1(
            spark,
            table,
            spark.createDataFrame([(2, "Bob", "bob@example.com")], ["id", "name", "email"]),
            2,
            "crm",
        )

    # the failed attempt must not have touched the table
    assert set(rows(spark.table(table.fqn))) == {1}


def test_ignore_policy_silently_drops_the_new_column(spark):
    table = make_table("customer11", "scd1", schema_policy="ignore")
    merge_scd1(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 1, "crm")

    result = merge_scd1(
        spark,
        table,
        spark.createDataFrame([(2, "Bob", "bob@example.com")], ["id", "name", "email"]),
        2,
        "crm",
    )

    written = rows(spark.table(table.fqn))
    assert "email" not in spark.table(table.fqn).columns
    assert written[2]["name"] == "Bob"
    assert len(result["schema_changes"]) == 1


def test_evolve_allows_a_safe_widening_without_failing(spark):
    table = make_table("customer12", "scd1")
    merge_scd1(
        spark,
        table,
        spark.createDataFrame([(1, "Alice")], ["id", "name"]).selectExpr(
            "cast(id as int) as id", "name"
        ),
        1,
        "crm",
    )

    # id arrives as bigint this time - a safe widening from int, not a failure
    result = merge_scd1(spark, table, spark.createDataFrame([(2, "Bob")], ["id", "name"]), 2, "crm")

    assert result["schema_changes"]
    assert set(rows(spark.table(table.fqn))) == {1, 2}


def test_evolve_rejects_an_unsafe_retype(spark):
    table = make_table("customer13", "scd1")
    merge_scd1(spark, table, spark.createDataFrame([(1, "Alice")], ["id", "name"]), 1, "crm")

    from pipetree.schema.errors import SchemaError

    with pytest.raises(SchemaError, match="unsafe retype"):
        merge_scd1(
            spark,
            table,
            spark.createDataFrame([("2", "Bob")], ["id", "name"]),  # id is now a string
            2,
            "crm",
        )


# ------------------------------------------------- reserved audit columns


def test_strip_reserved_columns_drops_only_exact_reserved_names_and_logs_them(spark, caplog):
    table = make_table("strip1", "scd1")
    source = spark.createDataFrame(
        [(1, "a", 5, True, "x", "__keep__")],
        ["id", "name", "_execution_id", "_is_current", "_note", "__pipetree_is_delete__"],
    )

    with caplog.at_level(logging.DEBUG, logger="pipetree.adapters.spark"):
        stripped = strip_reserved_columns(source, table)

    assert stripped.columns == ["id", "name", "_note", "__pipetree_is_delete__"]
    assert "test_scd1.strip1" in caplog.text
    assert "_execution_id, _is_current" in caplog.text


def test_strip_reserved_columns_is_a_noop_without_reserved_columns(spark, caplog):
    table = make_table("strip2", "scd1")
    source = spark.createDataFrame([(1, "a")], ["id", "name"])

    with caplog.at_level(logging.DEBUG, logger="pipetree.adapters.spark"):
        stripped = strip_reserved_columns(source, table)

    assert stripped is source
    assert caplog.text == ""


def test_drop_null_business_keys_drops_a_null_in_any_key_column(spark):
    table = make_table("dropnull1", "scd1", unknown_member=True, business_key=["id", "code"])
    source = spark.createDataFrame(
        [(1, "DE"), (2, None), (None, "FR"), (3, "IT")], "id INT, code STRING"
    )

    kept, dropped = drop_null_business_keys(source, table)

    assert dropped == 2
    assert sorted(r["id"] for r in kept.collect()) == [1, 3]


def test_drop_null_business_keys_is_a_noop_without_unknown_member(spark):
    table = make_table("dropnull2", "scd1")
    source = spark.createDataFrame([(None, "x")], "id STRING, name STRING")

    kept, dropped = drop_null_business_keys(source, table)

    assert kept is source
    assert dropped == 0


@pytest.mark.parametrize("strategy,merge_fn", [("scd1", merge_scd1), ("scd2", merge_scd2)])
@pytest.mark.parametrize("path", ["first_run", "init"])
def test_duplicates_are_warned_about_and_reported_on_the_seed_paths(
    spark, caplog, strategy, merge_fn, path
):
    table = make_table(f"dupseed_{path}", strategy, merge={"sequence_by": ["version"]})
    if path == "init":  # the table exists, init reloads it from scratch
        merge_fn(
            spark,
            table,
            spark.createDataFrame([(9, "old", 1)], ["id", "name", "version"]),
            1,
            "crm",
        )
    source = spark.createDataFrame([(1, "v1", 1), (1, "v2", 2)], ["id", "name", "version"])

    with caplog.at_level(logging.WARNING, logger="pipetree"):
        result = merge_fn(spark, table, source, 2, "crm", init=path == "init")

    assert result["duplicates_dropped"] == 1
    messages = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "dropped 1 duplicate row(s) for key (id) - kept the highest (version)" in m
        for m in messages
    )


# ------------------------------------------- delete_mode ignore (delete_mode ignore)
# `ignore` drops the source's delete signal entirely: a delete row behaves as if
# it were not in the batch (never inserted, never part of the duplicate
# resolution, never counted in `duplicates_dropped`).

_DEL_COLS = ["id", "name", "is_deleted_flag", "seq"]
_MERGE = {"scd1": merge_scd1, "scd2": merge_scd2}


def _del_table(name: str, strategy: str, mode: str) -> Table:
    return make_table(
        f"{name}_{strategy}",
        strategy,
        merge={
            "delete_when": "is_deleted_flag = true",
            "delete_mode": mode,
            "sequence_by": ["seq"],
        },
    )


def _del_run(spark, table, data, execution_id):
    df = spark.createDataFrame(data, _DEL_COLS)
    return _MERGE[table.strategy](spark, table, df, execution_id, "crm")


def _snapshot(spark, table):
    """Every stored row, with the audit columns that show whether it was touched."""
    cols = ["id", "name", "_is_deleted", "_execution_id"]
    if table.strategy == "scd2":
        cols.append("_is_current")
    return sorted(tuple(r[c] for c in cols) for r in spark.table(table.fqn).collect())


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_ignore_delete_row_for_an_existing_key_does_nothing(spark, strategy):
    table = _del_table("ign_existing", strategy, "ignore")
    _del_run(spark, table, [(1, "a", False, 1)], 1)
    before = _snapshot(spark, table)

    _del_run(spark, table, [(1, "a", True, 2)], 2)

    assert _snapshot(spark, table) == before


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_ignore_key_with_only_a_delete_row_is_not_inserted(spark, strategy):
    table = _del_table("ign_only", strategy, "ignore")

    # first run (seed path) ...
    _del_run(spark, table, [(1, "a", False, 1), (2, "gone", True, 1)], 1)
    assert [r[0] for r in _snapshot(spark, table)] == [1]

    # ... and a later run (merge path)
    _del_run(spark, table, [(3, "gone too", True, 2)], 2)
    assert [r[0] for r in _snapshot(spark, table)] == [1]


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
@pytest.mark.parametrize("seeded", [False, True])
def test_ignore_normal_row_wins_over_a_delete_row_with_a_higher_sequence(spark, strategy, seeded):
    table = _del_table(f"ign_seq{int(seeded)}", strategy, "ignore")
    if seeded:
        _del_run(spark, table, [(9, "other", False, 1)], 1)

    result = _del_run(spark, table, [(1, "normal", False, 1), (1, "deleted", True, 5)], 2)

    assert result["duplicates_dropped"] == 0
    assert [(r[0], r[1], r[2]) for r in _snapshot(spark, table) if r[0] == 1] == [
        (1, "normal", False)
    ]


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
@pytest.mark.parametrize("seeded", [False, True])
def test_ignore_only_normal_duplicates_are_counted(spark, strategy, seeded):
    table = _del_table(f"ign_dups{int(seeded)}", strategy, "ignore")
    if seeded:
        _del_run(spark, table, [(9, "other", False, 1)], 1)

    result = _del_run(
        spark,
        table,
        [(1, "v1", False, 1), (1, "v2", False, 2), (1, "deleted", True, 9)],
        2,
    )

    assert result["duplicates_dropped"] == 1
    assert [(r[0], r[1]) for r in _snapshot(spark, table) if r[0] == 1] == [(1, "v2")]


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_ignore_a_null_delete_when_result_keeps_the_row(spark, strategy):
    table = _del_table("ign_null", strategy, "ignore")

    _del_run(spark, table, [(1, "a", None, 1), (2, "b", False, 1)], 1)

    assert [(r[0], r[1]) for r in _snapshot(spark, table)] == [(1, "a"), (2, "b")]


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_soft_delete_row_with_the_higher_sequence_still_wins(spark, strategy):
    table = _del_table("soft_seq", strategy, "soft")
    _del_run(spark, table, [(1, "a", False, 1)], 1)

    result = _del_run(spark, table, [(1, "a", False, 2), (1, "a", True, 5)], 2)

    assert result["duplicates_dropped"] == 1
    assert [r[2] for r in _snapshot(spark, table)] == [True]


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_hard_delete_row_with_the_higher_sequence_still_wins(spark, strategy):
    table = _del_table("hard_seq", strategy, "hard")
    _del_run(spark, table, [(1, "a", False, 1)], 1)

    result = _del_run(spark, table, [(1, "a", False, 2), (1, "a", True, 5)], 2)

    assert result["duplicates_dropped"] == 1
    if strategy == "scd1":
        assert _snapshot(spark, table) == []
    else:
        # scd2 hard delete: the history is kept, closed
        assert [(r[0], r[4]) for r in _snapshot(spark, table)] == [(1, False)]
