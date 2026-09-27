from typing import Any

import pytest

from pipetree.adapters.spark.merge import (
    merge_append,
    merge_replace,
    merge_scd1,
    merge_scd2,
    seed_unknown_member,
)
from pipetree.model import Table

pytestmark = pytest.mark.spark


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
