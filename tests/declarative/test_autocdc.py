"""No Databricks needed: translate_table/render_sql are pure functions.
AUTO CDC only runs inside a Lakeflow Declarative Pipeline, so there's
nothing here to execute against - only something to compile to."""

from typing import Any

import pytest

from pipetree.declarative.autocdc import AutoCdcFlow, render_sql, translate_table
from pipetree.model import Table


def make_table(strategy: str, **overrides) -> Table:
    defaults = {
        "name": "customer",
        "layer": "bronze",
        "table_schema": "bronze",
        "fqn": "bronze.customer",
        "strategy": strategy,
        "business_key": ["accountid"],
    }
    defaults.update(overrides)
    return Table.model_validate(defaults)


# ---------------------------------------------------------------- translate


def test_maps_business_key_to_keys():
    table = make_table("scd1", business_key=["accountid", "region"])

    flow = translate_table(table, source="bronze_raw.customer")

    assert flow.keys == ["accountid", "region"]


def test_maps_sequence_by():
    table = make_table("scd2", merge={"sequence_by": ["versionnumber"]})

    flow = translate_table(table, source="bronze_raw.customer")

    assert flow.sequence_by == ["versionnumber"]


def test_maps_delete_when_to_apply_as_delete_when():
    table = make_table("scd2", merge={"delete_when": "isdelete = true"})

    flow = translate_table(table, source="bronze_raw.customer")

    assert flow.apply_as_delete_when == "isdelete = true"


def test_delete_mode_ignore_drops_the_delete_signal():
    table = make_table("scd1", merge={"delete_when": "isdelete = true", "delete_mode": "ignore"})

    flow = translate_table(table, source="bronze_raw.customer")

    assert flow.apply_as_delete_when is None


def test_scd1_sets_stored_as_scd_type_1():
    flow = translate_table(make_table("scd1"), source="bronze_raw.customer")
    assert flow.stored_as_scd_type == 1


def test_scd2_sets_stored_as_scd_type_2():
    flow = translate_table(make_table("scd2"), source="bronze_raw.customer")
    assert flow.stored_as_scd_type == 2


def test_maps_ignore_columns_to_track_history_except_columns():
    table = make_table("scd2", merge={"ignore_columns": ["modifiedon"]})

    flow = translate_table(table, source="bronze_raw.customer")

    assert flow.track_history_except_columns == ["modifiedon"]


def test_target_and_source_are_carried_through():
    flow = translate_table(make_table("scd1"), source="bronze_raw.customer")

    assert flow.target == "bronze.customer"
    assert flow.source == "bronze_raw.customer"


@pytest.mark.parametrize("strategy", ["replace", "append"])
def test_rejects_strategies_that_need_no_cdc_apparatus(strategy):
    table = make_table(strategy)

    with pytest.raises(ValueError, match=strategy):
        translate_table(table, source="bronze_raw.customer")


def test_requires_a_business_key():
    table = make_table("scd1", business_key=[])

    with pytest.raises(ValueError, match="business_key"):
        translate_table(table, source="bronze_raw.customer")


# -------------------------------------------------------------------- render


def make_flow(**overrides: Any) -> AutoCdcFlow:
    defaults: dict[str, Any] = {
        "target": "bronze.customer",
        "source": "bronze_raw.customer",
        "keys": ["accountid"],
        "sequence_by": [],
        "apply_as_delete_when": None,
        "stored_as_scd_type": 1,
        "track_history_except_columns": [],
    }
    defaults.update(overrides)
    return AutoCdcFlow(**defaults)


def test_render_sql_declares_the_streaming_table_and_the_flow():
    sql = render_sql(make_flow())

    assert "CREATE OR REFRESH STREAMING TABLE bronze.customer;" in sql
    assert "CREATE FLOW" in sql
    assert "AS AUTO CDC INTO" in sql
    assert "bronze.customer" in sql


def test_render_sql_includes_the_from_and_keys_clauses():
    sql = render_sql(make_flow(keys=["accountid", "region"]))

    assert "FROM\n  bronze_raw.customer" in sql
    assert "KEYS\n  (accountid, region)" in sql


def test_render_sql_includes_apply_as_delete_when_if_present():
    sql = render_sql(make_flow(apply_as_delete_when="isdelete = true"))

    assert "APPLY AS DELETE WHEN\n  isdelete = true" in sql


def test_render_sql_omits_apply_as_delete_when_if_absent():
    sql = render_sql(make_flow(apply_as_delete_when=None))

    assert "APPLY AS DELETE WHEN" not in sql


def test_render_sql_includes_sequence_by_if_present():
    sql = render_sql(make_flow(sequence_by=["versionnumber"]))

    assert "SEQUENCE BY\n  versionnumber" in sql


def test_render_sql_omits_sequence_by_if_absent():
    sql = render_sql(make_flow(sequence_by=[]))

    assert "SEQUENCE BY" not in sql


def test_render_sql_includes_stored_as_scd_type():
    assert "STORED AS SCD TYPE 2" in render_sql(make_flow(stored_as_scd_type=2))
    assert "STORED AS SCD TYPE 1" in render_sql(make_flow(stored_as_scd_type=1))


def test_render_sql_includes_track_history_for_scd2_with_ignore_columns():
    sql = render_sql(make_flow(stored_as_scd_type=2, track_history_except_columns=["modifiedon"]))

    assert "TRACK HISTORY ON * EXCEPT (modifiedon)" in sql


def test_render_sql_omits_track_history_for_scd1_even_with_ignore_columns():
    # scd1 has no history to track - the field is meaningless there even
    # if a caller (incorrectly) populated it.
    sql = render_sql(make_flow(stored_as_scd_type=1, track_history_except_columns=["modifiedon"]))

    assert "TRACK HISTORY" not in sql


def test_render_sql_omits_track_history_for_scd2_without_ignore_columns():
    sql = render_sql(make_flow(stored_as_scd_type=2, track_history_except_columns=[]))

    assert "TRACK HISTORY" not in sql


def test_default_flow_name_is_derived_from_the_target():
    sql = render_sql(make_flow(target="gold.dim_customer"))

    assert "CREATE FLOW gold_dim_customer_flow AS AUTO CDC INTO" in sql


def test_custom_flow_name_is_used_when_given():
    sql = render_sql(make_flow(), flow_name="my_custom_flow")

    assert "CREATE FLOW my_custom_flow AS AUTO CDC INTO" in sql
