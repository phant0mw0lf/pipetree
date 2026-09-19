import pytest

from pipetree.adapters.spark.audit import (
    insert_audit_values,
    scd2_close_values,
    scd2_insert_audit_values,
    update_audit_values,
)

pytestmark = pytest.mark.spark


def apply(spark, values: dict) -> dict:
    df = spark.range(1).select(*[col.alias(name) for name, col in values.items()])
    return df.collect()[0].asDict()


def test_insert_audit_values_stamps_all_five_lineage_columns(spark):
    row = apply(spark, insert_audit_values(execution_id=1, source_system="crm", now=100))

    assert row == {
        "_inserted_at": 100,
        "_updated_at": 100,
        "_is_deleted": False,
        "_execution_id": 1,
        "_source_system": "crm",
    }


def test_update_audit_values_only_touches_updated_at_execution_id_and_source(spark):
    values = update_audit_values(execution_id=2, source_system="crm", now=200)

    assert set(values) == {"_updated_at", "_execution_id", "_source_system"}
    row = apply(spark, values)
    assert row == {"_updated_at": 200, "_execution_id": 2, "_source_system": "crm"}


def test_scd2_insert_audit_values_adds_valid_from_to_and_is_current(spark):
    values = scd2_insert_audit_values(execution_id=1, source_system="crm", now=100)
    row = apply(spark, values)

    assert row["_valid_from"] == 100
    assert row["_valid_to"] is None
    assert row["_is_current"] is True
    # still a superset of the plain insert columns
    assert row["_inserted_at"] == 100
    assert row["_is_deleted"] is False


def test_scd2_close_values_only_touches_valid_to_and_is_current(spark):
    values = scd2_close_values(now=300)

    assert set(values) == {"_valid_to", "_is_current"}
    row = apply(spark, values)
    assert row == {"_valid_to": 300, "_is_current": False}
