"""No JVM needed: reconcile() operates on plain ColumnSchema dicts."""

import pytest

from pipetree.schema.diff import SchemaChange
from pipetree.schema.errors import SchemaError
from pipetree.schema.infer import ColumnSchema
from pipetree.schema.policy import is_safe_widening, reconcile


def col(name: str, data_type: str, nullable: bool = True) -> ColumnSchema:
    return ColumnSchema(name, data_type, nullable)


def test_first_write_has_nothing_to_reconcile_against():
    result = reconcile(
        source_columns=["id", "name"],
        source_schema={"id": col("id", "bigint"), "name": col("name", "string")},
        target_schema=None,
        policy="fail",
        table_fqn="bronze.customer",
    )

    assert result.columns == ["id", "name"]
    assert result.changes == []
    assert result.needs_schema_evolution is False


def test_no_drift_is_a_no_op_under_any_policy():
    schema = {"id": col("id", "bigint")}

    for policy in ("evolve", "fail", "ignore"):
        result = reconcile(["id"], schema, schema, policy, "bronze.customer")
        assert result.columns == ["id"]
        assert result.changes == []


def test_fail_policy_raises_on_any_drift():
    with pytest.raises(SchemaError, match="bronze.customer"):
        reconcile(
            source_columns=["id", "email"],
            source_schema={"id": col("id", "bigint"), "email": col("email", "string")},
            target_schema={"id": col("id", "bigint")},
            policy="fail",
            table_fqn="bronze.customer",
        )


def test_ignore_policy_keeps_only_the_intersection():
    result = reconcile(
        source_columns=["id", "email"],
        source_schema={"id": col("id", "bigint"), "email": col("email", "string")},
        target_schema={"id": col("id", "bigint")},
        policy="ignore",
        table_fqn="bronze.customer",
    )

    assert result.columns == ["id"]
    assert len(result.changes) == 1
    assert result.needs_schema_evolution is False


def test_evolve_allows_a_new_column_and_flags_schema_evolution_needed():
    result = reconcile(
        source_columns=["id", "email"],
        source_schema={"id": col("id", "bigint"), "email": col("email", "string")},
        target_schema={"id": col("id", "bigint")},
        policy="evolve",
        table_fqn="bronze.customer",
    )

    assert result.columns == ["id", "email"]
    assert result.needs_schema_evolution is True


def test_evolve_allows_a_safe_widening():
    result = reconcile(
        source_columns=["id"],
        source_schema={"id": col("id", "bigint")},
        target_schema={"id": col("id", "int")},
        policy="evolve",
        table_fqn="bronze.customer",
    )

    assert result.columns == ["id"]
    assert result.needs_schema_evolution is False  # a retype, not an add


def test_evolve_rejects_an_unsafe_retype():
    with pytest.raises(SchemaError, match="unsafe retype"):
        reconcile(
            source_columns=["id"],
            source_schema={"id": col("id", "string")},
            target_schema={"id": col("id", "bigint")},
            policy="evolve",
            table_fqn="bronze.customer",
        )


@pytest.mark.parametrize(
    "old_type,new_type",
    [
        ("tinyint", "smallint"),
        ("tinyint", "bigint"),
        ("smallint", "int"),
        ("int", "bigint"),
        ("int", "double"),
        ("bigint", "double"),
        ("float", "double"),
    ],
)
def test_is_safe_widening_recognizes_the_numeric_hierarchy(old_type, new_type):
    change = SchemaChange("retyped", "x", old_type=old_type, new_type=new_type)
    assert is_safe_widening(change)


@pytest.mark.parametrize(
    "old_type,new_type",
    [
        ("bigint", "int"),  # narrowing
        ("double", "float"),  # narrowing
        ("string", "bigint"),  # unrelated types
        ("bigint", "string"),
    ],
)
def test_is_safe_widening_rejects_narrowing_and_unrelated_types(old_type, new_type):
    change = SchemaChange("retyped", "x", old_type=old_type, new_type=new_type)
    assert not is_safe_widening(change)
