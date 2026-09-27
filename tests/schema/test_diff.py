"""No JVM needed: diff_schemas operates on plain ColumnSchema dicts."""

from pipetree.schema.diff import SchemaChange, diff_schemas
from pipetree.schema.infer import ColumnSchema


def col(name: str, data_type: str, nullable: bool = True) -> ColumnSchema:
    return ColumnSchema(name, data_type, nullable)


def test_no_changes_when_schemas_match():
    schema = {"id": col("id", "bigint"), "name": col("name", "string")}

    assert diff_schemas(schema, schema) == []


def test_detects_an_added_column():
    source = {"id": col("id", "bigint"), "email": col("email", "string")}
    target = {"id": col("id", "bigint")}

    changes = diff_schemas(source, target)

    assert changes == [SchemaChange("added", "email", new_type="string")]


def test_detects_a_removed_column():
    source = {"id": col("id", "bigint")}
    target = {"id": col("id", "bigint"), "email": col("email", "string")}

    changes = diff_schemas(source, target)

    assert changes == [SchemaChange("removed", "email", old_type="string")]


def test_detects_a_retyped_column():
    source = {"id": col("id", "bigint")}
    target = {"id": col("id", "int")}

    changes = diff_schemas(source, target)

    assert changes == [SchemaChange("retyped", "id", old_type="int", new_type="bigint")]


def test_detects_a_nullability_change():
    source = {"id": col("id", "bigint", nullable=True)}
    target = {"id": col("id", "bigint", nullable=False)}

    changes = diff_schemas(source, target)

    assert changes == [SchemaChange("nullability", "id", old_nullable=False, new_nullable=True)]


def test_retype_takes_priority_over_nullability_for_the_same_column():
    # a column can't sensibly report both in one diff pass
    source = {"id": col("id", "bigint", nullable=True)}
    target = {"id": col("id", "int", nullable=False)}

    changes = diff_schemas(source, target)

    assert len(changes) == 1
    assert changes[0].kind == "retyped"


def test_multiple_changes_are_sorted_by_column_name():
    source = {"b": col("b", "string"), "a": col("a", "string")}
    target = {}

    changes = diff_schemas(source, target)

    assert [c.column for c in changes] == ["a", "b"]


def test_schema_change_string_formatting():
    assert str(SchemaChange("added", "email", new_type="string")) == "added: email (string)"
    assert str(SchemaChange("removed", "email", old_type="string")) == "removed: email (string)"
    assert str(SchemaChange("retyped", "id", old_type="int", new_type="bigint")) == (
        "retyped: id (int -> bigint)"
    )
    assert (
        str(SchemaChange("nullability", "id", old_nullable=False, new_nullable=True))
        == "nullability changed: id (False -> True)"
    )
