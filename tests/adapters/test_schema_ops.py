"""ensure_schema creates a schema only when the catalog does not have it yet.

Unity Catalog rejects CREATE SCHEMA IF NOT EXISTS from a principal without CREATE SCHEMA on
the catalog even when the schema exists - a writer with table privileges on a pre-created
schema must therefore never issue the statement for an existing schema.
"""

from pipetree.adapters.spark.schema_ops import ensure_schema


class _Catalog:
    def __init__(self, existing: set[str]) -> None:
        self._existing = existing

    def databaseExists(self, name: str) -> bool:  # noqa: N802 - PySpark API name
        return name in self._existing


class _FakeSpark:
    def __init__(self, existing: set[str]) -> None:
        self.catalog = _Catalog(existing)
        self.statements: list[str] = []

    def sql(self, statement: str) -> None:
        self.statements.append(statement)


def test_existing_schema_is_left_alone():
    spark = _FakeSpark({"silver"})

    ensure_schema(spark, "silver")  # type: ignore[arg-type]

    assert spark.statements == []


def test_missing_schema_is_created():
    spark = _FakeSpark(set())

    ensure_schema(spark, "silver")  # type: ignore[arg-type]

    assert spark.statements == ["CREATE SCHEMA IF NOT EXISTS silver"]
