"""Schema (database) existence for the Spark adapter.

Unity Catalog checks the CREATE SCHEMA privilege before it checks whether the schema
exists, so an unconditional ``CREATE SCHEMA IF NOT EXISTS`` fails for a principal that
may only write tables into a pre-created schema. The schema is therefore created only
when the catalog does not know it yet.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def ensure_schema(spark: SparkSession, schema: str) -> None:
    """Create ``schema`` unless it already exists (no privilege check on an existing schema)."""
    if not spark.catalog.databaseExists(schema):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
