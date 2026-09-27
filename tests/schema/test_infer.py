import pytest

from pipetree.schema.infer import ColumnSchema, infer_schema

pytestmark = pytest.mark.spark


def test_infers_name_type_and_nullability(spark):
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

    schema = infer_schema(df)

    assert schema["id"] == ColumnSchema(name="id", data_type="bigint", nullable=True)
    assert schema["name"] == ColumnSchema(name="name", data_type="string", nullable=True)


def test_excludes_the_given_column_names(spark):
    df = spark.createDataFrame([(1, "Alice", 100)], ["id", "name", "_execution_id"])

    schema = infer_schema(df, exclude=frozenset({"_execution_id"}))

    assert set(schema) == {"id", "name"}


def test_real_spark_type_names_are_not_pythons(spark):
    # A real gotcha: Spark's simpleString() is "bigint"/"tinyint"/"smallint",
    # not "long"/"byte"/"short" - this is what schema/policy.py's safe
    # widening table has to match.
    from pyspark.sql import Row
    from pyspark.sql.types import LongType, StructField, StructType

    df = spark.createDataFrame(
        [Row(id=1)], schema=StructType([StructField("id", LongType(), True)])
    )

    assert infer_schema(df)["id"].data_type == "bigint"
