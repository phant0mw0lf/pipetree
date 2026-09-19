import pytest

pytestmark = pytest.mark.spark


def test_local_session_can_run_sql(spark):
    rows = spark.sql("SELECT 1 AS ok").collect()

    assert rows[0]["ok"] == 1


def test_local_session_has_delta_configured(spark, tmp_path):
    path = str(tmp_path / "roundtrip")

    spark.range(3).write.format("delta").mode("overwrite").save(path)
    result = spark.read.format("delta").load(path)

    assert result.count() == 3
