import logging

import pytest

from pipetree.adapters.spark.dedupe import dedupe_for_merge
from pipetree.model import Merge, Table

pytestmark = pytest.mark.spark


def make_table(business_key: list[str], sequence_by: list[str] | None = None) -> Table:
    return Table(
        name="customer",
        layer="bronze",
        table_schema="bronze",
        fqn="bronze.customer",
        strategy="scd1",
        business_key=business_key,
        merge=Merge(sequence_by=sequence_by or []),
    )


def test_dedupe_keeps_the_row_with_the_highest_sequence_by_value(spark):
    df = spark.createDataFrame(
        [(1, "v1", 1), (1, "v3", 3), (1, "v2", 2), (2, "only", 1)],
        ["id", "name", "version"],
    )
    table = make_table(business_key=["id"], sequence_by=["version"])

    deduped, dropped = dedupe_for_merge(df, table)

    rows = {r["id"]: r["name"] for r in deduped.collect()}
    assert rows == {1: "v3", 2: "only"}
    assert dropped == 2


def test_dedupe_is_a_noop_when_there_are_no_duplicates(spark):
    df = spark.createDataFrame([(1, "a", 1), (2, "b", 1)], ["id", "name", "version"])
    table = make_table(business_key=["id"], sequence_by=["version"])

    deduped, dropped = dedupe_for_merge(df, table)

    assert deduped.count() == 2
    assert dropped == 0


def test_dedupe_without_sequence_by_still_returns_exactly_one_row_per_key(spark):
    df = spark.createDataFrame([(1, "a"), (1, "b"), (1, "c")], ["id", "name"])
    table = make_table(business_key=["id"], sequence_by=None)

    deduped, dropped = dedupe_for_merge(df, table)

    assert deduped.count() == 1
    assert dropped == 2


def test_dedupe_supports_composite_business_keys(spark):
    df = spark.createDataFrame(
        [(1, "a", "v1", 1), (1, "a", "v2", 2), (1, "b", "v1", 1)],
        ["tenant", "id", "name", "version"],
    )
    table = make_table(business_key=["tenant", "id"], sequence_by=["version"])

    deduped, dropped = dedupe_for_merge(df, table)

    assert deduped.count() == 2
    assert dropped == 1


def test_dedupe_without_business_key_returns_input_unchanged(spark):
    df = spark.createDataFrame([(1, "a"), (1, "a")], ["id", "name"])
    table = make_table(business_key=[])

    deduped, dropped = dedupe_for_merge(df, table)

    assert deduped.count() == 2
    assert dropped == 0


def test_dedupe_warns_with_the_count_key_and_sequence_by(spark, caplog):
    df = spark.createDataFrame([(1, "a", 1), (1, "b", 2), (2, "c", 1)], ["id", "name", "v"])
    table = make_table(business_key=["id"], sequence_by=["v"])

    with caplog.at_level(logging.WARNING, logger="pipetree"):
        dedupe_for_merge(df, table)

    [record] = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert record.getMessage() == (
        "bronze.customer: dropped 1 duplicate row(s) for key (id) - kept the highest (v)"
    )


def test_dedupe_warning_says_the_winner_is_arbitrary_without_sequence_by(spark, caplog):
    df = spark.createDataFrame([(1, 1, "a"), (1, 1, "b")], ["tenant", "id", "name"])
    table = make_table(business_key=["tenant", "id"], sequence_by=None)

    with caplog.at_level(logging.WARNING, logger="pipetree"):
        dedupe_for_merge(df, table)

    [record] = [r for r in caplog.records if r.levelno == logging.WARNING]
    message = record.getMessage()
    assert "dropped 1 duplicate row(s) for key (tenant, id)" in message
    assert message.endswith("; no sequence_by is declared, so which duplicate won is arbitrary")


def test_dedupe_does_not_log_when_nothing_was_dropped(spark, caplog):
    df = spark.createDataFrame([(1, "a", 1), (2, "b", 1)], ["id", "name", "v"])

    with caplog.at_level(logging.DEBUG, logger="pipetree"):
        dedupe_for_merge(df, make_table(business_key=["id"], sequence_by=["v"]))

    assert not caplog.records
