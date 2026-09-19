"""The four merge strategies, against Delta: scd1, scd2, replace, append.

scd1/scd2 use Delta's merge builder. Delta's MERGE can only take one action
per matched row (update *or* delete, never "close this row and also insert
its replacement"), so scd2 runs in two passes: a merge that closes changed
or deleted current versions (and inserts brand-new keys), then a plain
INSERT of the new version for every key that actually changed - determined
from the *pre-merge* snapshot, materialized before the merge runs so it
isn't re-evaluated against the table the merge just mutated.
"""

from __future__ import annotations

import operator
from functools import reduce
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from pipetree.adapters.spark.audit import (
    insert_audit_values,
    scd2_close_values,
    scd2_insert_audit_values,
    update_audit_values,
)
from pipetree.adapters.spark.dedupe import dedupe_for_merge
from pipetree.executor.execution_id import timestamp_bigint
from pipetree.model import Table

_IS_DELETE_COL = "__pipetree_is_delete__"


def merge_replace(
    spark: SparkSession,
    table: Table,
    source: DataFrame,
    execution_id: int,
    source_system: str | None,
) -> dict:
    _ensure_schema(spark, table.fqn)
    now = timestamp_bigint()

    stamped = _with_values(source, insert_audit_values(execution_id, source_system, now))
    row_count = stamped.count()
    stamped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        table.fqn
    )
    return {"rows_written": row_count}


def merge_append(
    spark: SparkSession,
    table: Table,
    source: DataFrame,
    execution_id: int,
    source_system: str | None,
) -> dict:
    _ensure_schema(spark, table.fqn)
    now = timestamp_bigint()

    stamped = _with_values(source, insert_audit_values(execution_id, source_system, now))
    row_count = stamped.count()
    mode = "append" if _table_exists(spark, table.fqn) else "overwrite"
    stamped.write.format("delta").mode(mode).saveAsTable(table.fqn)
    return {"rows_written": row_count}


def merge_scd1(
    spark: SparkSession,
    table: Table,
    source: DataFrame,
    execution_id: int,
    source_system: str | None,
) -> dict:
    _ensure_schema(spark, table.fqn)
    now = timestamp_bigint()
    deduped, duplicates_dropped = dedupe_for_merge(source, table)
    prepared = _prepare_source(deduped, table)
    columns = deduped.columns

    if not _table_exists(spark, table.fqn):
        seed = prepared.filter(~F.col(_IS_DELETE_COL)).drop(_IS_DELETE_COL)
        stamped = _with_values(seed, insert_audit_values(execution_id, source_system, now))
        row_count = stamped.count()
        stamped.write.format("delta").mode("overwrite").saveAsTable(table.fqn)
        return {"rows_written": row_count, "duplicates_dropped": duplicates_dropped}

    key_cond = _key_condition(table)
    change_cond = _change_condition_sql(columns, table)
    insert_values = _column_mapping(columns, insert_audit_values(execution_id, source_system, now))
    update_values = _column_mapping(columns, update_audit_values(execution_id, source_system, now))

    delta_table = DeltaTable.forName(spark, table.fqn)
    merge_builder = delta_table.alias("target").merge(prepared.alias("source"), key_cond)

    if table.merge.delete_mode == "soft":
        soft_delete_values: dict[str, str | Column] = {
            **update_audit_values(execution_id, source_system, now),
            "_is_deleted": F.lit(True),
        }
        merge_builder = merge_builder.whenMatchedUpdate(
            condition=f"source.{_IS_DELETE_COL} = true", set=soft_delete_values
        )
    elif table.merge.delete_mode == "hard":
        merge_builder = merge_builder.whenMatchedDelete(condition=f"source.{_IS_DELETE_COL} = true")

    row_count = prepared.count()
    (
        merge_builder.whenMatchedUpdate(
            condition=f"source.{_IS_DELETE_COL} = false AND ({change_cond})", set=update_values
        )
        .whenNotMatchedInsert(condition=f"source.{_IS_DELETE_COL} = false", values=insert_values)
        .execute()
    )

    return {"rows_written": row_count, "duplicates_dropped": duplicates_dropped}


def merge_scd2(
    spark: SparkSession,
    table: Table,
    source: DataFrame,
    execution_id: int,
    source_system: str | None,
) -> dict:
    _ensure_schema(spark, table.fqn)
    now = timestamp_bigint()
    deduped, duplicates_dropped = dedupe_for_merge(source, table)
    prepared = _prepare_source(deduped, table)
    columns = deduped.columns

    if not _table_exists(spark, table.fqn):
        seed = prepared.filter(~F.col(_IS_DELETE_COL)).drop(_IS_DELETE_COL)
        stamped = _with_values(seed, scd2_insert_audit_values(execution_id, source_system, now))
        row_count = stamped.count()
        stamped.write.format("delta").mode("overwrite").saveAsTable(table.fqn)
        return {"rows_written": row_count, "duplicates_dropped": duplicates_dropped}

    not_deleted = prepared.filter(~F.col(_IS_DELETE_COL)).drop(_IS_DELETE_COL)
    current_before = spark.table(table.fqn).filter("_is_current = true")
    # Materialized now, before the merge below mutates the table this reads
    # from - otherwise a lazy re-evaluation after the merge would compare
    # against the already-closed rows instead of the pre-merge snapshot.
    # Pulled to the driver, not just cached: Delta's merge below writes to
    # the same table this reads from, and a write to a cataloged table
    # invalidates Spark's DataFrame cache for it - a merely .cache()'d plan
    # would silently recompute against the post-merge (already-closed) rows
    # the moment it's touched again. Demo-scale data only; a real dimension
    # table would need a different approach (see NOTES-for-blog.md).
    new_version_rows = _rows_that_changed(not_deleted, current_before, table, columns).collect()
    new_version_count = len(new_version_rows)
    new_versions_schema = not_deleted.select(*columns).schema

    key_cond = _key_condition(table) + " AND target._is_current = true"
    change_cond = _change_condition_sql(columns, table)
    close_for_delete: dict[str, str | Column] = {
        **update_audit_values(execution_id, source_system, now),
        **scd2_close_values(now),
        "_is_deleted": F.lit(True),
    }
    close_for_change: dict[str, str | Column] = {
        **update_audit_values(execution_id, source_system, now),
        **scd2_close_values(now),
    }
    insert_new_key_values = _column_mapping(
        columns, scd2_insert_audit_values(execution_id, source_system, now)
    )

    row_count = prepared.count()
    delta_table = DeltaTable.forName(spark, table.fqn)
    (
        delta_table.alias("target")
        .merge(prepared.alias("source"), key_cond)
        .whenMatchedUpdate(condition=f"source.{_IS_DELETE_COL} = true", set=close_for_delete)
        .whenMatchedUpdate(
            condition=f"source.{_IS_DELETE_COL} = false AND ({change_cond})", set=close_for_change
        )
        .whenNotMatchedInsert(
            condition=f"source.{_IS_DELETE_COL} = false", values=insert_new_key_values
        )
        .execute()
    )

    if new_version_count > 0:
        new_versions = spark.createDataFrame(new_version_rows, schema=new_versions_schema)
        stamped_new_versions = _with_values(
            new_versions, scd2_insert_audit_values(execution_id, source_system, now)
        )
        stamped_new_versions.write.format("delta").mode("append").saveAsTable(table.fqn)
        row_count += new_version_count

    return {"rows_written": row_count, "duplicates_dropped": duplicates_dropped}


def seed_unknown_member(spark: SparkSession, table: Table, execution_id: int) -> None:
    """Seed the default -1 unknown member so facts can resolve a missing
    foreign key to it instead of leaving a NULL. A no-op if the table
    doesn't ask for it, doesn't exist yet, or already has one."""
    if not table.unknown_member or not table.business_key:
        return
    if not _table_exists(spark, table.fqn):
        return

    key_col = table.business_key[0]
    already_seeded = spark.table(table.fqn).filter(F.col(key_col) == -1).limit(1).count() > 0
    if already_seeded:
        return

    schema = spark.table(table.fqn).schema
    now = timestamp_bigint()
    values: dict[str, Any] = dict.fromkeys(f.name for f in schema.fields)
    values[key_col] = -1
    values.update(
        {
            "_inserted_at": now,
            "_updated_at": now,
            "_is_deleted": False,
            "_execution_id": execution_id,
        }
    )
    if "_is_current" in values:
        values["_is_current"] = True
    if "_valid_from" in values:
        values["_valid_from"] = now

    row = Row(**{f.name: values[f.name] for f in schema.fields})
    spark.createDataFrame([row], schema=schema).write.format("delta").mode("append").saveAsTable(
        table.fqn
    )


def _ensure_schema(spark: SparkSession, fqn: str) -> None:
    schema = fqn.split(".")[0]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def _table_exists(spark: SparkSession, fqn: str) -> bool:
    return spark.catalog.tableExists(fqn)


def _with_values(df: DataFrame, values: dict) -> DataFrame:
    return df.select("*", *[value.alias(name) for name, value in values.items()])


def _column_mapping(
    source_columns: list, audit_values: dict[str, Column]
) -> dict[str, str | Column]:
    """`{col: "source.col", ...}` for every business column, plus the given
    audit columns - built as one literal so the merged dict's value type is
    `str | Column` throughout, matching what Delta's merge builder expects."""
    return {**{c: f"source.{c}" for c in source_columns}, **audit_values}


def _prepare_source(df: DataFrame, table: Table) -> DataFrame:
    if table.merge.delete_mode == "ignore" or not table.merge.delete_when:
        return df.withColumn(_IS_DELETE_COL, F.lit(False))
    return df.withColumn(_IS_DELETE_COL, F.expr(table.merge.delete_when))


def _key_condition(table: Table) -> str:
    return " AND ".join(f"target.{k} = source.{k}" for k in table.business_key)


def _comparable_columns(columns: list, table: Table) -> list:
    return [
        c for c in columns if c not in table.business_key and c not in table.merge.ignore_columns
    ]


def _change_condition_sql(columns: list, table: Table) -> str:
    compare_cols = _comparable_columns(columns, table)
    if not compare_cols:
        return "true"
    same = " AND ".join(f"target.{c} <=> source.{c}" for c in compare_cols)
    return f"NOT ({same})"


def _rows_that_changed(
    source_df: DataFrame, current_df: DataFrame, table: Table, columns: list
) -> DataFrame:
    """Source rows whose business columns differ from their current target
    row - used to decide which keys need a new scd2 version opened."""
    compare_cols = _comparable_columns(columns, table)
    join_cond = [source_df[k] == current_df[k] for k in table.business_key]
    joined = source_df.join(current_df, join_cond, "inner")

    if not compare_cols:
        return joined.select(*[source_df[c] for c in columns])

    conditions = [~source_df[col].eqNullSafe(current_df[col]) for col in compare_cols]
    diff = reduce(operator.or_, conditions)

    return joined.filter(diff).select(*[source_df[c] for c in columns])
