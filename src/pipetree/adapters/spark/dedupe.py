"""Pre-merge deduplication: a source batch with several rows per
`business_key` is deduped before an scd1/scd2 merge (Delta's MERGE errors
on multi-matches otherwise). This is info, not failure - the caller logs
`duplicates_dropped` and keeps going; without `sequence_by` the winner is
deterministic but arbitrary, which the caller logs as a warning."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipetree.model import Table


def dedupe_for_merge(df: DataFrame, table: Table) -> tuple[DataFrame, int]:
    if not table.business_key:
        return df, 0

    total_before = df.count()

    if table.merge.sequence_by:
        order_cols = [F.col(c).desc_nulls_last() for c in table.merge.sequence_by]
    else:
        # No tie-breaker declared: still pick exactly one row per key,
        # deterministic for a given input but not meaningful.
        order_cols = [F.monotonically_increasing_id().desc()]

    window = Window.partitionBy(*table.business_key).orderBy(*order_cols)
    rank_col = "__pipetree_dedupe_rank__"

    deduped = (
        df.withColumn(rank_col, F.row_number().over(window))
        .filter(F.col(rank_col) == 1)
        .drop(rank_col)
    )

    duplicates_dropped = total_before - deduped.count()
    return deduped, duplicates_dropped
