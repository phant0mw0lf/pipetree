"""Pre-merge deduplication: a source batch with several rows per
`business_key` is deduped before an scd1/scd2 merge (Delta's MERGE errors
on multi-matches otherwise). This is not a failure: duplicates are dropped
and logged here as a warning (the count also lands in the run's details as
`duplicates_dropped`), and the run keeps going. Without `sequence_by` the
winner is deterministic for a given input but arbitrary, which the warning
says."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipetree.model import Table

_logger = logging.getLogger("pipetree.adapters.spark")


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
    if duplicates_dropped:
        if table.merge.sequence_by:
            winner = f" - kept the highest ({', '.join(table.merge.sequence_by)})"
        else:
            winner = "; no sequence_by is declared, so which duplicate won is arbitrary"
        _logger.warning(
            "%s: dropped %d duplicate row(s) for key (%s)%s",
            table.fqn,
            duplicates_dropped,
            ", ".join(table.business_key),
            winner,
        )
    return deduped, duplicates_dropped
