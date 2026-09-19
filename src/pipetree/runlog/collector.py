"""Shapes a RunDigest into the rows written to `_meta.pipeline_run_log`.

Collected in memory as the run progresses (each table result comes back
through its future); shaped and written once, at the end - see
`pipetree.runlog.writer`.
"""

from __future__ import annotations

from typing import Any

from pipetree.executor.status import RunDigest
from pipetree.graph.builder import Graph


def build_run_log_rows(digest: RunDigest, graph: Graph) -> list[dict[str, Any]]:
    return [_row_for(fqn, digest, graph) for fqn in digest.results]


def _row_for(fqn: str, digest: RunDigest, graph: Graph) -> dict[str, Any]:
    result = digest.results[fqn]
    table = graph.tables[fqn]
    details = result.details

    return {
        "_execution_id": digest.execution_id,
        "table_fqn": fqn,
        "layer": table.layer,
        "strategy": table.strategy,
        "status": result.status.value,
        "attempts": result.attempts,
        "started_at": result.started_at,
        "ended_at": result.ended_at,
        "duration_ms": result.duration_ms,
        "rows_written": details.get("rows_written"),
        "duplicates_dropped": details.get("duplicates_dropped"),
        "schema_changes": details.get("schema_changes"),
        "error_type": result.error_type,
        "error_message": result.error_message,
    }
