"""The ready-queue scheduler: part 2's actual subject.

A table starts as soon as all its parents have succeeded - never when its
*layer* is done. A failing table is logged and every transitive descendant
is marked `upstream_failed` instead of starting; independent branches run
to completion regardless.

The scheduling state (`pending_parents`, `results`, `settled`) is only ever
touched from this function's own thread: worker threads run `adapter.run_table`
and hand back a `TableResult` through their future, so none of that state
needs a lock.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

from pipetree.adapters.base import Adapter
from pipetree.executor.execution_id import generate_execution_id, timestamp_bigint
from pipetree.executor.retry import RetryPolicy, backoff_delay
from pipetree.executor.status import RunDigest, TableResult, TableStatus
from pipetree.graph.builder import Graph
from pipetree.model import Table

_logger = logging.getLogger("pipetree.executor")


def run_pipeline(
    graph: Graph,
    adapter: Adapter,
    *,
    execution_id: int | None = None,
    max_workers: int = 4,
    retry_policy: RetryPolicy | None = None,
) -> RunDigest:
    execution_id = generate_execution_id() if execution_id is None else execution_id
    retry_policy = retry_policy or RetryPolicy()

    pending_parents = {fqn: len(parents) for fqn, parents in graph.edges.items()}
    results: dict[str, TableResult] = {}
    settled: set[str] = set()  # tables with a final result: no longer schedulable

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures: dict[Future[TableResult], str] = {}

        def submit(fqn: str) -> None:
            future = pool.submit(
                _run_with_retries, graph.tables[fqn], adapter, execution_id, retry_policy
            )
            futures[future] = fqn

        for fqn, count in pending_parents.items():
            if count == 0:
                submit(fqn)

        while futures:
            done, _ = wait(list(futures), return_when=FIRST_COMPLETED)
            for future in done:
                fqn = futures.pop(future)
                result = future.result()
                results[fqn] = result
                settled.add(fqn)

                if result.status == TableStatus.FAILED:
                    _mark_upstream_failed(fqn, graph, results, settled)
                else:
                    for child in sorted(graph.reverse_edges.get(fqn, ())):
                        if child in settled:
                            continue
                        pending_parents[child] -= 1
                        if pending_parents[child] == 0:
                            submit(child)

    return RunDigest(execution_id=execution_id, results=results)


def _mark_upstream_failed(
    failed_fqn: str,
    graph: Graph,
    results: dict[str, TableResult],
    settled: set[str],
) -> None:
    """BFS over descendants of a failed table, marking each `upstream_failed`
    exactly once - a table can be reachable from more than one failed
    ancestor, so `settled` guards against double-marking it."""
    now = timestamp_bigint()
    stack = list(graph.reverse_edges.get(failed_fqn, ()))
    while stack:
        fqn = stack.pop()
        if fqn in settled:
            continue
        settled.add(fqn)
        _logger.warning("%s: upstream_failed (ancestor %s failed)", fqn, failed_fqn)
        results[fqn] = TableResult(
            table_fqn=fqn,
            status=TableStatus.UPSTREAM_FAILED,
            attempts=0,
            started_at=now,
            ended_at=now,
            duration_ms=0,
        )
        stack.extend(graph.reverse_edges.get(fqn, ()))


def _run_with_retries(
    table: Table, adapter: Adapter, execution_id: int, retry_policy: RetryPolicy
) -> TableResult:
    started_at = timestamp_bigint()
    start_perf = time.perf_counter()
    attempts = 0

    _logger.info("%s: starting", table.fqn)

    while True:
        attempts += 1
        try:
            details = adapter.run_table(table, execution_id=execution_id) or {}
        except Exception as exc:  # noqa: BLE001 - classified below, not swallowed
            can_retry = retry_policy.can_retry(attempts, exc)

            # append is not an idempotent write: a retry after a partial
            # write duplicates rows, unless the adapter can make the retry
            # atomic by clearing out this run's rows first.
            if can_retry and table.strategy == "append":
                if adapter.capabilities.supports_delete_by_execution_id:
                    adapter.delete_by_execution_id(table, execution_id)
                else:
                    can_retry = False

            if not can_retry:
                _logger.error(
                    "%s: failed after %d attempt(s): %s: %s",
                    table.fqn,
                    attempts,
                    type(exc).__name__,
                    exc,
                )
                return _finish(
                    table.fqn,
                    TableStatus.FAILED,
                    attempts,
                    started_at,
                    start_perf,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )

            delay = backoff_delay(attempts, retry_policy)
            _logger.warning(
                "%s: attempt %d failed (%s: %s), retrying in %.2fs",
                table.fqn,
                attempts,
                type(exc).__name__,
                exc,
                delay,
            )
            time.sleep(delay)
            continue

        status = TableStatus.SUCCEEDED if attempts == 1 else TableStatus.RETRIED_SUCCEEDED
        result = _finish(table.fqn, status, attempts, started_at, start_perf, details=details)
        _logger.info(
            "%s: %s (attempts=%d, %dms)", table.fqn, status.value, attempts, result.duration_ms
        )
        return result


def _finish(
    table_fqn: str,
    status: TableStatus,
    attempts: int,
    started_at: int,
    start_perf: float,
    *,
    error_type: str | None = None,
    error_message: str | None = None,
    details: dict | None = None,
) -> TableResult:
    return TableResult(
        table_fqn=table_fqn,
        status=status,
        attempts=attempts,
        started_at=started_at,
        ended_at=timestamp_bigint(),
        duration_ms=int((time.perf_counter() - start_perf) * 1000),
        error_type=error_type,
        error_message=error_message,
        details=details or {},
    )
