"""The ready-queue scheduler: part 2's actual subject.

A table starts as soon as all its parents have succeeded - never when its
*layer* is done. A failing table is logged and every transitive descendant
is marked `upstream_failed` instead of starting; independent branches run
to completion regardless.

The scheduling state (`pending_parents`, `results`, `settled`) is only ever
touched from this function's own thread: worker threads run `adapter.run_table`
and hand back a `TableResult` through their future, so none of that state
needs a lock.

Progress events (`events.py`) follow the same rule: workers only post them
to a queue, and this thread delivers them to the observers.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

from pipetree.adapters.base import Adapter
from pipetree.executor.events import (
    ObserverArg,
    ObserverDispatcher,
    ProgressStatus,
    TableEvent,
    summarize_error,
)
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
    selected: set[str] | None = None,
    init: bool = False,
    observer: ObserverArg = None,
    idle_interval_s: float = 0.5,
) -> RunDigest:
    """Run every table in `selected` (everything when None) along the graph.

    `observer` - one `RunObserver` or several - is told about the run as it
    happens (see `events.py`); every callback arrives on this thread. With
    no observer the scheduler does exactly what it always did.
    """
    execution_id = generate_execution_id() if execution_id is None else execution_id
    retry_policy = retry_policy or RetryPolicy()
    run_set = set(graph.tables) if selected is None else selected
    dispatcher = ObserverDispatcher.create(observer)
    emit = dispatcher.post if dispatcher is not None else None
    if dispatcher is not None:
        dispatcher.start(graph, run_set, execution_id)

    results: dict[str, TableResult] = {}
    settled: set[str] = set()  # tables with a final result: no longer schedulable

    now = timestamp_bigint()
    for fqn in graph.tables:
        if fqn not in run_set:
            results[fqn] = TableResult(
                table_fqn=fqn,
                status=TableStatus.SKIPPED,
                attempts=0,
                started_at=now,
                ended_at=now,
                duration_ms=0,
            )
            settled.add(fqn)
            if emit is not None:
                emit(TableEvent.from_result(results[fqn]))

    # A parent outside the run set won't run in this invocation - treat it
    # as already satisfied rather than blocking the selected table forever.
    pending_parents = {
        fqn: len([p for p in graph.edges.get(fqn, ()) if p in run_set]) for fqn in run_set
    }

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures: dict[Future[TableResult], str] = {}

        def submit(fqn: str) -> None:
            future = pool.submit(
                _run_with_retries,
                graph.tables[fqn],
                adapter,
                execution_id,
                retry_policy,
                init,
                emit,
            )
            futures[future] = fqn

        for fqn, count in pending_parents.items():
            if count == 0:
                submit(fqn)

        if dispatcher is not None:
            dispatcher.drain()

        while futures:
            if dispatcher is None:
                done, _ = wait(list(futures), return_when=FIRST_COMPLETED)
            else:
                # Wake up periodically to deliver the workers' events (and
                # give throttled observers their `on_idle`) while tables run.
                done, _ = wait(list(futures), timeout=idle_interval_s, return_when=FIRST_COMPLETED)
                dispatcher.idle()
            for future in done:
                fqn = futures.pop(future)
                result = future.result()
                results[fqn] = result
                settled.add(fqn)
                if emit is not None:
                    emit(TableEvent.from_result(result))

                if result.status == TableStatus.FAILED:
                    _mark_upstream_failed(fqn, graph, results, settled, emit)
                else:
                    for child in sorted(graph.reverse_edges.get(fqn, ())):
                        if child not in run_set or child in settled:
                            continue
                        pending_parents[child] -= 1
                        if pending_parents[child] == 0:
                            submit(child)

            if dispatcher is not None:
                dispatcher.drain()

    digest = RunDigest(execution_id=execution_id, results=results)
    if dispatcher is not None:
        dispatcher.end(digest)
    return digest


def _mark_upstream_failed(
    failed_fqn: str,
    graph: Graph,
    results: dict[str, TableResult],
    settled: set[str],
    emit: Callable[[TableEvent], None] | None = None,
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
        if emit is not None:
            emit(TableEvent.from_result(results[fqn]))
        stack.extend(graph.reverse_edges.get(fqn, ()))


def _run_with_retries(
    table: Table,
    adapter: Adapter,
    execution_id: int,
    retry_policy: RetryPolicy,
    init: bool = False,
    emit: Callable[[TableEvent], None] | None = None,
) -> TableResult:
    started_at = timestamp_bigint()
    start_perf = time.perf_counter()
    attempts = 0

    _logger.info("%s: starting%s", table.fqn, " (init)" if init else "")

    while True:
        attempts += 1
        if emit is not None:
            emit(TableEvent(table.fqn, ProgressStatus.RUNNING, attempts, started_at=started_at))
        try:
            details = adapter.run_table(table, execution_id=execution_id, init=init) or {}
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
            if emit is not None:
                emit(
                    TableEvent(
                        table.fqn,
                        ProgressStatus.RETRYING,
                        attempts,
                        started_at=started_at,
                        error_type=type(exc).__name__,
                        error_message=summarize_error(str(exc)),
                    )
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
