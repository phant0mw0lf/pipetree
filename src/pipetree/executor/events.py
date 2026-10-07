"""Progress events: what the executor is doing *while* it runs.

`TableStatus` (status.py) is the final verdict the digest and run log
record. A run in progress has more states than that - a table can be
waiting, running, or backing off between retries - so progress has its
own `ProgressStatus` rather than overloading the final one.

A `RunObserver` gets `on_run_start`, one `on_table_event` per state change,
and `on_run_end`. Worker threads never call an observer directly: they post
events to an `ObserverDispatcher`, and the scheduler's own thread (the one
that called `run_pipeline`) delivers them in order. So observers needn't be
thread-safe, and in a notebook they run on the cell's own thread - which is
what display updates want. An observer that raises is logged once and
switched off; it can never fail or stall the run.
"""

from __future__ import annotations

import logging
import queue
from collections.abc import Iterable, Set
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from pipetree.executor.status import RunDigest, TableResult, TableStatus

if TYPE_CHECKING:
    from pipetree.graph.builder import Graph

_logger = logging.getLogger("pipetree.executor.events")

# Long Spark errors carry the whole JVM stack in str(exc); a progress view
# only needs enough to recognise the failure. The digest keeps the full text.
ERROR_SUMMARY_MAX_CHARS = 1000


class ProgressStatus(StrEnum):
    PENDING = "pending"  # never emitted: the state of a table nothing has happened to yet
    RUNNING = "running"
    RETRYING = "retrying"  # an attempt failed transiently; backing off before the next one
    SUCCEEDED = "succeeded"
    RETRIED_SUCCEEDED = "retried_succeeded"
    FAILED = "failed"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"

    @property
    def is_final(self) -> bool:
        return self not in _IN_FLIGHT


_IN_FLIGHT = frozenset({ProgressStatus.PENDING, ProgressStatus.RUNNING, ProgressStatus.RETRYING})

_FROM_TABLE_STATUS = {
    TableStatus.SUCCEEDED: ProgressStatus.SUCCEEDED,
    TableStatus.RETRIED_SUCCEEDED: ProgressStatus.RETRIED_SUCCEEDED,
    TableStatus.FAILED: ProgressStatus.FAILED,
    TableStatus.UPSTREAM_FAILED: ProgressStatus.UPSTREAM_FAILED,
    TableStatus.SKIPPED: ProgressStatus.SKIPPED,
}


def progress_status(status: TableStatus) -> ProgressStatus:
    """The progress status a final `TableStatus` corresponds to."""
    return _FROM_TABLE_STATUS[status]


def summarize_error(message: str | None) -> str | None:
    if message is None or len(message) <= ERROR_SUMMARY_MAX_CHARS:
        return message
    return message[: ERROR_SUMMARY_MAX_CHARS - 1] + "…"


@dataclass(frozen=True)
class TableEvent:
    fqn: str
    status: ProgressStatus
    attempt: int  # 1-based attempt this event is about; 0 for skipped/upstream_failed
    started_at: int | None = None  # YYMMddHHmmssSSS UTC bigint, like TableResult
    ended_at: int | None = None
    duration_ms: int | None = None
    error_type: str | None = None
    error_message: str | None = None  # summarised: see ERROR_SUMMARY_MAX_CHARS
    notes: tuple[str, ...] = ()  # data-quality notes, known once a table has finished

    @classmethod
    def from_result(cls, result: TableResult) -> TableEvent:
        return cls(
            fqn=result.table_fqn,
            status=progress_status(result.status),
            attempt=result.attempts,
            started_at=result.started_at,
            ended_at=result.ended_at,
            duration_ms=result.duration_ms,
            error_type=result.error_type,
            error_message=summarize_error(result.error_message),
            notes=tuple(result.notes),
        )


@runtime_checkable
class RunObserver(Protocol):
    """Watches a run as it happens. All three calls arrive on the thread
    that called `run_pipeline`, one at a time.

    An observer may also define `on_idle(self) -> None`: while the
    scheduler waits on running tables it calls that every
    `idle_interval_s` (0.5s by default), so a throttled view can flush an
    update it held back - without its own timer thread.
    """

    def on_run_start(self, graph: Graph, run_set: Set[str], execution_id: int) -> None: ...

    def on_table_event(self, event: TableEvent) -> None: ...

    def on_run_end(self, digest: RunDigest) -> None: ...


ObserverArg = RunObserver | Iterable[RunObserver] | None


class ObserverDispatcher:
    """Serialises observer callbacks onto one thread.

    `post()` is thread-safe and is what worker threads call; `drain()`,
    `start()`, `idle()` and `end()` are only called from the scheduler
    thread and are where observers actually run.
    """

    def __init__(self, observers: Iterable[RunObserver]) -> None:
        self._observers: list[RunObserver] = list(observers)
        self._queue: queue.SimpleQueue[TableEvent] = queue.SimpleQueue()

    @classmethod
    def create(cls, observer: ObserverArg) -> ObserverDispatcher | None:
        """None (no dispatcher, no overhead) when there's nothing to observe."""
        if observer is None:
            return None
        if isinstance(observer, RunObserver):
            observers: list[RunObserver] = [observer]
        else:
            observers = list(observer)
        return cls(observers) if observers else None

    @property
    def active(self) -> bool:
        return bool(self._observers)

    def post(self, event: TableEvent) -> None:
        self._queue.put(event)

    def start(self, graph: Graph, run_set: Set[str], execution_id: int) -> None:
        frozen = frozenset(run_set)
        self._each("on_run_start", lambda o: o.on_run_start(graph, frozen, execution_id))

    def drain(self) -> None:
        while True:
            try:
                event = self._queue.get_nowait()
            except queue.Empty:
                return
            self._each("on_table_event", lambda o, e=event: o.on_table_event(e))

    def idle(self) -> None:
        self.drain()
        for observer in list(self._observers):
            hook = getattr(observer, "on_idle", None)
            if callable(hook):
                self._call(observer, "on_idle", lambda _o, h=hook: h())

    def end(self, digest: RunDigest) -> None:
        self.drain()
        self._each("on_run_end", lambda o: o.on_run_end(digest))

    def _each(self, name: str, call) -> None:
        for observer in list(self._observers):
            self._call(observer, name, call)

    def _call(self, observer: RunObserver, name: str, call) -> None:
        if observer not in self._observers:
            return  # disabled by an earlier failure in this same delivery
        try:
            call(observer)
        except Exception:  # noqa: BLE001 - an observer must never fail the run
            self._observers.remove(observer)
            _logger.warning(
                "run observer %r raised in %s and has been disabled for the rest of this run",
                observer,
                name,
                exc_info=True,
            )
