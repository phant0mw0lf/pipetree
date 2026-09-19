"""Writes the run log rows collected by `pipetree.runlog.collector`.

Written once, at the end of the run, as a partition overwrite keyed on
`_execution_id` - so a crashed process still leaves whatever it collected
before dying (the write sits in a `finally`), and re-running the same
execution id is idempotent rather than doubling up rows. The real
Delta-backed writer (Phase A step 6) implements this same protocol.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class RunLogWriter(Protocol):
    def write(self, rows: list[dict[str, Any]], *, execution_id: int) -> None: ...


class InMemoryRunLogWriter:
    """Collects rows in memory, keyed by execution id - useful for tests,
    and for a local run with nowhere durable to put a run log."""

    def __init__(self) -> None:
        self._by_execution_id: dict[int, list[dict[str, Any]]] = {}
        self.write_calls = 0

    def write(self, rows: list[dict[str, Any]], *, execution_id: int) -> None:
        self.write_calls += 1
        self._by_execution_id[execution_id] = list(rows)

    def rows_for(self, execution_id: int) -> list[dict[str, Any]]:
        return self._by_execution_id.get(execution_id, [])

    @property
    def rows(self) -> list[dict[str, Any]]:
        return [row for rows in self._by_execution_id.values() for row in rows]
