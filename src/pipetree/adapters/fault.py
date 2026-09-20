"""A fault-injecting Adapter wrapper, built for the demo run: makes
retries, failures and upstream_failed propagation something a reader can
actually see happen in real log output, instead of taking it on faith."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Literal

from pipetree.adapters.base import Adapter, Capabilities
from pipetree.executor.retry import Throttled
from pipetree.model import Table


@dataclass(frozen=True)
class FaultSpec:
    kind: Literal["throttle", "fail", "slow"]
    count: int = 1  # throttle: how many calls raise before it succeeds
    delay: float = 2.0  # slow: seconds to sleep before delegating
    message: str = "injected fault"


class FaultInjectingAdapter:
    """Wraps any Adapter. A table with a `throttle` spec raises `Throttled`
    (transient, so the executor retries it) `count` times, then succeeds.
    `fail` always raises a plain `ValueError` (not transient - never
    retried). `slow` sleeps before delegating to the wrapped adapter."""

    def __init__(self, wrapped: Adapter, faults: dict[str, FaultSpec]) -> None:
        self._wrapped = wrapped
        self._faults = faults
        self._throttle_calls: dict[str, int] = {}

    @property
    def capabilities(self) -> Capabilities:
        return self._wrapped.capabilities

    def run_table(self, table: Table, *, execution_id: int) -> dict[str, Any] | None:
        spec = self._faults.get(table.fqn)
        if spec is not None:
            if spec.kind == "fail":
                raise ValueError(spec.message)
            if spec.kind == "throttle":
                count = self._throttle_calls.get(table.fqn, 0) + 1
                self._throttle_calls[table.fqn] = count
                if count <= spec.count:
                    raise Throttled(spec.message)
            elif spec.kind == "slow":
                time.sleep(spec.delay)

        return self._wrapped.run_table(table, execution_id=execution_id)

    def delete_by_execution_id(self, table: Table, execution_id: int) -> None:
        self._wrapped.delete_by_execution_id(table, execution_id)
