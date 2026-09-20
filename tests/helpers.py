from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pipetree.adapters.base import Capabilities
from pipetree.graph.builder import Graph
from pipetree.model import Strategy, Table


def make_graph(strategies: dict[str, Strategy], edges: dict[str, set[str]] | None = None) -> Graph:
    """Build a Graph directly from fqn -> strategy (+ optional fqn -> parent fqns),
    bypassing YAML/config entirely - executor tests care about scheduling, not
    about how the graph got built."""
    edges = edges or {}
    tables = {
        fqn: Table(
            name=fqn.split(".")[-1],
            layer=fqn.split(".")[0],
            table_schema=fqn.split(".")[0],
            fqn=fqn,
            strategy=strategy,
            business_key=[],
            depends_on=sorted(edges.get(fqn, ())),
        )
        for fqn, strategy in strategies.items()
    }

    frozen_edges = {fqn: frozenset(edges.get(fqn, ())) for fqn in tables}
    reverse: dict[str, set[str]] = {fqn: set() for fqn in tables}
    for fqn, parents in frozen_edges.items():
        for parent in parents:
            reverse[parent].add(fqn)
    reverse_edges = {fqn: frozenset(children) for fqn, children in reverse.items()}

    return Graph(
        tables=tables,
        edges=frozen_edges,
        reverse_edges=reverse_edges,
        order=list(tables),  # scheduling doesn't consult precomputed order
    )


class FakeAdapter:
    """A hand-written Adapter test double - real code, not a mock. Each
    table can be given a `behavior`: a zero-arg callable invoked on every
    run_table() call for that table, returning details or raising."""

    def __init__(
        self,
        behaviors: dict[str, Callable[[], dict[str, Any] | None]] | None = None,
        capabilities: Capabilities | None = None,
    ) -> None:
        self.capabilities = capabilities or Capabilities()
        self._behaviors = behaviors or {}
        self._lock = threading.Lock()
        self.calls: list[str] = []
        self.init_calls: list[str] = []
        self.deleted_execution_ids: list[tuple[str, int]] = []

    def run_table(
        self, table: Table, *, execution_id: int, init: bool = False
    ) -> dict[str, Any] | None:
        with self._lock:
            self.calls.append(table.fqn)
            if init:
                self.init_calls.append(table.fqn)
        behavior = self._behaviors.get(table.fqn)
        if behavior is None:
            return {}
        return behavior()

    def delete_by_execution_id(self, table: Table, execution_id: int) -> None:
        with self._lock:
            self.deleted_execution_ids.append((table.fqn, execution_id))


@dataclass
class Flaky:
    """A behavior that fails `n_failures` times (raising `exc_factory()`)
    before succeeding."""

    n_failures: int
    exc_factory: Callable[[], Exception]
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _calls: int = field(default=0, repr=False)

    def __call__(self) -> dict[str, Any]:
        with self._lock:
            self._calls += 1
            call_number = self._calls
        if call_number <= self.n_failures:
            raise self.exc_factory()
        return {}


@dataclass
class AlwaysFails:
    exc_factory: Callable[[], Exception]

    def __call__(self) -> dict[str, Any]:
        raise self.exc_factory()
