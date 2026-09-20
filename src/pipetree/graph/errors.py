"""Errors raised while building the dependency graph."""

from __future__ import annotations


class GraphError(Exception):
    """Base class for graph-building failures."""


class UnresolvedDependencyError(GraphError):
    """An explicit `depends_on` entry doesn't resolve to exactly one table."""

    def __init__(self, table_fqn: str, dependency_name: str, reason: str = "no such table"):
        self.table_fqn = table_fqn
        self.dependency_name = dependency_name
        super().__init__(f"{table_fqn}: depends_on {dependency_name!r} - {reason}")


class CycleError(GraphError):
    """The dependency graph contains a cycle. ``cycle`` starts and ends on the
    same table so the path can be printed as ``a → b → a``."""

    def __init__(self, cycle: list[str]):
        self.cycle = cycle
        super().__init__(f"dependency cycle: {' → '.join(cycle)}")


class UnknownTableError(GraphError):
    """A `--select` argument doesn't match exactly one table in the graph."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(
            f"no table matches {name!r} - not a known fully qualified name, "
            "and no single table has that as its bare name"
        )
