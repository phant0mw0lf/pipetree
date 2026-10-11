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


class UnknownTagError(GraphError):
    """A requested `--tag` / `--exclude-tag` that no table carries."""

    def __init__(self, name: str, known: list[str]):
        self.name = name
        self.known = known
        available = (
            f"known tags: {', '.join(known)}" if known else "no table has tags in this config"
        )
        super().__init__(f"no table is tagged {name!r} - {available}")


class EmptySelectionError(GraphError):
    """The selection options together leave no table to run."""

    def __init__(self, detail: str = ""):
        suffix = f" ({detail})" if detail else ""
        super().__init__(f"no table is left to run after applying the selection options{suffix}")


class DependencyParseError(GraphError, ValueError):
    """`depends_on: auto` cannot read a dependency out of a logic file: the
    file does not parse, or a table name is only known at run time. The
    message carries the file, the offending expression and the fix."""

    HINT = "declare the parents with an explicit depends_on list"

    def __init__(self, file: str, reason: str, expression: str | None = None):
        self.file = file
        self.reason = reason
        self.expression = expression
        detail = f" in `{expression}`" if expression else ""
        super().__init__(f"{file}: {reason}{detail} - {self.HINT}")
