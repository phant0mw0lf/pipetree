"""Table selection and the `--with-dependents` subtree override.

The tree also decides what a re-run touches: a single table, or a table
and everything downstream of it. The second mode is the one CI/CD wants -
the release pipeline hands the package the tables that changed, and it
re-runs each plus every dependent, so a new column added upstream actually
propagates through the whole subtree instead of leaving stale schemas
behind.
"""

from __future__ import annotations

from pipetree.graph.builder import Graph, resolve_table_name
from pipetree.graph.errors import UnknownTableError


def resolve_selection(
    graph: Graph, select: list[str] | None, with_dependents: bool
) -> set[str] | None:
    """`select` names (fqn or unambiguous bare name) resolved to fqns.

    Returns None (meaning "everything") when `select` is None. Raises
    `UnknownTableError` for a name that doesn't resolve to exactly one
    table.
    """
    if select is None:
        return None

    resolved: set[str] = set()
    for name in select:
        match = resolve_table_name(name, graph.tables)
        if match is None:
            raise UnknownTableError(name)
        resolved.add(match)

    if with_dependents:
        resolved = descendant_closure(graph, resolved)

    return resolved


def descendant_closure(graph: Graph, seeds: set[str]) -> set[str]:
    """`seeds` plus every table reachable from them via `reverse_edges`."""
    result: set[str] = set()
    stack = list(seeds)
    while stack:
        fqn = stack.pop()
        if fqn in result:
            continue
        result.add(fqn)
        stack.extend(graph.reverse_edges.get(fqn, ()))
    return result
