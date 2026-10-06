"""Table selection and the `--with-dependents` / `--with-ancestors` scope.

The tree also decides what a re-run touches: a single table, or a table
and everything downstream of it. The second mode is the one CI/CD wants -
the release pipeline hands the package the tables that changed, and it
re-runs each plus every dependent, so a new column added upstream actually
propagates through the whole subtree instead of leaving stale schemas
behind. The mirror image, `--with-ancestors`, runs a table together with
everything it transitively reads - e.g. a gold table and every table upstream
of it, to rebuild one output from its sources.
"""

from __future__ import annotations

from pipetree.graph.builder import Graph, resolve_table_name
from pipetree.graph.errors import UnknownTableError


def resolve_selection(
    graph: Graph,
    select: list[str] | None,
    with_dependents: bool,
    with_ancestors: bool = False,
) -> set[str] | None:
    """`select` names (fqn or unambiguous bare name) resolved to fqns.

    Returns None (meaning "everything") when `select` is None. Raises
    `UnknownTableError` for a name that doesn't resolve to exactly one
    table. `with_dependents` adds every table downstream of the selection,
    `with_ancestors` every table upstream of it; with both, each selected
    table gets both closures (not the ancestors of its dependents).
    """
    if select is None:
        return None

    resolved: set[str] = set()
    for name in select:
        match = resolve_table_name(name, graph.tables)
        if match is None:
            raise UnknownTableError(name)
        resolved.add(match)

    closure = set(resolved)
    if with_dependents:
        closure |= descendant_closure(graph, resolved)
    if with_ancestors:
        closure |= ancestor_closure(graph, resolved)

    return closure


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


def ancestor_closure(graph: Graph, seeds: set[str]) -> set[str]:
    """`seeds` plus every table they transitively read, via `edges`."""
    result: set[str] = set()
    stack = list(seeds)
    while stack:
        fqn = stack.pop()
        if fqn in result:
            continue
        result.add(fqn)
        stack.extend(graph.edges.get(fqn, ()))
    return result
