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

from collections.abc import Iterable

from pipetree.graph.builder import Graph, resolve_table_name
from pipetree.graph.errors import EmptySelectionError, UnknownTableError, UnknownTagError
from pipetree.model import Table


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
    `with_ancestors` every table upstream of everything that runs - so with
    both, the dependents come first and their other parents are pulled in too:
    nothing runs on a parent that was left out.
    """
    if select is None:
        return None

    return _extend(graph, _resolve_names(graph, select), with_dependents, with_ancestors)


def split_names(values: str | Iterable[str] | None) -> list[str]:
    """Names from a comma-separated string or a list of such strings, trimmed,
    blanks dropped, duplicates removed (first one wins). `"daily, crm"` and
    `["daily", " crm,"]` give the same result; None and `""` give `[]`."""
    if values is None:
        return []
    items = [values] if isinstance(values, str) else list(values)
    names: list[str] = []
    for item in items:
        for part in item.split(","):
            name = part.strip()
            if name and name not in names:
                names.append(name)
    return names


def count_tags(graph: Graph) -> dict[str, int]:
    """Every tag in the graph with the number of tables carrying it, sorted by tag."""
    return count_table_tags(graph.tables.values())


def count_table_tags(tables: Iterable[Table]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in tables:
        for tag in table.tags:
            counts[tag] = counts.get(tag, 0) + 1
    return dict(sorted(counts.items()))


def resolve_run_set(
    graph: Graph,
    *,
    select: list[str] | None = None,
    tags: list[str] | None = None,
    exclude_tags: list[str] | None = None,
    with_dependents: bool = False,
    with_ancestors: bool = False,
) -> set[str] | None:
    """The tables a run covers: None (everything) when no option narrows it.

    Order: `select` and `tags` intersect (`tags`: a table needs at least one
    of them); `with_dependents` / `with_ancestors` extend that set exactly as
    they do for `select`; `exclude_tags` is applied last and wins, so an
    excluded table stays out even when a closure pulled it in. A tag no table
    carries raises `UnknownTagError`, an empty result `EmptySelectionError`.
    """
    tags = list(tags or [])
    exclude_tags = list(exclude_tags or [])
    known = count_tags(graph)
    for tag in (*tags, *exclude_tags):
        if tag not in known:
            raise UnknownTagError(tag, list(known))

    if select is None and not tags and not exclude_tags:
        return None

    seeds = set(graph.tables)
    if select is not None:
        seeds = _resolve_names(graph, select)
    if tags:
        seeds = {fqn for fqn in seeds if set(graph.tables[fqn].tags) & set(tags)}
    if not seeds:
        raise EmptySelectionError(
            "--select and --tag have no table in common" if select and tags else ""
        )

    result = _extend(graph, seeds, with_dependents, with_ancestors)
    if exclude_tags:
        result = {fqn for fqn in result if not set(graph.tables[fqn].tags) & set(exclude_tags)}
    if not result:
        raise EmptySelectionError("--exclude-tag removed every table")
    return result


def inputs_not_refreshed(graph: Graph, run_set: set[str]) -> dict[str, list[str]]:
    """For each table in the run, its direct parents that are not part of it."""
    result: dict[str, list[str]] = {}
    for fqn in sorted(run_set):
        missing = sorted(parent for parent in graph.edges.get(fqn, ()) if parent not in run_set)
        if missing:
            result[fqn] = missing
    return result


def _resolve_names(graph: Graph, select: list[str]) -> set[str]:
    resolved: set[str] = set()
    for name in select:
        match = resolve_table_name(name, graph.tables)
        if match is None:
            raise UnknownTableError(name)
        resolved.add(match)
    return resolved


def _extend(graph: Graph, seeds: set[str], with_dependents: bool, with_ancestors: bool) -> set[str]:
    if with_dependents:
        seeds = descendant_closure(graph, seeds)
    if with_ancestors:
        seeds = ancestor_closure(graph, seeds)
    return seeds


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
