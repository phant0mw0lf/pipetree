"""Builds the dependency graph: edges, self-read filtering, topological order.

Execution follows this tree, not the layers a table happens to be declared
under - a layer is a namespace in the YAML, never an execution barrier.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pipetree.graph.errors import CycleError, GraphError, UnresolvedDependencyError
from pipetree.graph.parsers import parse_logic_dependencies
from pipetree.model import PipelineConfig, Table


@dataclass(frozen=True)
class Graph:
    tables: dict[str, Table]
    edges: dict[str, frozenset[str]]  # fqn -> parents (what it depends on)
    reverse_edges: dict[str, frozenset[str]]  # fqn -> children (what depends on it)
    order: list[str]  # topological run order: parents before children


def build_graph(config: PipelineConfig, base_dir: str | Path) -> Graph:
    base_dir = Path(base_dir)
    tables = config.tables

    edges: dict[str, set[str]] = {}
    for fqn, table in tables.items():
        deps = _resolve_table_dependencies(table, tables, base_dir)
        deps.discard(fqn)  # a table reading its own current state isn't a cycle
        edges[fqn] = deps

    order = _topological_sort(edges)

    frozen_edges = {fqn: frozenset(parents) for fqn, parents in edges.items()}
    reverse_edges = _reverse(frozen_edges)

    return Graph(tables=tables, edges=frozen_edges, reverse_edges=reverse_edges, order=order)


def _resolve_table_dependencies(table: Table, tables: dict[str, Table], base_dir: Path) -> set[str]:
    if table.depends_on == "auto":
        if table.logic is None:
            raise GraphError(f"{table.fqn}: depends_on: auto requires a 'logic' file")
        raw_names = parse_logic_dependencies(base_dir / table.logic)
        # auto-parsed names that don't match a known table are treated as
        # reads of something outside the pipeline (e.g. a raw external
        # table) rather than an error - the parser can't tell the two apart.
        return {
            resolved
            for name in raw_names
            if (resolved := resolve_table_name(name, tables)) is not None
        }

    resolved_deps: set[str] = set()
    for name in table.depends_on:
        resolved = resolve_table_name(name, tables)
        if resolved is None:
            raise UnresolvedDependencyError(table.fqn, name)
        resolved_deps.add(resolved)
    return resolved_deps


def resolve_table_name(name: str, tables: dict[str, Table]) -> str | None:
    """Resolve a name to a table's fqn: exact fqn match, or - if
    unambiguous - a match on the bare table name. Used both for
    dependency resolution and for `--select` on the CLI."""
    if name in tables:
        return name

    matches = [fqn for fqn, t in tables.items() if t.name == name]
    if len(matches) == 1:
        return matches[0]
    return None  # zero or ambiguous matches both come back unresolved


def _topological_sort(edges: dict[str, set[str]]) -> list[str]:
    state: dict[str, int] = {}  # 0 = unvisited (absent), 1 = in progress, 2 = done
    order: list[str] = []
    stack: list[str] = []

    def visit(node: str) -> None:
        if state.get(node) == 2:
            return
        if state.get(node) == 1:
            cycle_start = stack.index(node)
            raise CycleError(stack[cycle_start:] + [node])

        state[node] = 1
        stack.append(node)
        for parent in sorted(edges.get(node, ())):
            visit(parent)
        stack.pop()

        state[node] = 2
        order.append(node)

    for node in sorted(edges):
        visit(node)

    return order


def _reverse(edges: dict[str, frozenset[str]]) -> dict[str, frozenset[str]]:
    children: dict[str, set[str]] = {fqn: set() for fqn in edges}
    for fqn, parents in edges.items():
        for parent in parents:
            children.setdefault(parent, set()).add(fqn)
    return {fqn: frozenset(kids) for fqn, kids in children.items()}
