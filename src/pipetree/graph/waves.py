"""Waves: "when what can run", read off the dependency tree.

A table's wave is the length of the longest dependency path from a root
(a table with no parents) to it. Wave 0 is every root; a table in wave k
needs at least k tables to finish, one after another, before it can
start. Tables in the same wave never depend on each other, so they *can*
run in parallel - how many actually do is bounded by `max_workers`.

The executor doesn't wait for a whole wave, though: it starts a table the
moment its own parents have succeeded. A wave is the earliest step a
table can run in, not a barrier - which is why it's a drawing aid here
and not something the scheduler consults.
"""

from __future__ import annotations

from collections import deque

from pipetree.graph.builder import Graph


def layer_of(fqn: str) -> str:
    """The layer a table is drawn under: its fqn's prefix before the first dot."""
    return fqn.split(".", 1)[0]


def layer_order(graph: Graph) -> list[str]:
    """Layers in the order they first appear in the config - stable for the
    same YAML, and usually bronze -> silver -> gold as written."""
    seen: dict[str, None] = {}
    for fqn in graph.tables:
        seen.setdefault(layer_of(fqn), None)
    return list(seen)


def compute_waves(graph: Graph) -> dict[str, int]:
    """fqn -> wave (longest path from a root). Iterative (Kahn's), so a
    very deep chain doesn't hit Python's recursion limit. The graph is
    assumed acyclic - `build_graph` already refuses a cycle."""
    remaining = {fqn: len(graph.edges.get(fqn, ())) for fqn in graph.tables}
    wave = dict.fromkeys(graph.tables, 0)
    ready = deque(sorted(fqn for fqn, count in remaining.items() if count == 0))

    while ready:
        fqn = ready.popleft()
        for child in sorted(graph.reverse_edges.get(fqn, ())):
            if child not in remaining:
                continue
            wave[child] = max(wave[child], wave[fqn] + 1)
            remaining[child] -= 1
            if remaining[child] == 0:
                ready.append(child)

    return wave


def wave_columns(graph: Graph, waves: dict[str, int] | None = None) -> list[list[str]]:
    """One list per wave, in wave order; within a wave grouped by layer
    (in `layer_order`) and then sorted by fqn."""
    waves = compute_waves(graph) if waves is None else waves
    rank = {layer: i for i, layer in enumerate(layer_order(graph))}
    count = max(waves.values(), default=-1) + 1
    columns: list[list[str]] = [[] for _ in range(count)]
    for fqn, k in waves.items():
        columns[k].append(fqn)
    for column in columns:
        column.sort(key=lambda fqn: (rank[layer_of(fqn)], fqn))
    return columns
