"""Renders the dependency tree - because the package holds the whole tree,
it can draw it. Usually the first time someone actually *sees* their
pipeline instead of trusting a run order they wired by hand."""

from __future__ import annotations

from typing import Literal

from pipetree.graph.builder import Graph

Format = Literal["text", "mermaid"]


def render_graph(graph: Graph, fmt: str = "text") -> str:
    """`fmt` is deliberately `str`, not the `Format` literal: it's meant to
    validate a value that came from outside (CLI, user code) and raise a
    clear error for anything else, not to be a compile-time-only choice."""
    if fmt == "text":
        return _render_text(graph)
    if fmt == "mermaid":
        return _render_mermaid(graph)
    raise ValueError(f"{fmt!r} is not a supported format (expected 'text' or 'mermaid')")


def _render_text(graph: Graph) -> str:
    lines = []
    for fqn in graph.order:
        parents = sorted(graph.edges.get(fqn, ()))
        if parents:
            lines.append(f"{fqn}  (depends on: {', '.join(parents)})")
        else:
            lines.append(fqn)
    return "\n".join(lines)


def _render_mermaid(graph: Graph) -> str:
    node_ids = {fqn: f"n{i}" for i, fqn in enumerate(sorted(graph.tables))}
    lines = ["graph LR"]

    for fqn in sorted(graph.tables):
        parents = sorted(graph.edges.get(fqn, ()))
        for parent in parents:
            lines.append(f"  {node_ids[parent]}[{parent}] --> {node_ids[fqn]}[{fqn}]")
        if not parents and not graph.reverse_edges.get(fqn):
            lines.append(f"  {node_ids[fqn]}[{fqn}]")

    return "\n".join(lines)
