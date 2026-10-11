"""Renders the dependency tree - because the package holds the whole tree,
it can draw it. Usually the first time someone actually *sees* their
pipeline instead of trusting a run order they wired by hand."""

from __future__ import annotations

from collections.abc import Set
from typing import Literal

from pipetree.graph.builder import Graph

Format = Literal["text", "mermaid", "html"]


def render_graph(graph: Graph, fmt: str = "text", run_set: Set[str] | None = None) -> str:
    """`fmt` is deliberately `str`, not the `Format` literal: it's meant to
    validate a value that came from outside (CLI, user code) and raise a
    clear error for anything else, not to be a compile-time-only choice.

    `run_set` is the tables a run would cover (None: all of them); the others
    are marked as not selected."""
    if fmt == "text":
        return _render_text(graph, run_set)
    if fmt == "mermaid":
        return _render_mermaid(graph, run_set)
    if fmt == "html":
        # Imported here: the HTML renderer is the one format that knows
        # about run progress, and text/mermaid callers shouldn't pay for it.
        from pipetree.graph.html import preview_states, render_html_page

        return render_html_page(graph, preview_states(graph, run_set), title="pipetree graph")
    raise ValueError(f"{fmt!r} is not a supported format (expected 'text', 'mermaid' or 'html')")


def _render_text(graph: Graph, run_set: Set[str] | None) -> str:
    lines = []
    for fqn in graph.order:
        parents = sorted(graph.edges.get(fqn, ()))
        line = f"{fqn}  (depends on: {', '.join(parents)})" if parents else fqn
        if run_set is not None and fqn not in run_set:
            line += "  [not selected]"
        lines.append(line)
    return "\n".join(lines)


def _render_mermaid(graph: Graph, run_set: Set[str] | None) -> str:
    node_ids = {fqn: f"n{i}" for i, fqn in enumerate(sorted(graph.tables))}
    lines = ["graph LR"]

    for fqn in sorted(graph.tables):
        parents = sorted(graph.edges.get(fqn, ()))
        for parent in parents:
            lines.append(f"  {node_ids[parent]}[{parent}] --> {node_ids[fqn]}[{fqn}]")
        if not parents and not graph.reverse_edges.get(fqn):
            lines.append(f"  {node_ids[fqn]}[{fqn}]")

    if run_set is not None:
        skipped = [node_ids[fqn] for fqn in sorted(graph.tables) if fqn not in run_set]
        if skipped:
            lines.append("  classDef notselected stroke-dasharray:3 3,opacity:0.5")
            lines.append(f"  class {','.join(skipped)} notselected")

    return "\n".join(lines)
