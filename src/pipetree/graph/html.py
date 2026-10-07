"""The dependency tree as one self-contained HTML string - the picture of
"when what runs".

Waves are columns, left to right (see `waves.py`): wave k holds the tables
whose longest dependency path from a root is k, so everything in one
column can run side by side. Within a column tables are grouped by layer
(a coloured stripe on each cell), and a very tall column wraps into
balanced sub-columns so a 170-table pipeline still fits on a screen.

Everything is inline - CSS, SVG, no scripts, fonts or URLs - so the same
string works in a sandboxed notebook iframe (Databricks `displayHTML`,
Jupyter/Fabric `display(HTML(...))`) and as a file in a browser. Edges are
a faint always-on layer; hovering a table highlights its own edges (CSS
`:has()`, no JavaScript). Every piece of text that comes from the config
or from an error goes through `html.escape`. Output is deterministic: the
same graph and states give the same string.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from html import escape

from pipetree.executor.events import ProgressStatus, TableEvent, progress_status
from pipetree.executor.status import RunDigest, TableResult, TableStatus
from pipetree.graph.builder import Graph
from pipetree.graph.waves import compute_waves, layer_of, layer_order, wave_columns

THEMES = ("auto", "light", "dark")


@dataclass(frozen=True)
class NodeState:
    """What the picture shows for one table."""

    status: ProgressStatus = ProgressStatus.PENDING
    attempt: int = 0
    duration_ms: int | None = None
    error_type: str | None = None
    error_message: str | None = None
    notes: tuple[str, ...] = ()  # data-quality notes; a badge, never a status colour

    @classmethod
    def from_event(cls, event: TableEvent) -> NodeState:
        return cls(
            status=event.status,
            attempt=event.attempt,
            duration_ms=event.duration_ms,
            error_type=event.error_type,
            error_message=event.error_message,
            notes=event.notes,
        )

    @classmethod
    def from_result(cls, result: TableResult) -> NodeState:
        return cls.from_event(TableEvent.from_result(result))


def states_from_digest(digest: RunDigest) -> dict[str, NodeState]:
    return {fqn: NodeState.from_result(result) for fqn, result in digest.results.items()}


StateValue = NodeState | ProgressStatus | str


# --- layout ---------------------------------------------------------------

_CELL_W = 188
_CELL_H = 20
_ROW_GAP = 4
_SUBCOL_GAP = 12
_WAVE_GAP = 56
_PAD = 14
_HEAD_H = 40
_LABEL_CHARS = 24  # what fits in a cell at 11px monospace beside the glyph
_LABEL_CHARS_NOTED = 22  # the notes badge takes the cell's right edge
_NOTE_GLYPH = "⚠"

# --- status vocabulary ----------------------------------------------------

# (glyph, label). Glyphs carry the status as well as colour, so the picture
# still reads for colour-blind viewers and in a greyscale print.
_STATUS = {
    ProgressStatus.PENDING: ("○", "pending"),
    ProgressStatus.RUNNING: ("▶", "running"),
    ProgressStatus.RETRYING: ("↻", "retrying"),
    ProgressStatus.SUCCEEDED: ("✓", "succeeded"),
    ProgressStatus.RETRIED_SUCCEEDED: ("✓", "retried→succeeded"),
    ProgressStatus.FAILED: ("✗", "failed"),
    ProgressStatus.UPSTREAM_FAILED: ("⊘", "upstream_failed"),
    ProgressStatus.SKIPPED: ("–", "skipped"),
}

# Header counts: (label, statuses folded into it, always shown?)
_COUNT_GROUPS = (
    ("succeeded", (ProgressStatus.SUCCEEDED, ProgressStatus.RETRIED_SUCCEEDED), True),
    ("running", (ProgressStatus.RUNNING, ProgressStatus.RETRYING), True),
    ("failed", (ProgressStatus.FAILED,), True),
    ("upstream_failed", (ProgressStatus.UPSTREAM_FAILED,), False),
    ("skipped", (ProgressStatus.SKIPPED,), False),
    ("pending", (ProgressStatus.PENDING,), True),
)

# Layer stripes: muted, distinct from the status fills, cycled if a
# pipeline has more layers than this.
_LAYER_COLORS = ("#8c6d46", "#5b7b8a", "#b08b2e", "#7a5c8e", "#4f7f52", "#a05a5a", "#6b7280")

_DIGEST_SEVERITY = {
    TableStatus.FAILED: 0,
    TableStatus.UPSTREAM_FAILED: 1,
    TableStatus.RETRIED_SUCCEEDED: 2,
    TableStatus.SUCCEEDED: 3,
    TableStatus.SKIPPED: 4,
}


def render_html(
    graph: Graph,
    states: Mapping[str, StateValue] | None = None,
    *,
    execution_id: int | None = None,
    title: str | None = None,
    elapsed_s: float | None = None,
    digest: RunDigest | None = None,
    theme: str = "auto",
    max_rows: int = 30,
) -> str:
    """The graph as an HTML fragment (a `<div>` with its own `<style>`),
    ready for `displayHTML` / `IPython.display.HTML` or for embedding.

    `states` maps fqn -> `NodeState` (or a bare `ProgressStatus`); a table
    without one is pending. `digest`, when given, adds the end-of-run
    summary table under the graph. `theme` is "auto" (follows the
    viewer's light/dark preference), "light" or "dark" - either way every
    colour, background included, is explicit. `max_rows` caps a column's
    height before a wave wraps into sub-columns.
    """
    if theme not in THEMES:
        raise ValueError(f"theme must be one of {THEMES}, got {theme!r}")
    if max_rows < 1:
        raise ValueError("max_rows must be at least 1")

    resolved = _resolve_states(graph, states)
    waves = compute_waves(graph)
    columns = wave_columns(graph, waves)
    layers = layer_order(graph)
    layer_color = {layer: _LAYER_COLORS[i % len(_LAYER_COLORS)] for i, layer in enumerate(layers)}
    index = {fqn: i for i, fqn in enumerate(sorted(graph.tables))}

    parts = [
        f'<div class="ptg ptg-{theme}">',
        "<style>",
        _CSS,
        _hover_css(graph, index),
        "</style>",
    ]
    parts.append(_header(graph, resolved, execution_id, title, elapsed_s, digest))
    parts.append(_legend(graph, layers, layer_color, any(s.notes for s in resolved.values())))
    parts.append(
        '<div class="ptg-caption">Columns are <b>waves</b>: a table sits in wave <i>k</i> when '
        "its longest chain of dependencies is <i>k</i> tables long, so a column's tables can run "
        "in parallel (up to <code>max_workers</code> at a time). The executor doesn't wait for a "
        "whole wave - a table starts the moment its own parents have succeeded. Hover a table "
        "for its edges and details.</div>"
    )
    parts.append(_svg(graph, columns, waves, resolved, layer_color, index, max_rows))
    if digest is not None:
        parts.append(_digest_table(digest))
    parts.append("</div>")
    return "".join(parts)


def render_html_page(
    graph: Graph,
    states: Mapping[str, StateValue] | None = None,
    *,
    refresh_s: int | None = None,
    title: str | None = None,
    **kwargs,
) -> str:
    """`render_html` wrapped in a full HTML document - for a file a browser
    opens. `refresh_s` adds `<meta http-equiv="refresh">`, so an open tab
    re-reads the file while a run is still writing it."""
    fragment = render_html(graph, states, title=title, **kwargs)
    refresh = (
        f'<meta http-equiv="refresh" content="{int(refresh_s)}">' if refresh_s is not None else ""
    )
    return (
        "<!DOCTYPE html>\n"
        '<html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"{refresh}<title>{escape(title or 'pipetree graph')}</title>"
        "<style>html,body{margin:0;padding:0;background:#f6f7f9}"
        "@media (prefers-color-scheme: dark){html,body{background:#14171b}}</style>"
        f"</head><body>{fragment}</body></html>\n"
    )


# --- pieces -----------------------------------------------------------------


def _resolve_states(graph: Graph, states: Mapping[str, StateValue] | None) -> dict[str, NodeState]:
    resolved: dict[str, NodeState] = {}
    for fqn in graph.tables:
        value = (states or {}).get(fqn)
        if value is None:
            resolved[fqn] = NodeState()
        elif isinstance(value, NodeState):
            resolved[fqn] = value
        else:
            resolved[fqn] = NodeState(ProgressStatus(value))
    return resolved


def _header(
    graph: Graph,
    states: dict[str, NodeState],
    execution_id: int | None,
    title: str | None,
    elapsed_s: float | None,
    digest: RunDigest | None,
) -> str:
    counts = Counter(state.status for state in states.values())
    meta = []
    if execution_id is not None:
        meta.append(f"execution <b>{escape(str(execution_id))}</b>")
    if digest is not None:
        outcome = "SUCCEEDED" if digest.succeeded else "FAILED"
        meta.append(f'<span class="ptg-outcome ptg-{outcome.lower()}">{outcome}</span>')
    elif any(s in counts for s in (ProgressStatus.RUNNING, ProgressStatus.RETRYING)):
        meta.append("in progress")
    if elapsed_s is not None:
        meta.append(f"elapsed {escape(_fmt_seconds(elapsed_s))}")
    meta.append(f"{len(graph.tables)} tables")

    chips = []
    for label, statuses, always in _COUNT_GROUPS:
        n = sum(counts[s] for s in statuses)
        if n or always:
            glyph = _STATUS[statuses[0]][0]
            chips.append(
                f'<span class="ptg-chip ptg-s-{statuses[0].value}">'
                f'<span class="ptg-glyph">{glyph}</span> {n} {label}</span>'
            )
    noted = sum(bool(state.notes) for state in states.values())
    if noted:
        chips.append(
            f'<span class="ptg-chip ptg-chip-notes"><span class="ptg-glyph">{_NOTE_GLYPH}</span>'
            f" {noted} with notes</span>"
        )
    return (
        '<div class="ptg-head">'
        f'<div class="ptg-title">{escape(title or "pipetree")}</div>'
        f'<div class="ptg-meta">{" · ".join(meta)}</div>'
        f'<div class="ptg-counts">{"".join(chips)}</div>'
        "</div>"
    )


def _legend(
    graph: Graph, layers: list[str], layer_color: dict[str, str], any_notes: bool = False
) -> str:
    status_items = "".join(
        f'<span class="ptg-key"><span class="ptg-swatch ptg-s-{status.value}">{glyph}</span>'
        f"{escape(label)}</span>"
        for status, (glyph, label) in _STATUS.items()
    )
    per_layer = Counter(layer_of(fqn) for fqn in graph.tables)
    layer_items = "".join(
        f'<span class="ptg-key"><span class="ptg-stripe" style="background:{layer_color[layer]}">'
        f'</span>{escape(layer)} <span class="ptg-dim">{per_layer[layer]}</span></span>'
        for layer in layers
    )
    notes_item = (
        f'<div><span class="ptg-key ptg-key-notes"><span class="ptg-badge-key">{_NOTE_GLYPH}</span>'
        "merge changed data - hover for notes</span></div>"
        if any_notes
        else ""
    )
    return (
        '<div class="ptg-legend">'
        f'<div><span class="ptg-legend-title">status</span>{status_items}</div>'
        f'<div><span class="ptg-legend-title">layer</span>{layer_items}</div>'
        f"{notes_item}"
        "</div>"
    )


def _svg(
    graph: Graph,
    columns: list[list[str]],
    waves: dict[str, int],
    states: dict[str, NodeState],
    layer_color: dict[str, str],
    index: dict[str, int],
    max_rows: int,
) -> str:
    positions: dict[str, tuple[float, float]] = {}
    headers: list[str] = []
    x = float(_PAD)
    tallest = 0

    for k, column in enumerate(columns):
        n_sub = max(1, math.ceil(len(column) / max_rows))
        rows = max(1, math.ceil(len(column) / n_sub))
        tallest = max(tallest, rows)
        for i, fqn in enumerate(column):
            sub, row = divmod(i, rows)
            positions[fqn] = (
                x + sub * (_CELL_W + _SUBCOL_GAP),
                _PAD + _HEAD_H + row * (_CELL_H + _ROW_GAP),
            )
        noun = "table" if len(column) == 1 else "tables"
        tally = _wave_tally(column, states)
        headers.append(
            f'<text class="ptg-wave" x="{x:.1f}" y="{_PAD + 13}">wave {k} · {len(column)} {noun}'
            "</text>"
            f'<text class="ptg-wave-sub" x="{x:.1f}" y="{_PAD + 28}">{tally}</text>'
        )
        x += n_sub * _CELL_W + (n_sub - 1) * _SUBCOL_GAP + _WAVE_GAP

    width = max(x - _WAVE_GAP + _PAD, 2 * _PAD + _CELL_W)
    height = _PAD + _HEAD_H + tallest * (_CELL_H + _ROW_GAP) - _ROW_GAP + _PAD

    edge_parts = []
    for child in sorted(graph.tables):
        for parent in sorted(graph.edges.get(child, ())):
            if parent not in positions:
                continue
            px, py = positions[parent]
            cx, cy = positions[child]
            x1, y1 = px + _CELL_W, py + _CELL_H / 2
            x2, y2 = cx, cy + _CELL_H / 2
            bend = max(24.0, (x2 - x1) / 2)
            edge_parts.append(
                f'<path class="ptg-edge e{index[parent]} e{index[child]}" '
                f'data-from="{escape(parent)}" data-to="{escape(child)}" '
                f'd="M{x1:.1f},{y1:.1f} C{x1 + bend:.1f},{y1:.1f} {x2 - bend:.1f},{y2:.1f} '
                f'{x2:.1f},{y2:.1f}"/>'
            )

    node_parts = []
    for column in columns:
        for fqn in column:
            node_parts.append(
                _node(graph, fqn, positions[fqn], waves[fqn], states[fqn], layer_color, index)
            )

    return (
        '<div class="ptg-scroll">'
        f'<svg class="ptg-svg" width="{width:.0f}" height="{height:.0f}" '
        f'viewBox="0 0 {width:.0f} {height:.0f}" role="img" '
        f'aria-label="dependency graph, {len(graph.tables)} tables in {len(columns)} waves">'
        f"{''.join(headers)}"
        f'<g class="ptg-edges">{"".join(edge_parts)}</g>'
        f"{''.join(node_parts)}"
        "</svg></div>"
    )


def _node(
    graph: Graph,
    fqn: str,
    pos: tuple[float, float],
    wave: int,
    state: NodeState,
    layer_color: dict[str, str],
    index: dict[str, int],
) -> str:
    x, y = pos
    layer = layer_of(fqn)
    glyph, label = _STATUS[state.status]
    short = fqn.split(".", 1)[1] if "." in fqn else fqn
    label_chars = _LABEL_CHARS_NOTED if state.notes else _LABEL_CHARS
    if len(short) > label_chars:
        short = short[: label_chars - 1] + "…"

    tip = [fqn, f"status: {label}" + (f" (attempt {state.attempt})" if state.attempt else "")]
    if state.duration_ms is not None:
        tip.append(f"duration: {_fmt_seconds(state.duration_ms / 1000)}")
    tip.append(f"wave {wave} · layer {layer}")
    parents = sorted(graph.edges.get(fqn, ()))
    tip.append("depends on: " + (", ".join(parents) if parents else "nothing (a root)"))
    if state.error_type or state.error_message:
        tip.append(f"error: {state.error_type or 'Error'}: {state.error_message or ''}")
    if state.notes:
        tip.append("notes:")
        tip.extend(f"  {note}" for note in state.notes)
    noted_attr = f' data-notes="{len(state.notes)}"' if state.notes else ""
    badge = (
        f'<text class="ptg-badge" x="{_CELL_W - 15}" y="14">{_NOTE_GLYPH}</text>'
        if state.notes
        else ""
    )

    return (
        f'<g class="ptg-node n{index[fqn]} ptg-s-{state.status.value}" '
        f'data-fqn="{escape(fqn)}" data-status="{state.status.value}" data-wave="{wave}" '
        f'data-layer="{escape(layer)}"{noted_attr} transform="translate({x:.1f},{y:.1f})">'
        f"<title>{escape(chr(10).join(tip))}</title>"
        f'<rect class="ptg-cell" width="{_CELL_W}" height="{_CELL_H}" rx="4"/>'
        f'<rect class="ptg-layer" x="2" y="3" width="3" height="{_CELL_H - 6}" rx="1.5" '
        f'fill="{layer_color[layer]}"/>'
        f'<text class="ptg-glyph-t" x="10" y="14">{glyph}</text>'
        f'<text class="ptg-label" x="24" y="14">{escape(short)}</text>'
        f"{badge}"
        "</g>"
    )


def _wave_tally(column: list[str], states: dict[str, NodeState]) -> str:
    counts = Counter(states[fqn].status for fqn in column)
    if set(counts) == {ProgressStatus.PENDING}:
        return ""
    parts = []
    for _label, statuses, _always in _COUNT_GROUPS:
        n = sum(counts[s] for s in statuses)
        if n:
            parts.append(f"{_STATUS[statuses[0]][0]} {n}")
    return "  ".join(parts)


def _digest_table(digest: RunDigest) -> str:
    rows = sorted(
        digest.results.values(), key=lambda r: (_DIGEST_SEVERITY.get(r.status, 9), r.table_fqn)
    )
    problems = sum(r.status in (TableStatus.FAILED, TableStatus.UPSTREAM_FAILED) for r in rows)
    outcome = "SUCCEEDED" if digest.succeeded else "FAILED"
    any_notes = any(r.notes for r in rows)
    body = []
    for r in rows:
        status = progress_status(r.status)
        glyph = _STATUS[status][0]
        error = f"{r.error_type}: {r.error_message}" if r.error_type or r.error_message else ""
        body.append(
            f'<tr><td><span class="ptg-swatch ptg-s-{status.value}">{glyph}</span>'
            f"{escape(r.status.value)}</td><td>{escape(r.table_fqn)}</td>"
            f'<td class="ptg-num">{r.attempts}</td>'
            f'<td class="ptg-num">{escape(_fmt_seconds(r.duration_ms / 1000))}</td>'
            f'<td class="ptg-err">{escape(error)}</td>'
            f"{_notes_cell(r) if any_notes else ''}</tr>"
        )
    return (
        f'<details class="ptg-digest"{" open" if problems else ""}>'
        f"<summary>Run digest - {outcome} (exit code {digest.exit_code}), "
        f"{len(rows)} tables, {problems} failed or upstream_failed</summary>"
        "<table><thead><tr><th>status</th><th>table</th><th>attempts</th><th>duration</th>"
        f"<th>error</th>{'<th>notes</th>' if any_notes else ''}</tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table></details>"
    )


def _notes_cell(result: TableResult) -> str:
    return f'<td class="ptg-notes">{escape("; ".join(result.notes))}</td>'


def _fmt_seconds(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    whole = int(round(seconds))
    hours, rest = divmod(whole, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m {secs:02d}s"


def _hover_css(graph: Graph, index: dict[str, int]) -> str:
    """One rule per table that has edges: hovering it lights up its own
    edges. Must come after the dimming rule in `_CSS` (same specificity)."""
    rules = [
        f".ptg-svg:has(.n{i}:hover) .e{i}{{stroke:var(--edge-hi);stroke-opacity:.95;"
        "stroke-width:1.8}"
        for fqn, i in sorted(index.items(), key=lambda item: item[1])
        if graph.edges.get(fqn) or graph.reverse_edges.get(fqn)
    ]
    return "".join(rules)


# Colours: status hues from the Okabe-Ito colour-blind-safe set (blue,
# orange, bluish green, vermillion, reddish purple), as light fills with a
# strong border; the dark theme swaps in deep fills with the same borders.
_LIGHT_VARS = (
    "--bg:#ffffff;--fg:#1f2328;--muted:#5f6670;--line:#d0d7de;--edge:#57606a;--edge-hi:#0b5cad;"
    "--pending-f:#f1f3f5;--pending-s:#adb5bd;--running-f:#dcebf8;--running-s:#0072b2;"
    "--retrying-f:#fdecc8;--retrying-s:#b97800;--succeeded-f:#d4f0e5;--succeeded-s:#00876a;"
    "--retried-f:#d4f0e5;--retried-s:#b97800;--failed-f:#fbd5c4;--failed-s:#c24100;"
    "--upstream-f:#f5e0ec;--upstream-s:#a8558a;--skipped-f:#ffffff;--skipped-s:#c3c9cf;"
    "--note:#9a5b00;--note-bd:#d98e04;"
)
_DARK_VARS = (
    "--bg:#1b1f24;--fg:#e6e8eb;--muted:#9aa3ad;--line:#3a4048;--edge:#9aa3ad;--edge-hi:#5cb6ff;"
    "--pending-f:#2a2f36;--pending-s:#5b636d;--running-f:#0f3554;--running-s:#56b4e9;"
    "--retrying-f:#4a3608;--retrying-s:#e69f00;--succeeded-f:#0f3d31;--succeeded-s:#2fbf95;"
    "--retried-f:#0f3d31;--retried-s:#e69f00;--failed-f:#55210a;--failed-s:#ff7a33;"
    "--upstream-f:#42213a;--upstream-s:#d98cbc;--skipped-f:#1b1f24;--skipped-s:#4b525b;"
    "--note:#ffc24d;--note-bd:#e69f00;"
)

_CSS = (
    f".ptg{{{_LIGHT_VARS}}}"
    f".ptg.ptg-dark{{{_DARK_VARS}}}"
    f"@media (prefers-color-scheme: dark){{.ptg.ptg-auto{{{_DARK_VARS}}}}}"
    ".ptg{background:var(--bg);color:var(--fg);border:1px solid var(--line);border-radius:8px;"
    "padding:12px 14px;font:13px/1.4 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,"
    "Helvetica,Arial,sans-serif;box-sizing:border-box;max-width:100%;text-align:left}"
    ".ptg *{box-sizing:border-box}"
    ".ptg-head{display:flex;flex-wrap:wrap;align-items:baseline;gap:6px 16px}"
    ".ptg-title{font-weight:600;font-size:15px}"
    ".ptg-meta{color:var(--muted)}.ptg-meta b{color:var(--fg)}"
    ".ptg-outcome{font-weight:700}.ptg-succeeded{color:var(--succeeded-s)}"
    ".ptg-failed{color:var(--failed-s)}"
    ".ptg-counts{display:flex;flex-wrap:wrap;gap:6px;width:100%}"
    ".ptg-chip{border:1px solid;border-radius:10px;padding:0 8px;font-size:12px;"
    "font-variant-numeric:tabular-nums;color:var(--fg)}"
    ".ptg-legend{display:flex;flex-wrap:wrap;gap:4px 18px;margin:8px 0 4px;font-size:12px}"
    ".ptg-legend>div{display:flex;flex-wrap:wrap;align-items:center;gap:4px 10px}"
    ".ptg-legend-title{color:var(--muted);text-transform:uppercase;font-size:10px;"
    "letter-spacing:.06em}"
    ".ptg-key{display:inline-flex;align-items:center;gap:4px}"
    ".ptg-swatch{display:inline-block;min-width:16px;height:14px;line-height:12px;border:1px solid;"
    "border-radius:3px;text-align:center;font-size:10px;margin-right:4px;color:var(--fg)}"
    ".ptg-stripe{display:inline-block;width:4px;height:14px;border-radius:2px}"
    ".ptg-dim{color:var(--muted)}"
    ".ptg-caption{color:var(--muted);font-size:12px;margin:2px 0 6px;max-width:880px}"
    ".ptg-caption code{font-size:11px}"
    ".ptg-scroll{overflow:auto;max-width:100%;border-top:1px solid var(--line)}"
    ".ptg-svg{display:block;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}"
    ".ptg-wave{font:600 12px -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;"
    "fill:var(--fg)}"
    ".ptg-wave-sub{font-size:11px;fill:var(--muted)}"
    ".ptg-edge{fill:none;stroke:var(--edge);stroke-opacity:.22;stroke-width:1}"
    ".ptg-svg:has(.ptg-node:hover) .ptg-edge{stroke-opacity:.06}"
    ".ptg-cell{stroke-width:1.2}"
    ".ptg-label{font-size:11px;fill:var(--fg)}"
    ".ptg-glyph-t{font-size:11px;fill:var(--fg)}"
    ".ptg-node{cursor:default}.ptg-node:hover .ptg-cell{stroke-width:2.4}"
    ".ptg-s-pending .ptg-cell,.ptg-s-pending.ptg-chip,.ptg-s-pending.ptg-swatch"
    "{fill:var(--pending-f);background:var(--pending-f);stroke:var(--pending-s);"
    "border-color:var(--pending-s)}"
    ".ptg-s-pending .ptg-label{fill:var(--muted)}"
    ".ptg-s-running .ptg-cell,.ptg-s-running.ptg-chip,.ptg-s-running.ptg-swatch"
    "{fill:var(--running-f);background:var(--running-f);stroke:var(--running-s);"
    "border-color:var(--running-s)}"
    ".ptg-s-running .ptg-cell{stroke-width:2;animation:ptg-pulse 1.4s ease-in-out infinite}"
    ".ptg-s-retrying .ptg-cell,.ptg-s-retrying.ptg-chip,.ptg-s-retrying.ptg-swatch"
    "{fill:var(--retrying-f);background:var(--retrying-f);stroke:var(--retrying-s);"
    "border-color:var(--retrying-s)}"
    ".ptg-s-retrying .ptg-cell{stroke-width:2;stroke-dasharray:4 2;"
    "animation:ptg-pulse 1.4s ease-in-out infinite}"
    ".ptg-s-succeeded .ptg-cell,.ptg-s-succeeded.ptg-chip,.ptg-s-succeeded.ptg-swatch"
    "{fill:var(--succeeded-f);background:var(--succeeded-f);stroke:var(--succeeded-s);"
    "border-color:var(--succeeded-s)}"
    ".ptg-s-retried_succeeded .ptg-cell,.ptg-s-retried_succeeded.ptg-swatch"
    "{fill:var(--retried-f);background:var(--retried-f);stroke:var(--retried-s);"
    "border-color:var(--retried-s)}"
    ".ptg-s-retried_succeeded .ptg-cell{stroke-dasharray:4 2;stroke-width:1.6}"
    ".ptg-s-failed .ptg-cell,.ptg-s-failed.ptg-chip,.ptg-s-failed.ptg-swatch"
    "{fill:var(--failed-f);background:var(--failed-f);stroke:var(--failed-s);"
    "border-color:var(--failed-s)}"
    ".ptg-s-failed .ptg-cell{stroke-width:2.2}.ptg-s-failed .ptg-label{font-weight:700}"
    ".ptg-s-upstream_failed .ptg-cell,.ptg-s-upstream_failed.ptg-chip,"
    ".ptg-s-upstream_failed.ptg-swatch"
    "{fill:var(--upstream-f);background:var(--upstream-f);stroke:var(--upstream-s);"
    "border-color:var(--upstream-s)}"
    ".ptg-s-upstream_failed .ptg-cell{stroke-dasharray:2 2}"
    ".ptg-s-skipped .ptg-cell,.ptg-s-skipped.ptg-chip,.ptg-s-skipped.ptg-swatch"
    "{fill:var(--skipped-f);background:var(--skipped-f);stroke:var(--skipped-s);"
    "border-color:var(--skipped-s)}"
    ".ptg-s-skipped .ptg-cell{stroke-dasharray:3 3}.ptg-s-skipped .ptg-label{fill:var(--muted)}"
    "@keyframes ptg-pulse{0%,100%{stroke-opacity:1}50%{stroke-opacity:.3}}"
    "@media (prefers-reduced-motion: reduce){.ptg-cell{animation:none!important}}"
    # The badge is amber text, separate from the status fill: a succeeded
    # table with notes stays green.
    ".ptg-badge{font-size:12px;font-weight:700;fill:var(--note)}"
    ".ptg-chip-notes{border-color:var(--note-bd);color:var(--note)}"
    ".ptg-badge-key{color:var(--note);font-weight:700}"
    ".ptg-notes{color:var(--note)!important}"
    ".ptg-digest{margin-top:10px}.ptg-digest summary{cursor:pointer;font-weight:600}"
    ".ptg-digest table{border-collapse:collapse;margin-top:6px;font-size:12px;width:100%}"
    ".ptg-digest th,.ptg-digest td{border-bottom:1px solid var(--line);padding:2px 8px;"
    "text-align:left;vertical-align:top;color:var(--fg);background:var(--bg)}"
    ".ptg-digest th{color:var(--muted);font-weight:600}"
    ".ptg-num{text-align:right!important;font-variant-numeric:tabular-nums}"
    ".ptg-err{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;"
    "white-space:pre-wrap;word-break:break-word;max-width:640px}"
)
