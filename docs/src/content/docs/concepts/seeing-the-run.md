---
title: Seeing the run
description: The picture of the tree, the live view in a notebook or as a file, and observers.
sidebar:
  order: 6
---

The package builds the tree from the edges; it can also draw it, and keep
drawing it while a run goes. The picture is one self-contained HTML string
(inline CSS + SVG - no scripts, fonts or CDNs, so it works inside a
notebook's sandboxed output iframe):

- **Waves, left to right.** A table's wave is the length of its longest
  dependency chain from a root. Everything in one column *can* run in
  parallel (bounded by `max_workers`) - that's "when what runs". The
  executor doesn't wait for a whole wave, though: a table starts the moment
  its own parents have succeeded. Within a wave tables are grouped by layer
  (the coloured stripe; the fqn's prefix), and a tall wave wraps into
  balanced sub-columns (`max_rows`, 30 by default).
- **Status** is colour (colour-blind-safe Okabe-Ito hues) *and* a glyph:
  `○` pending, `▶` running (pulsing), `↻` retrying, `✓` succeeded (dashed
  orange border if it took a retry), `✗` failed, `⊘` upstream_failed, `–`
  skipped. Hover a table for its full fqn, status, attempt, duration,
  parents and error, and to light up its edges.
- A header with the execution id, counts and elapsed time; at the end of a
  run, the digest as a table under the graph (problems first).

Notes (what a merge quietly changed) show up in the picture too: a table with notes gets an amber `⚠` badge, the hover text lists them and the digest table gets a `notes` column. See [Notes and run log](../notes-and-run-log/).

**Render it yourself** - a pure function, deterministic for the same input:

```python
from pipetree.graph.html import render_html, render_html_page

# a fragment, every table pending
html = render_html(graph)
# states by fqn (a NodeState or a bare status); anything missing is pending
html = render_html(graph, {"silver.orders": "running"}, execution_id=42, elapsed_s=12.5)
# a complete document, for a file
page = render_html_page(graph, title="nightly")
```

**Live, in a notebook cell** (Databricks, Fabric, Jupyter) - pass an
observer to `run_pipeline`:

```python
from pipetree import run_pipeline
from pipetree.notebook import LiveGraphView

view = LiveGraphView(title="nightly")
digest = run_pipeline("pipeline.yaml", observer=view)
```

It draws the whole tree (all pending) when the run starts, redraws as
tables start, retry and finish - at most once a second - and always once
more at the end, with the digest. In an IPython kernel it updates one
output in place (`display(HTML(...), display_id=True)` then
`handle.update(...)`); if that isn't available it falls back to
`clear_output(wait=True)` and a fresh display (on Databricks through
`displayHTML`), which re-renders the cell's output and clears anything else
the cell printed. With no notebook at all it prints a `[pipetree] 42/171
done · 3 running · ...` line when the counts change. Force one with
`LiveGraphView(backend="ipython" | "clear" | "text")`. The in-place path
needs an IPython kernel, which Databricks and Fabric notebooks run on
(Databricks documents `update_display` as working within the current cell,
which is where the run happens).

**Live, as a file** (anywhere - a laptop, a job's driver):

```bash
uv run pipetree run --config pipeline.yaml --live-html run.html
```

or `observer=HtmlFileObserver("run.html")` from Python. The file is
rewritten atomically (temp file + rename) on each update and carries a
`<meta http-equiv="refresh" content="2">` while the run is going, so an
open browser tab follows along; the final write drops it.

Observers are called on the thread that called `run_pipeline`, one at a
time (worker threads only queue events), and one that raises is logged
once and switched off - it can never fail or stall a run. Write your own
against `pipetree.executor.events.RunObserver` (`on_run_start`,
`on_table_event`, `on_run_end`, optional `on_idle`).

To see what a big pipeline looks like without running anything,
`examples/graph_snapshots.py <pipeline.yaml> <out-dir>` (in the repository) writes the
all-pending picture and a **simulated** mid-run one (first layer done,
half the second, a few running, one failure with its descendants
upstream_failed) - no Spark needed.
