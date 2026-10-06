"""Write two HTML pictures of a pipeline's dependency tree, no Spark needed:

- `<out>/pipeline-graph.html`        - every table pending (what `pipetree
  graph --format html` prints)
- `<out>/pipeline-graph-midrun.html` - a *simulated* mid-run state: the
  first layer done, half of the second done, a couple of tables running,
  one failed and its descendants upstream_failed. Nothing is executed;
  this is the picture a live run would show at that moment.

    uv run python examples/graph_snapshots.py examples/pipeline.yaml /tmp
"""

from __future__ import annotations

import sys
from pathlib import Path

from pipetree.config.loader import load_config
from pipetree.executor.events import ProgressStatus
from pipetree.graph.builder import build_graph
from pipetree.graph.html import NodeState, render_html_page
from pipetree.graph.select import descendant_closure
from pipetree.graph.waves import layer_of, layer_order
from pipetree.model import PipelineConfig


def simulated_midrun(graph) -> dict[str, NodeState]:
    layers = layer_order(graph)
    first = sorted(f for f in graph.tables if layer_of(f) == layers[0])
    second = sorted(f for f in graph.tables if len(layers) > 1 and layer_of(f) == layers[1])

    states: dict[str, NodeState] = {}
    for i, fqn in enumerate(first):
        status = ProgressStatus.RETRIED_SUCCEEDED if i % 29 == 7 else ProgressStatus.SUCCEEDED
        attempt = 2 if status == ProgressStatus.RETRIED_SUCCEEDED else 1
        states[fqn] = NodeState(status, attempt=attempt, duration_ms=800 + (i * 137) % 9000)

    done = second[: len(second) // 2]
    for i, fqn in enumerate(done):
        states[fqn] = NodeState(
            ProgressStatus.SUCCEEDED, attempt=1, duration_ms=1500 + (i * 211) % 20000
        )

    # A failure among the finished half that actually has descendants, so
    # the upstream_failed cascade shows; prefer the one with the most.
    failed = max(
        done or first,
        key=lambda f: (len(descendant_closure(graph, {f})), f),
    )
    states[failed] = NodeState(
        ProgressStatus.FAILED,
        attempt=1,
        duration_ms=4210,
        error_type="AnalysisException",
        error_message="[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column with name `region_id` "
        "cannot be resolved. (simulated)",
    )
    for fqn in descendant_closure(graph, {failed}) - {failed}:
        states[fqn] = NodeState(ProgressStatus.UPSTREAM_FAILED)

    rest = [f for f in second[len(second) // 2 :] if f not in states]
    for fqn in rest[:3]:
        states[fqn] = NodeState(ProgressStatus.RUNNING, attempt=1)
    if len(rest) > 3:
        states[rest[3]] = NodeState(
            ProgressStatus.RETRYING,
            attempt=1,
            error_type="Throttled",
            error_message="429 Too Many Requests (simulated)",
        )
    return states


def main(config: str, out_dir: str) -> None:
    config_path = Path(config)
    raw = load_config(config_path)
    graph = build_graph(PipelineConfig.from_validated_raw(raw), base_dir=config_path.parent)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    pending = out / "pipeline-graph.html"
    pending.write_text(
        render_html_page(graph, title=f"{config_path.name} - dependency tree"), encoding="utf-8"
    )

    midrun = out / "pipeline-graph-midrun.html"
    midrun.write_text(
        render_html_page(
            graph,
            simulated_midrun(graph),
            title=f"{config_path.name} - simulated mid-run",
            execution_id=260927101500123,
            elapsed_s=754.0,
        ),
        encoding="utf-8",
    )
    print(pending)
    print(midrun)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: graph_snapshots.py <pipeline.yaml> <out-dir>")
    main(sys.argv[1], sys.argv[2])
