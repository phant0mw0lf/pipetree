"""pipetree: metadata-driven pipeline orchestration.

`run_pipeline()` is the whole public API - the same function a `pipetree
run` CLI invocation, a Databricks job task, or a Fabric notebook calls.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from pipetree.config.loader import load_config
from pipetree.executor.retry import RetryPolicy
from pipetree.executor.runner import run_pipeline as _execute
from pipetree.executor.status import RunDigest
from pipetree.graph.builder import build_graph
from pipetree.graph.select import resolve_selection
from pipetree.model import PipelineConfig
from pipetree.runlog.collector import build_run_log_rows
from pipetree.runlog.notifier import ConsoleNotifier
from pipetree.runlog.writer import InMemoryRunLogWriter

if TYPE_CHECKING:
    from pipetree.adapters.base import Adapter
    from pipetree.runlog.notifier import Notifier
    from pipetree.runlog.writer import RunLogWriter

__version__ = "0.1.0"

__all__ = ["__version__", "run_pipeline"]

_logger = logging.getLogger("pipetree")


def run_pipeline(
    config_path: str | Path,
    *,
    adapter: Adapter | None = None,
    execution_id: int | None = None,
    max_workers: int = 4,
    retry_policy: RetryPolicy | None = None,
    run_log_writer: RunLogWriter | None = None,
    notifier: Notifier | None = None,
    select: list[str] | None = None,
    with_dependents: bool = False,
    init: bool = False,
) -> RunDigest:
    """Load `config_path`, build the dependency graph, and run it.

    With no `adapter`, this builds a local Spark + Delta session and a
    `SparkAdapter` around it - the one real engine, same code path Fabric
    and Databricks run on. Pass `adapter` to use a different one (a
    fault-injecting wrapper for a demo, or a platform adapter later).

    `select` runs only the named tables (by fqn or unambiguous bare name);
    `with_dependents` extends that to the full descendant closure - the
    CI/CD mode from part 1, where a changed table's dependents get rebuilt
    too. `init` is the run-level "full reload" parameter.
    """
    config_path = Path(config_path)
    raw = load_config(config_path)
    config = PipelineConfig.from_validated_raw(raw)
    graph = build_graph(config, base_dir=config_path.parent)

    selected = resolve_selection(graph, select, with_dependents)
    if selected is not None and not with_dependents:
        _logger.warning(
            "running a subtree in isolation (--select without --with-dependents) can leave "
            "_execution_id out of step across the tree, depending on how the incremental "
            "load resolves it"
        )

    if adapter is None:
        adapter = _default_spark_adapter(config, config_path.parent)

    digest = _execute(
        graph,
        adapter,
        execution_id=execution_id,
        max_workers=max_workers,
        retry_policy=retry_policy or RetryPolicy(),
        selected=selected,
        init=init,
    )

    writer = run_log_writer or InMemoryRunLogWriter()
    writer.write(build_run_log_rows(digest, graph), execution_id=digest.execution_id)

    (notifier or ConsoleNotifier()).notify(digest)

    return digest


def _default_spark_adapter(config: PipelineConfig, base_dir: Path) -> Adapter:
    # Imported lazily: pyspark is the optional 'spark' extra, and code that
    # supplies its own adapter (tests, a future platform adapter) shouldn't
    # have to install it.
    from pipetree.adapters.spark.adapter import SparkAdapter
    from pipetree.adapters.spark.session import build_local_session

    spark = build_local_session()
    return SparkAdapter(spark, systems=config.systems, base_dir=base_dir)
