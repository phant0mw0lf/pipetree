# pipetree

Metadata-driven pipeline orchestration: describe *what* to move in declarative
YAML, and a small package turns it into a runnable dependency tree.

`YAML → config loader → typed model → graph builder → executor`

This is the companion package to the
[Pipeline Engineering](https://datadave.dev/tags/pipeline-engineering/) blog
series. Part 1 covers the design and the YAML schema; part 2 covers the
executor this repo implements (multithreading, retries, failure handling).

Status: Phase A (part 2's core) and Phase B (selection, subtree closure,
tree rendering) are complete. See `docs/build-order.md` for what's next -
Phase C (platform adapters) is the one that needs a real Databricks/Fabric
workspace to verify.

## Prerequisites

- Python ≥ 3.11 (developed against 3.12) and [`uv`](https://docs.astral.sh/uv/).
- A JDK for PySpark local mode - e.g. `brew install openjdk@17`, then point
  `JAVA_HOME` at it:
  ```bash
  export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
  export PATH="$JAVA_HOME/bin:$PATH"
  ```

## Quick start

```bash
uv sync --extra spark --dev
uv run pipetree run --config examples/pipeline.yaml
```

This runs the example pipeline from part 1 (bronze customer/orders/employee →
silver → gold dim_customer/fact_sales) against local CSV seed files, writing
Delta tables under `spark-warehouse/` and a digest to the console.

To see the failure-handling behaviour part 2 is actually about - a retry
that recovers, a table that fails and takes its dependents `upstream_failed`
with it, and an independent branch that finishes anyway - run the
fault-injecting demo instead:

```bash
uv run python examples/run_demo.py
```

A captured transcript of that run, annotated, is in `examples/demo-run.txt`.

`uv run pipetree validate --config <path>` checks a config without running
anything - useful in CI before a deploy.

`uv run pipetree graph --config <path> [--format text|mermaid]` prints the
dependency tree - `--format mermaid` on the example config reproduces part
1's own diagram.

A run can be scoped instead of running everything: `--select
bronze.orders,silver.orders` runs just those tables (skipping the rest);
add `--with-dependents` to extend that to the full descendant closure -
the CI/CD mode from part 1, where a changed table's dependents get rebuilt
too. `--init` is the run-level full-reload parameter: every selected table
is seeded from scratch rather than merged against what's already there.

## Architecture

See the blog series for the full design rationale. In short:

- **Config loader** (`pipetree.config`) — parses and validates the YAML,
  failing fast with an error that names the offending key
  (`silver.tables.orders.merge.delete_mode: ...`).
- **Typed model** (`pipetree.model`) — defaults resolved, every table given a
  fully qualified name from `table_schema` (defaulting to its layer) + its
  block key.
- **Graph builder** (`pipetree.graph`) — derives the dependency tree (`auto`
  parses SQL `FROM`/`JOIN` or PySpark `spark.table(...)`/`spark.sql(...)`
  literals; `depends_on` lists are matched by fqn or, if unambiguous, by bare
  table name), sorts it topologically, and reports a cycle's full path if it
  finds one. `select.py` resolves `--select`/`--with-dependents` to a set of
  fqns to run; `render.py` draws the tree as text or Mermaid.
- **Executor** (`pipetree.executor`) — walks the dependency tree (not the
  layers) with a thread pool and a ready queue: a table starts the instant
  its parents have succeeded. Retries transient errors only (throttling,
  timeouts, connection resets) with exponential backoff and jitter, marks
  every descendant of a failed table `upstream_failed` instead of stopping
  the run, and marks anything outside a `--select` scope `skipped`
  (skipped tables never affect the run's overall success).
- **Adapters** (`pipetree.adapters`) sit behind `run_table(table)` and know
  nothing about the engine. `SparkAdapter` (`pipetree.adapters.spark`) is the
  one real implementation - local Spark + Delta today, the same code path
  Fabric and Databricks run on in later parts - with `scd1`/`scd2`/`replace`/
  `append` all implemented against Delta's merge builder.
- **Run log & notifier** (`pipetree.runlog`) — every table's result is
  collected in memory and written once, at the end, to
  `_meta.pipeline_run_log` (a `replaceWhere` on `_execution_id`, so
  re-running the same execution id overwrites rather than duplicates); a
  pluggable `Notifier` reports the digest (`ConsoleNotifier` by default).

See `NOTES-for-blog.md` for the design decisions and trade-offs (retry
parameters, the `append` retry rule, two real Spark/Delta bugs) made while
building this.

## Development

```bash
uv run pytest              # full suite
uv run pytest -m "not spark"  # skip the JVM-backed integration tests
uv run ruff check . && uv run ruff format --check .
uv run pyright
```

## License

MIT — see `LICENSE`.
