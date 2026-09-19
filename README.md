# pipetree

Metadata-driven pipeline orchestration: describe *what* to move in declarative
YAML, and a small package turns it into a runnable dependency tree.

`YAML → config loader → typed model → graph builder → executor`

This is the companion package to the
[Pipeline Engineering](https://datadave.dev/tags/pipeline-engineering/) blog
series. Part 1 covers the design and the YAML schema; part 2 covers the
executor this repo implements (multithreading, retries, failure handling).

Status: under active development, following the build order in
`docs/build-order.md` (part 2's core lands first).

## Quick start

```bash
uv sync --extra spark
uv run pipetree run --config examples/pipeline.yaml
```

## Architecture

See the blog series for the full design rationale. In short:

- **Config loader** (`pipetree.config`) — parses and validates the YAML,
  failing fast with an error that names the offending key.
- **Typed model** (`pipetree.model`) — defaults resolved, every table given a
  fully qualified name.
- **Graph builder** (`pipetree.graph`) — derives the dependency tree (`auto`
  from SQL/PySpark logic files, or explicit `depends_on`), sorts it
  topologically, and renders it.
- **Executor** (`pipetree.executor`) — walks the dependency tree (not the
  layers) with a thread pool and a ready queue, retries transient failures,
  and marks descendants of a failed table `upstream_failed` instead of
  stopping the run.
- **Adapters** (`pipetree.adapters`) sit behind `run_table(table)` and know
  nothing about the engine; **platforms** (`pipetree.platform`) know how to
  resolve secrets and name tables on Databricks, Fabric, or a laptop.

## License

MIT — see `LICENSE`.
