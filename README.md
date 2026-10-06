# pipetree

Metadata-driven pipeline orchestration: describe *what* to move in declarative
YAML, and a small package turns it into a runnable dependency tree.

`YAML → config loader → typed model → graph builder → executor`

This is the companion package to the
[Pipeline Engineering](https://datadave.dev/tags/pipeline-engineering/) blog
series. Part 1 covers the design and the YAML schema; part 2 covers the
executor this repo implements (multithreading, retries, failure handling).

Status: all six phases of the original build order are done - core (part
2), graph features, the platform seam, source readers, schema drift, and
AUTO CDC translation. Phase C (Databricks/Fabric) is **not yet verified
against a real workspace** - see `examples/databricks/README.md` and
`examples/fabric/README.md`. See `docs/build-order.md` for what's left.

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
  Fabric and Databricks run on - with `scd1`/`scd2`/`replace`/`append` all
  implemented against Delta's merge builder. With no active SparkSession it
  builds a local one; on a real cluster it reuses the one already running.
- **Sources** (`pipetree.sources`) read a `source` table's raw input,
  keyed on `systems.<name>.type`: `csv`/`json`/`parquet` (local files, the
  example project's stand-in for a real connector), `sqlserver` (JDBC),
  `kusto`, `storage_stream` (Structured Streaming with
  `Trigger.AvailableNow`, so an always-on stream becomes a bounded unit of
  work the executor can run once and finish - `d365_export` and
  `synapse_link` are aliases of it) and `custom` (a dotted-path class you
  bring yourself, e.g. for Microsoft Graph).
- **Schema drift** (`pipetree.schema`) — structure is inferred, never
  declared. Before an scd1/scd2 merge, the source's schema is diffed
  against the target's (added/removed/retyped/nullability), and
  `schema_policy` (`evolve` by default, or `fail`/`ignore` per table)
  decides what happens: `evolve` applies safe numeric widenings and lets
  Delta add a genuinely new column (`withSchemaEvolution()`), never drops
  a removed one; `fail` raises before anything is written; `ignore` keeps
  only the columns already in the target. Every non-empty diff is
  recorded in the run log (`append`/`replace` don't enforce a policy -
  `replace` already means "whatever the source says, goes").
- **Audit columns** - every table gets `_inserted_at`, `_updated_at`,
  `_is_deleted`, `_execution_id`, `_source_system` (plus `_valid_from`,
  `_valid_to`, `_is_current` on `scd2`). They're reserved: any of these
  an incoming source already carries (a `SELECT *` from an upstream
  pipetree table, say) are dropped before the merge, since pipetree stamps
  the table's own. To use an upstream stamp downstream (e.g. in
  `delete_when` or `sequence_by`), alias it in the logic file
  (`_is_deleted AS upstream_is_deleted`).
- **Platforms** (`pipetree.platform`) answer "where am I running?" for
  secrets, table naming, storage paths and run metadata - `LocalPlatform`
  (env-var secrets), `DatabricksPlatform` (Unity-Catalog-backed secret
  scope by default, or a pluggable `secret_resolver` for reading Key Vault
  directly through an Access Connector; Unity Catalog naming is handled by
  setting the session's default catalog, not by rewriting every table
  reference), `FabricPlatform` (`notebookutils.credentials`, the attached
  lakehouse's `Files/` mount). Every platform also implements
  `acquire_token(resource)` - a short-lived AAD token via the platform's
  own identity, for the `sqlserver`/`kusto` sources' `auth.mode:
  aad_token` path: on Databricks, a named Unity Catalog service credential
  per resource (`dbutils.credentials.getServiceCredentialsProvider(name)`,
  DBR 16.2+); on Fabric, `notebookutils.credentials.getToken`, which runs
  as the notebook's *executing* identity (the user, the schedule owner, or
  the pipeline's last modifier - the workspace identity only when a
  pipeline Notebook activity uses a Workspace Identity connection) and
  supports a limited set of audiences (`storage`, `pbi`, `keyvault`,
  `kusto`, plus Kusto cluster URIs), so custom app audiences are
  Databricks/local only; `DefaultAzureCredential` locally. Tokens are
  minted once on the driver and not refreshed, so a read must finish
  within the token's lifetime (60-90 minutes by default). Secret-based
  auth stays the default for anyone without managed-identity infrastructure
  available; token-based auth is additive, not a replacement. `detect()`
  picks one from runtime markers.
- **Run log & notifier** (`pipetree.runlog`) — every table's result is
  collected in memory and written once, at the end, to
  `_meta.pipeline_run_log` (a `replaceWhere` on `_execution_id`, so
  re-running the same execution id overwrites rather than duplicates); a
  pluggable `Notifier` reports the digest (`ConsoleNotifier` by default).
- **Declarative pipelines** (`pipetree.declarative.autocdc`) — a pure
  translation of an `scd1`/`scd2` table into Databricks' AUTO CDC flow
  syntax (`KEYS`, `SEQUENCE BY`, `APPLY AS DELETE WHEN`,
  `STORED AS SCD TYPE`, `TRACK HISTORY ON * EXCEPT`) - the comparison part
  1 draws, made mechanical. Not an executor: AUTO CDC only runs inside a
  Lakeflow Declarative Pipeline, where the platform owns the DAG; this
  compiles to it, and doesn't attempt to run anything itself.

See `NOTES-for-blog.md` for the design decisions and trade-offs (retry
parameters, the `append` retry rule, two real Spark/Delta bugs, the
platform seam, making `Trigger.AvailableNow` behave, schema drift, AUTO
CDC) made while building this.

## Running on Databricks or Fabric

`examples/databricks/` (a Databricks Asset Bundle) and `examples/fabric/`
(a notebook) run the same example pipeline against a real workspace
instead of a laptop. **Neither has been verified against a real workspace
yet** - each README says exactly what to check before trusting it.

## Development

```bash
uv run pytest              # full suite
uv run pytest -m "not spark"  # skip the JVM-backed integration tests
uv run ruff check . && uv run ruff format --check .
uv run pyright
```

## License

MIT — see `LICENSE`.
