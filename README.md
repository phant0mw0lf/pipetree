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

`uv run pipetree graph --config <path> [--format text|mermaid|html]` prints
the dependency tree - `--format mermaid` on the example config reproduces
part 1's own diagram; `--format html` is the picture described in
[Seeing the run](#seeing-the-run).

A run can be scoped instead of running everything: `--select
bronze.orders,silver.orders` runs just those tables (skipping the rest);
add `--with-dependents` to extend that to the full descendant closure -
the CI/CD mode from part 1, where a changed table's dependents get rebuilt
too. `--with-ancestors` is the mirror image: everything the selection
transitively reads, so `--select gold.fact_sales --with-ancestors` rebuilds one
output together with all its upstream tables (combine both flags for both
directions). `--init` is the run-level full-reload parameter: every selected table
is seeded from scratch rather than merged against what's already there.

## Seeing the run

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
`LiveGraphView(backend="ipython" | "clear" | "text")`. The in-place path is
verified in a real Jupyter kernel; **on Databricks and Fabric it is not yet
verified in a real workspace** (both run Python notebooks on an IPython
kernel, and Databricks documents `update_display` as working within the
current cell - which is where the run happens).

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
`examples/graph_snapshots.py <pipeline.yaml> <out-dir>` writes the
all-pending picture and a **simulated** mid-run one (first layer done,
half the second, a few running, one failure with its descendants
upstream_failed) - no Spark needed.

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
  fqns to run; `render.py` draws the tree as text or Mermaid, `html.py`
  as a self-contained HTML picture laid out in waves (`waves.py`).
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
- **Unknown member** - `unknown_member: true` seeds one row whose business
  key columns each hold a sentinel of their own type, for facts to resolve
  a missing foreign key to: `-1` for a numeric key, SQL `NULL` for a
  string, `1900-01-01` for a date or timestamp (other key types are
  rejected; a composite key gets one sentinel per column). A fact reaches
  a `NULL`-keyed unknown member with a null-safe join (`<=>` /
  `eqNullSafe`); a plain `=` join behaves exactly as before for every
  matched row. Seeding is null-safe and idempotent. On such a table a
  source row with a `NULL` in any business key column is dropped (logged
  as a warning, counted as `null_keys_dropped` in the run log) - `NULL`
  is reserved for the unknown member there.
- **Surrogate keys** - `surrogate_key: <column>` on an `scd1`/`scd2`
  dimension adds a Kimball surrogate key: a Delta identity column
  (`BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1)`),
  first in the table, created with it. See [Surrogate keys](#surrogate-keys).
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
- **Progress** (`pipetree.executor.events`, `pipetree.notebook`) - an
  optional `RunObserver` sees the run while it happens (per-table
  running/retrying/finished events); `LiveGraphView` and
  `HtmlFileObserver` turn that into the live picture. See
  [Seeing the run](#seeing-the-run).
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

## Surrogate keys

Dimensions keep their natural `business_key`. `surrogate_key` adds a
generated integer key alongside it:

```yaml
gold:
  tables:
    dim_product:
      logic: notebooks/gold/dim_product.sql
      business_key: [product_code]
      strategy: scd2
      unknown_member: true
      surrogate_key: product_sid
```

- pipetree creates the table on its first run with `product_sid` as a
  Delta identity column (`GENERATED BY DEFAULT`), first in the table.
  The source never supplies it.
- With `scd1`, a new business key gets a new sid, and a changed row is
  updated in place and keeps its sid.
- With `scd2`, every new version is an insert and gets a new sid. Closing a
  version never touches a sid, so a fact row keeps pointing at the version
  that was current when it loaded.
- The unknown member (`unknown_member: true`) gets sid `-1`, plus the usual
  business-key sentinel (`-1`, `NULL` for strings, `1900-01-01`). The
  explicit `-1` doesn't disturb generation (`1, 2, 3, ...`).
- Generated sids are unique and increase from load to load, but they
  aren't gapless. Delta hands out identity values in blocks per write task,
  so a parallel load can skip numbers. That's harmless for a surrogate key.
  The same applies on Databricks.
- A second identical run neither renumbers nor duplicates anything.
  `init` reloads the rows but keeps the identity high-water mark, so a sid
  is never reused for a different row.

Facts LEFT JOIN the dimension on its business key (plus
`_is_current = true` for an `scd2` dimension) and take its sid. A missing
or unmatched key falls back to `-1`:

```sql
SELECT f.sale_id,
       COALESCE(d.product_sid, -1) AS product_sid
FROM silver.sales f
LEFT JOIN gold.dim_product d
  ON f.product_code = d.product_code AND d._is_current = true
```

Matching on `=` and coalescing the sid to `-1` resolves a NULL foreign key
to the unknown member without a null-safe join. You only need `<=>` if you
want to join *to* a string dimension's NULL-keyed unknown row itself.

Rules, checked at config load:
- `surrogate_key` is valid only for `scd1`/`scd2` with a `business_key`.
  `replace` would renumber every sid on each run, and `append` has no key.
- The name can't be a business key column, an audit column or
  `__pipetree_is_delete__`.

At run time:
- A source that already has a column of that name is an error. Leave it out
  (`SELECT * EXCEPT (product_sid) ...`) or rename it.
- A table created before `surrogate_key` was set fails with a clear message.
  Delta can't add an identity column to an existing table, so you have to
  drop and recreate it.
- Downstream tables that `SELECT *` from the dimension just carry
  `product_sid` as an ordinary column.

Identity columns need Delta Lake 3.3+ (verified on delta-spark 4.0.1, the
local engine, through the `DeltaTable` builder; OSS's SQL `CREATE TABLE`
rejects the identity clause) or Databricks Runtime 10.4+ (SQL DDL).
**Fabric Runtime 1.3 (Spark 3.5, Delta 3.2) has no identity columns, and
Microsoft's Delta Lake interoperability page ("Current limitations") lists
"Identity columns writing" as unsupported in Fabric without naming a runtime -
so treat Fabric, including Runtime 2.0 (Spark 4.1, Delta 4.2), as unsupported
until tested.** On an engine without them, the first run fails with an error
naming this requirement rather than a raw Spark error. AUTO CDC translation
(`pipetree.declarative`) refuses `surrogate_key` instead of dropping it.

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
