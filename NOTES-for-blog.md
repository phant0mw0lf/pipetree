# Notes for the blog post (part 2)

Design decisions and trade-offs from building the executor, in the order
they came up. Written for David to draw on when writing the actual post -
these are the things that mattered in practice, not a restatement of the
design doc.

## Ready queue + thread pool, not layer-by-layer

The scheduler (`executor/runner.py`) keeps one piece of state per table -
how many of its parents haven't finished yet - and submits a table to the
pool the instant that count hits zero. Layers never gate anything; a gold
table can start before a sibling bronze table if its own dependencies are
already done.

The implementation is smaller than it sounds: `concurrent.futures.wait(...,
return_when=FIRST_COMPLETED)` in a loop, decrementing the finishing table's
children's pending-parent counts, submitting whichever hits zero. All of
that state - `pending_parents`, `results`, `settled` - is only ever touched
from the one thread running that loop; worker threads just run
`adapter.run_table` and hand a `TableResult` back through their future. No
locks anywhere in the scheduler itself. That was a deliberate design choice
up front, and it held up - the only place a lock actually shows up in the
whole codebase is inside the hand-written `FakeAdapter` test double, to
make its call-recording thread-safe.

## Retry parameters

Defaults: `max_attempts=3`, `base_delay=0.5s`, `max_delay=30s`, full jitter
(`uniform(0, min(max_delay, base * 2**attempt))`). Three attempts total -
not three *retries* - felt right for a batch pipeline: enough to ride out a
throttling window, not so many that a genuinely down source holds up the
whole run for minutes. All four numbers are constructor arguments on
`RetryPolicy`, not hardcoded, because the right value depends entirely on
what you're calling - a Graph API with aggressive throttling wants a longer
`max_delay` than a JDBC connection that either works or doesn't.

Classification is a fixed tuple of exception types (`Throttled`,
`TimeoutError`, `ConnectionError`) plus an optional predicate, rather than
parsing HTTP status codes or exception messages. Every adapter is
responsible for translating its own errors into these types (or into
something the predicate recognizes) - the alternative, teaching the
executor about every source's error shapes, is exactly the kind of
per-system special-casing this whole package exists to avoid.

## The append retry decision

`scd1`/`scd2`/`replace` are idempotent merges and retry freely.  `append`
is not - a retry after a partial write duplicates rows - so it only
retries if the adapter declares `Capabilities.supports_delete_by_execution_id`
and actually deletes this run's rows (`WHERE _execution_id = <this run>`)
before the retry attempt. `SparkAdapter` declares this capability and
implements it as a single `DeltaTable.delete()` call; without a Delta-like
engine underneath, `append` on a transient error just fails, immediately,
with no retry.

The alternative I didn't take: always retry `append` and rely on a
downstream dedup pass to clean up duplicates later. Rejected because it
turns a data-correctness problem (don't duplicate rows) into a data-quality
problem (find and remove duplicates after the fact), and because it would
have made the `append` strategy behave differently depending on how
paranoid the reader downstream happened to be.

## Duplicate keys are info, not failure

Before an scd1/scd2 merge, `dedupe.py` runs a `row_number()` window over
`business_key` (breaking ties on `sequence_by`, descending) and keeps rank
1. This isn't a data-quality gate - a source that delivers two changes for
the same key in one batch is normal, not broken, and Delta's MERGE simply
errors on a multi-match (`UnsupportedOperationException` from Delta's
engine, not something worth surfacing to a reader). The dropped count
surfaces as `duplicates_dropped` in the run log, logged at INFO. Without
`sequence_by` the winner is still deterministic for a given run (a stable
tie-break via `monotonically_increasing_id()`) but arbitrary - there's no
way to tell right from wrong without a sequence column, so I didn't try.

## The two real bugs that cost real time

Both were Spark/Delta specifics, not pipetree logic, and both are worth a
sentence in the post because they're the kind of thing that only shows up
once you actually run the thing against a real engine instead of reasoning
about it on paper.

**Delta merge invalidates the DataFrame cache for its own target table.**
scd2 needs to know, before the merge runs, which source rows represent a
real change against the *current* row - because the merge is about to close
that current row out, and a lazy comparison re-evaluated afterward would
see the row already closed. The obvious fix - read the current rows,
`.cache()` them, then use the cached copy after the merge - doesn't work:
writing to a cataloged table invalidates Spark's cache for anything
that reads from it, cache included, so the "cached" comparison silently
recomputes against the post-merge state the moment it's touched again.
Correct answer (`merge.py`, `merge_scd2`): `.collect()` the comparison to
the driver *before* the merge executes, so there's no lazy plan left to
reevaluate. Fine at demo scale; a real multi-million-row dimension table
would need something better (materializing to a temp Delta table instead
of the driver, most likely) - flagged for whoever touches this in parts
3/4.

**An untyped `None` literal breaks a DataFrame reused across two actions.**
`audit.py` stamps `_source_system` via `F.lit(source_system)`, and a
logic-based table (no `source`, so no system to stamp) passes `None`.
`F.lit(None)` alone is untyped (`NullType`), and a DataFrame built with it
that gets used for both `.count()` and `.write()` - two separate
re-analyses of the same logical plan - hit an internal Catalyst error
(`Couldn't find _source_system#N in [...]`) on the second one. Fixed by
casting explicitly: `F.lit(source_system).cast("string")`. The lesson
generalizes: don't leave a `lit(None)` untyped if the DataFrame carrying it
survives past the first action.

## Local file source as a stand-in

The `SourceReader` registry (sqlserver, kusto, storage_stream, custom) is
Phase D. For this phase's demo, `SparkAdapter` has one built-in reader:
`systems.<name>.type: csv|json|parquet` with a `path`, reading a local
file relative to the config. It's not meant to be more than a stand-in -
`object` on the source block is accepted but currently ignored by it
(it's part of the schema for when a real reader needs it) - but it kept
the demo honest: real Spark reads, real Delta writes, no mocking of the
adapter seam itself.

## PySpark logic files: the `result` convention

Part 1 shows `.sql` files as one implicit `SELECT`, but doesn't pin down
how a `.py` logic file hands its output back - a notebook's last-expression
`display()` behavior doesn't exist in a plain `exec()`. Landed on: the file
must assign its final DataFrame to a module-level variable named `result`.
It's the smallest convention that works, mirrors "one output table per
file" from the dependency-parsing rules, and is exactly what
`examples/notebooks/gold/fact_sales.py` demonstrates.

## Run log: one write, not one per table

Per the decision to write `_meta.pipeline_run_log` once at the end rather
than per table: each table's result is kept in memory (a plain dict,
returned through its future) and the whole batch is written in one
`DeltaRunLogWriter.write()` call, via
`replaceWhere _execution_id = <this run>` so a crash-and-rerun of the same
execution id overwrites cleanly instead of duplicating. The real trade-off:
a hard process kill mid-run leaves *no* row in the log for that execution
id, where a per-table write would at least show what got done before the
crash. Taken deliberately, for write throughput - worth a sentence in the
post so it doesn't read as an oversight.

One implementation snag from this: `spark.createDataFrame(rows)` can't
infer a schema for a column that's `None` in every row of the batch, and
that's the common case for a run log (`error_type` is `None` on every
successful run). `DeltaRunLogWriter` builds its DataFrame against an
explicit `StructType` for exactly this reason - schema inference from
row dicts is a fine default until the data you're logging is mostly nulls.

## Small things that came up

- `TableStatus` is a `StrEnum` (Python 3.11+) rather than the more common
  `class Foo(str, Enum)` - functionally identical here, but `ruff` flagged
  the pattern and `StrEnum` is the more direct spelling now that the
  package's floor is 3.11.
- Building a nested pydantic model (`Table(merge={...})`) via the plain
  constructor fights `pyright` - the generated `__init__` wants a `Merge`
  instance, not a dict, even though pydantic happily coerces it at
  runtime. `Table.model_validate({...})` (which is typed to accept `Any`)
  is the right call in tests that want to build a `Table` from a raw dict,
  and matches how the real loader builds one from parsed YAML anyway.
- Delta's own merge-builder type stubs declare `set`/`values` as `Dict[str,
  ExpressionOrColumn]`, and Python's `dict` is invariant in its value type
  - a `dict[str, Column]` (all Columns, no strings) isn't assignable to
  that even though every `Column` *is* an `ExpressionOrColumn`. No real bug
  here, just `pyright` noise; fixed with a couple of explicit
  `dict[str, str | Column]` annotations rather than fighting it.

## Phase C: the platform seam

**A real bug this phase caught: `run_pipeline()` was always forcing
`local[*]`.** With no adapter given, it built a brand-new SparkSession via
`build_local_session()` unconditionally - fine on a laptop, but wrong on
an actual Databricks or Fabric cluster, which already has a distributed
session running. The fix is one line
(`SparkSession.getActiveSession() or build_local_session()`), but it only
surfaced by actually writing the Databricks/Fabric entrypoints and asking
"what SparkSession does this use." Worth calling out in the post as an
example of why the platform work matters even before a real workspace is
involved - designing the entrypoint honestly finds bugs the local demo
never could.

**Secret resolution is pluggable on `DatabricksPlatform`, not hardcoded to
`dbutils.secrets`.** The first draft called `dbutils.secrets.get(scope,
key)` unconditionally. Corrected: that's fine as a default *as long as the
scope is Unity-Catalog-backed* (`--scope-backend-type UC`), but a legacy
Azure-Key-Vault-backed secret scope is exactly what Databricks itself now
treats as legacy and less secure. So `DatabricksPlatform` takes an
optional `secret_resolver` callable instead of assuming dbutils is the
only path - the intended real alternative is an Access Connector for
Azure Databricks (a UC-governed managed identity) reading Key Vault
directly, which a caller wires up as its own `secret_resolver` rather than
this package guessing the exact SDK calls.

**Unity Catalog naming: session-default catalog, not fqn rewriting.**
`DatabricksPlatform.qualify_table_name()` exists and is tested
(`catalog.schema.table`), but nothing forces it onto every table
reference. The reason: `depends_on: auto` and every logic file's
`spark.table(...)`/`FROM` reference the plain two-level `schema.table`
name, and rewriting all of those consistently across a whole pipeline
just to get three-level names would be real surgery for something Unity
Catalog already solves more simply - set the session's default catalog
once (`spark.sql(f"USE CATALOG {catalog}")` or
`spark.catalog.setCurrentCatalog(catalog)`) in the job entrypoint, and
every existing two-level reference resolves against it unchanged.
`qualify_table_name()` stays available for anything that genuinely needs
an explicit three-level reference later, without forcing that shape
everywhere.

**The DAB and Fabric notebook are unverified - deliberately, and said so
in both READMEs.** Neither `examples/databricks/` nor `examples/fabric/`
has run against a real workspace yet; both were built against documented
APIs (`dbutils.secrets`, `notebookutils.credentials.getSecret`) and
reasonable conventions, with the specific things most likely to be wrong
called out explicitly in each README (`dbutils` availability in a
`spark_python_task`, the exact Files-mount path convention, the runtime
version pinned in `databricks.yml`). This is the handoff point: the
verification round is David deploying both and reporting back what
breaks, not something to fake confidence about here.
