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

**Databricks secrets come from Key Vault through a service credential, not a
secret scope.** A Key Vault-backed scope needs the shared `AzureDatabricks`
Entra application to have access to the vault, and a Databricks-stored scope
keeps the values in Databricks. `DatabricksPlatform` instead reads Key Vault
directly with the identity of a Unity Catalog service credential (backed by an
Access Connector holding `Key Vault Secrets User`). `secret_resolver` stays as
the escape hatch.

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

## Phase D: sources

**`Trigger.AvailableNow` needs a sink to actually run, which took some
working out.** `Trigger.AvailableNow` is a write-side concept - it's how a
`writeStream` decides when to stop, not something a `readStream` has on
its own - so getting a plain batch DataFrame back out of "read what's new
since last time" means actually running a streaming query end to end, not
just building a lazy read. `StorageStreamSource` streams new files into a
staging Delta location with `Trigger.AvailableNow`, waits for it to
finish, then hands back a batch read of that staging location - the rest
of pipetree never sees a streaming DataFrame. Two things about that
staging location weren't obvious until the tests actually ran:

- The staging table has to be cleared *every run*, not just on `init`.
  Otherwise it accumulates every batch ever written, and a table that's
  supposed to see "just what's new" ends up re-merging its entire history
  each time. Clearing staging doesn't touch the checkpoint - the
  checkpoint is what actually remembers cross-run progress; staging is
  just this run's inbox.
- A run with nothing new to process never initializes the sink at all -
  no exception, just no Delta table at the staging path. Reading it back
  with a plain `spark.read.format("delta").load(...)` throws
  `PATH_NOT_FOUND` if you don't check for this first. The fix is
  `DeltaTable.isDeltaTable(...)` before reading, falling back to an empty
  DataFrame with the stream's own schema.

This is genuinely tested end to end locally (real files, real Spark, real
`Trigger.AvailableNow`) for the non-Databricks path - Auto Loader
(`cloudFiles`) is written against Databricks' documented options but
untested, same caveat as the rest of Phase C.

**`sqlserver` and `kusto` are tested without a JVM at all.** Both only
ever call `ctx.spark.read.format(...).option(...).load()` through duck
typing, so a hand-written fake `DataFrameReader` is enough to verify the
JDBC url / Kusto connector options they build, with no real database or
cluster to connect to regardless. These are some of the fastest tests in
the suite and don't need the `spark` marker - a nice side effect of
keeping the reader's job narrowly "build the right read call" rather than
"connect and prove it works," which isn't something this environment
could verify anyway.

**`custom` needed nothing built, on purpose.** `pipetree.sources.custom`
is a pure bring-your-own-reader seam - `load_custom_reader()` dynamically
imports whatever class `systems.<name>.class` points at and checks it
satisfies the `SourceReader` protocol. No concrete Graph/SharePoint
reader ships, and none should: that's a real implementation David would
write against a real tenant, using the Python Data Source API path from
part 1, not something to fake here.

**`d365_export` and `synapse_link` are literally the same reader
instance as `storage_stream`**, registered under three names. The plan
called these "thin wrappers, not separate subsystems," and once
`storage_stream` existed there was nothing left to wrap - all three are
"files landing in a storage account, streamed in with `Trigger.AvailableNow`."
If a real deployment needs source-specific behavior (Synapse Link's
delete-flagged rows, say), that's a difference in the *config* for that
table (`delete_when`), not a reason for a separate reader class.

## Phase E: schema drift

**Spark's type names are not Python's, and this would have been an
embarrassing bug to ship.** `LongType().simpleString()` is `"bigint"`,
not `"long"`; `ByteType()` is `"tinyint"`, `ShortType()` is `"smallint"`.
The safe-widening table (int→long, float→double are the two examples the
design settled on) had to be built against Spark's actual `simpleString()`
names, not the ones that read naturally in English - checked with a
two-line script against a real session before writing the table, not
assumed. Worth a line in the post: verify vendor type-name strings
empirically, they're rarely what your own prose implies.

**A newly-added column can't be part of its own "did this change"
check.** The first real integration attempt failed immediately:
`schema_policy: evolve` allowed a new `email` column through, but the
change-detection SQL still compared `target.email <=> source.email` -
and `target.email` doesn't exist yet, because it's the column *being*
added. Delta's error here was exact and immediate
(`DELTA_MERGE_UNRESOLVED_EXPRESSION`), which made the fix obvious once it
happened: exclude a column from the change comparison unless it already
exists on the target side. `SchemaReconciliation` carries the target's
pre-write column set for exactly this - a column with no old value can't
inform whether a row "changed," and the merge writes it regardless of
that comparison anyway (every column in scope is in the `SET`/`INSERT`
values either way).

**`DeltaMergeBuilder.withSchemaEvolution()` is a real, working method on
delta-spark 4.0.1** - checked directly (`dir(DeltaMergeBuilder)`) before
relying on it, then proved end-to-end with a throwaway script: merging a
source with an extra column into an existing table, with
`.withSchemaEvolution()` on the merge builder, actually adds the column
and backfills existing rows with null. The append-only "new scd2 version"
write needs the parallel `.option("mergeSchema", "true")` on its own
`saveAsTable` call - a Delta merge and a Delta append have separate
schema-evolution switches, and forgetting the second one would silently
fail (or silently succeed without writing the new column) only on a
`scd2` table with a genuinely new column, which is exactly the kind of
gap that's invisible until someone hits it in production.

**`fail` and `ignore` reuse the exact same detection path as `evolve` -
they don't special-case "no drift."** `reconcile()` always diffs first;
if the diff is empty, every policy just returns the source unchanged.
This means the fail-fast property is real: a `schema_policy: fail` table
never even builds a merge plan against drifted data, because the
`SchemaError` raises before `_prepare_source` or the `DeltaTable.merge()`
call ever runs.

**Scope decision: `append` and `replace` don't get schema-policy
enforcement.** `replace` already means "whatever the source says, goes" -
its `overwriteSchema: true` on every write already is unconditional
evolution, and enforcing `fail`/`ignore` there would contradict what
`replace` is for. `append` would need the same `mergeSchema` wiring as
scd2's new-version write, but wasn't wired up this pass; an append table
with a genuinely new column will fail with Delta's own schema-mismatch
error today rather than pipetree's `SchemaError`. Worth fixing before
relying on `append` + evolving sources in the same pipeline.

## Phase F: declarative pipelines (AUTO CDC)

**This one is a compiler, not a runtime, and staying honest about that
shaped the whole module.** AUTO CDC (`CREATE FLOW ... AS AUTO CDC INTO`)
only exists inside a running Lakeflow Declarative Pipeline - there's no
`dlt` package to import and call outside of one, so nothing here could
ever be "tested against the real thing" the way the Spark adapter's merge
logic was. `translate_table()` and `render_sql()` are pure functions
(`Table` in, a dataclass and then SQL text out), which turned out to be
exactly the right shape anyway: it makes the translation fully testable
without Databricks, and it's genuinely all pipetree can responsibly do
here - the platform owns the DAG once code runs inside a declarative
pipeline, pipetree just hands it the equivalent syntax.

**SQL over the Python API, on purpose, given what could and couldn't be
verified.** Databricks renamed `dlt.apply_changes` to
`dlt.create_auto_cdc_flow` at some point, which means the "current"
Python parameter names are a moving target I have no way to check from
here. The SQL grammar (`KEYS`, `SEQUENCE BY`, `APPLY AS DELETE WHEN`,
`STORED AS SCD TYPE`, `TRACK HISTORY ON * EXCEPT`) is the one part 1
already commits to in print, so it's the more stable thing to render with
any confidence - and it's directly usable in a pipeline's SQL source
regardless of which Python spelling is current this month.

**The field mapping is deliberately narrower than pipetree's own
`merge_mode`.** `delete_mode` (soft/hard/ignore) doesn't translate:
AUTO CDC's own delete behavior is fixed by `stored_as_scd_type` (closing
a version for SCD2, removing the row for SCD1, as far as the docs
describe it), not a further per-table choice - so only whether a delete
signal exists at all carries over (`ignore` drops it, matching the same
rule `_prepare_source` already uses in the Spark adapter), not which of
pipetree's three modes produced it. Translating a `soft`-delete `scd1`
table and expecting AUTO CDC to reproduce pipetree's exact
keep-the-row-mark-it-deleted semantics would be overclaiming something
I can't verify - said so in the docstring rather than guessing.

**`replace`/`append` are rejected outright, not silently ignored.**
Neither strategy has anything resembling `KEYS` or CDC semantics to
translate - a table that fully replaces or blindly appends each run is
already exactly a plain streaming table or materialized view in a
declarative pipeline, with no flow declaration needed at all. Raising
a clear `ValueError` naming the actual strategy seemed more useful than
returning `None` and leaving the caller to guess why.
