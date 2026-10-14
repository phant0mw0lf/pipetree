---
title: Architecture
description: The parts of the package and what each one does.
sidebar:
  order: 2
---

The design rationale is in the [Pipeline Engineering](https://datadave.dev/tags/pipeline-engineering/) blog series. In short:

- **Config loader** (`pipetree.config`) — parses and validates the YAML,
  failing fast with an error that names the offending key
  (`silver.tables.orders.merge.delete_mode: ...`).
- **Typed model** (`pipetree.model`) — defaults resolved, every table given a
  fully qualified name from `table_schema` (defaulting to its layer) + its
  block key.
- **Graph builder** (`pipetree.graph`) — derives the dependency tree (`auto`
  parses the logic file: Spark SQL with sqlglot, PySpark with `ast`, and
  fails with a clear error for a table name only known at run time;
  `depends_on` lists are matched by fqn or, if unambiguous, by bare
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
  Fabric and Databricks run on - with `scd1` and `scd2` implemented as a Delta
  merge, and `replace` and `append` as plain overwrite and append writes. With no active SparkSession it
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
- **NULL business keys** - on an `scd1`/`scd2` table *without*
  `unknown_member`, a source row whose key is `NULL` (a single-column key)
  or `NULL` in every key column (a composite key) cannot identify a row:
  it is dropped with a warning and counted as `null_keys_dropped`. A
  composite key with a `NULL` in just *one* column is an ordinary value:
  keys are matched null-safely, so the same key finds the same row in every
  batch. `replace` and `append` tables without `unknown_member` keep every
  row as it comes (a `NULL` key is data there).
- **Snapshot merge** - `merge.snapshot: true` on an `scd1`/`scd2` table treats
  the batch as the complete current state of the source: an active key that
  is missing from it is deleted following `delete_mode`, guarded by
  `snapshot_max_missing_ratio` (default 0.5) and `snapshot_allow_empty`. See
  [Snapshot merge](../../merge/snapshot/).
- **Surrogate keys** - `surrogate_key: <column>` on an `scd1`/`scd2`
  dimension adds a Kimball surrogate key: a Delta identity column
  (`BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1)`),
  first in the table, created with it. See [Surrogate key](../../merge/merge-options/#surrogate-key).
- **Platforms** (`pipetree.platform`) answer "where am I running?" for
  secrets, table naming, storage paths and run metadata - `LocalPlatform`
  (env-var secrets), `DatabricksPlatform` (Key Vault read directly with a
  Unity Catalog service credential, or a pluggable `secret_resolver`; Unity Catalog naming is handled by
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
  pluggable `Notifier` reports the digest (`ConsoleNotifier` by default; a
  `notifications.email` block in the YAML also mails a failed run through
  Azure Communication Services, see the [Notifications guide](../../guides/notifications/)).
- **Progress** (`pipetree.executor.events`, `pipetree.notebook`) - an
  optional `RunObserver` sees the run while it happens (per-table
  running/retrying/finished events); `LiveGraphView` and
  `HtmlFileObserver` turn that into the live picture. See
  [Seeing the run](../../concepts/seeing-the-run/).
- **Declarative pipelines** (`pipetree.declarative.autocdc`) — a pure
  translation of an `scd1`/`scd2` table into Databricks' AUTO CDC flow
  syntax (`KEYS`, `SEQUENCE BY`, `APPLY AS DELETE WHEN`,
  `STORED AS SCD TYPE`, `TRACK HISTORY ON * EXCEPT`) - the comparison part
  1 draws, made mechanical. Not an executor: AUTO CDC only runs inside a
  Lakeflow Declarative Pipeline, where the platform owns the DAG; this
  compiles to it, and doesn't attempt to run anything itself. The flow reads
  `FROM STREAM <source>`, as Databricks requires a streaming source.
  Semantics differ from pipetree's merge: scd2 history uses
  `__START_AT`/`__END_AT` (SEQUENCE BY values, no audit columns), duplicates
  of a key within one micro-batch each become history versions. A row with
  an older sequence is handled by sequence in the same micro-batch and in a
  later one (scd1 keeps the higher sequence, scd2 inserts an earlier
  version); pipetree follows the same rule when `sequence_by` is configured.
  `delete_mode` is only modeled for `ignore`.
