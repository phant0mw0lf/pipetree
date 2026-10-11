---
title: Notes and run log
description: What a note is, the graph badge, the digest and the run log columns.
sidebar:
  order: 4
---

```text
Run 261010132021461: FAILED (exit code 1)
  succeeded          bronze.customer     attempts=1 3120ms  notes: 2 duplicate row(s) dropped
  succeeded          bronze.employee     attempts=1 2987ms  notes: 1 NULL-key row(s) dropped
  succeeded          bronze.orders       attempts=1 3011ms  notes: schema: added: channel (string)
  failed             gold.dim_customer   attempts=1 412ms  AnalysisException: ...
  upstream_failed    gold.fact_sales     attempts=0 0ms
```

A **note** records something a merge changed without failing. The table still succeeds. There are three kinds:

| Note | When |
| --- | --- |
| `n duplicate row(s) dropped` | A batch had several rows for one key. Only the one with the highest `sequence_by` stays. With no `sequence_by` the winner is arbitrary, and the log says so. |
| `n NULL-key row(s) dropped` | scd1 and scd2 drop rows with a NULL business key. See [Merge options](../../merge/merge-options/#null-business-keys). |
| `schema: ...` | The source schema differs from the table: added, removed or retyped columns, or changed nullability. What happens depends on `schema_policy`. |

Each note is also a `WARNING` log line. In the graph the table gets an amber badge, its hover text lists the notes and the end-of-run digest gets a `notes` column. See the [example](../../getting-started/with-problems/).

## Schema policy

| `schema_policy` | On drift |
| --- | --- |
| `evolve` (default) | Safe numeric widening is applied, new columns are added, removed columns are kept. |
| `fail` | The table fails before anything is written. |
| `ignore` | Only the columns already in the table are written. |

It applies to scd1 and scd2. `replace` takes whatever the source says. `append` does not enforce a policy. Every non-empty diff is recorded as a note.

## Run log

One row per table and run, in `_meta.pipeline_run_log` when the `DeltaRunLogWriter` is used. The `pipetree run` command and `run_pipeline` keep the rows in memory unless you pass `run_log_writer`.

| Column | Meaning |
| --- | --- |
| `_execution_id` | Run id, a UTC timestamp number. Re-writing the same id replaces its rows. |
| `table_fqn`, `layer`, `strategy` | Which table |
| `status` | `succeeded`, `retried→succeeded`, `failed`, `upstream_failed`, `skipped` |
| `attempts` | Tries, 0 for `upstream_failed` |
| `started_at`, `ended_at`, `duration_ms` | Timing |
| `rows_written` | Rows written, where the strategy reports it |
| `duplicates_dropped` | Rows dropped by the batch dedupe |
| `schema_changes` | List of schema differences |
| `error_type`, `error_message` | For `failed` |
| `null_keys_dropped` | Rows dropped for a NULL key |

A run log written before `null_keys_dropped` existed gains the column on the next write.
