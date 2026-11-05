---
title: Running
description: Select what runs, retries, failures and exit codes.
sidebar:
  order: 3
slug: 0.1/concepts/running
---

```bash
pipetree run --config pipeline.yaml                                   # everything
pipetree run --config pipeline.yaml --select bronze.orders,silver.orders
pipetree run --config pipeline.yaml --select bronze.orders --with-dependents
pipetree run --config pipeline.yaml --select gold.fact_sales --with-ancestors
pipetree run --config pipeline.yaml --tag daily                       # tables tagged daily
pipetree run --config pipeline.yaml --init                            # full reload
pipetree run --config pipeline.yaml --max-workers 8
```

| Option | Effect |
| --- | --- |
| `--select a,b` | Run only these tables (fqn or unambiguous bare name). The rest is `skipped`. |
| `--with-dependents` | Add every table downstream of the selection. Use it when a table changed: its children are rebuilt too. |
| `--with-ancestors` | Add every table the selection reads, transitively. Rebuilds one output with all its inputs. |
| `--tag NAME` | Run only tables carrying this tag. Repeatable: a table needs at least one. See [Tags](../tags/). |
| `--exclude-tag NAME` | Never run tables carrying this tag, even if a `--with-*` flag pulls them in. Repeatable. |
| `--init` | Full reload: every selected table is seeded from scratch instead of merged. See [Merge options](../../merge/merge-options/). |
| `--max-workers N` | Tables running at the same time. Default 4. |
| `--live-html FILE` | Keep a picture of the run in a file that refreshes itself. |

With both `--with-*` flags the dependents are added first, then the ancestors of everything that runs, so nothing runs on a parent that was left out. `--select` or `--tag` without `--with-dependents` logs a warning: the run can leave `_execution_id` out of step across the tree. A table that runs while a table it reads does not gets the note `input not refreshed: <table>`; see [Tags](../tags/#inputs-that-are-not-refreshed).

## Failures

* A transient error (throttling, timeout, connection reset) is retried up to 3 attempts in total, with exponential backoff and jitter. Other errors are not retried. A retried `append` first deletes the rows of this run; if that delete fails, the table fails without a retry and the run goes on.
* A table that fails ends as `failed`. Every table downstream of it ends as `upstream_failed` and never starts.
* Independent branches keep running.

### Which errors are transient

The executor retries three exception types: `Throttled` (`pipetree.executor.retry`), `TimeoutError` and `ConnectionError`. Spark raises none of them, so the `SparkAdapter` translates what it recognises, by the Java class name in the error, also when the class is only the cause underneath another exception:

| Error from the engine | Retried as |
| --- | --- |
| Delta commit conflicts: `ConcurrentAppendException`, `ConcurrentDeleteReadException`, `ConcurrentDeleteDeleteException`, `ProtocolChangedException`, `MetadataChangedException` | `Throttled` |
| ADLS or Blob storage answering 429, 503, `ServerBusy` or `TooManyRequests` (`AbfsRestOperationException`, `BlobStorageException`, `DataLakeStorageException`). Storage has no class for throttling, so these are recognised by the status in the text, and only for these classes. | `Throttled` |
| `java.net.SocketTimeoutException`, `org.apache.hadoop.net.ConnectTimeoutException`, `java.util.concurrent.TimeoutException` | `TimeoutError` |
| `java.net.SocketException` (a connection reset), `java.net.ConnectException` | `ConnectionError` |

Everything else, such as a missing column, a failed merge condition or a `ValueError` from your own code, is raised unchanged and fails the table on the first attempt. The original error stays as the `__cause__` of the translated one.

An adapter you write yourself has to do the same: raise `Throttled`, `TimeoutError` or `ConnectionError` for errors worth retrying, or pass `RetryPolicy(is_transient=...)` to `run_pipeline(retry_policy=...)`. The predicate is only asked about errors that are not already one of those three types, and it sees the error as the adapter raised it. The CLI has no option for it.

Table statuses: `succeeded`, `retried→succeeded`, `failed`, `upstream_failed`, `skipped`. A table outside `--select` is `skipped` and does not count against the run.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | No table is `failed` or `upstream_failed` |
| 1 | At least one is, or the config or graph is invalid (the error is printed) |
| 2 | Wrong command line usage |

From Python, `run_pipeline(...)` returns the digest; `digest.exit_code` is the same 0 or 1.
