---
title: Running
description: Select what runs, retries, failures and exit codes.
sidebar:
  order: 3
---

```bash
pipetree run --config pipeline.yaml                                   # everything
pipetree run --config pipeline.yaml --select bronze.orders,silver.orders
pipetree run --config pipeline.yaml --select bronze.orders --with-dependents
pipetree run --config pipeline.yaml --select gold.fact_sales --with-ancestors
pipetree run --config pipeline.yaml --init                            # full reload
pipetree run --config pipeline.yaml --max-workers 8
```

| Option | Effect |
| --- | --- |
| `--select a,b` | Run only these tables (fqn or unambiguous bare name). The rest is `skipped`. |
| `--with-dependents` | Add every table downstream of the selection. Use it when a table changed: its children are rebuilt too. |
| `--with-ancestors` | Add every table the selection reads, transitively. Rebuilds one output with all its inputs. |
| `--init` | Full reload: every selected table is seeded from scratch instead of merged. See [Merge options](../../merge/merge-options/). |
| `--max-workers N` | Tables running at the same time. Default 4. |
| `--live-html FILE` | Keep a picture of the run in a file that refreshes itself. |

With both `--with-*` flags the dependents are added first, then the ancestors of everything that runs, so nothing runs on a parent that was left out. `--select` without `--with-dependents` logs a warning: the run can leave `_execution_id` out of step across the tree.

## Failures

- A transient error (throttling, timeout, connection reset) is retried up to 3 attempts in total, with exponential backoff and jitter. Other errors are not retried. A retried `append` first deletes the rows of this run.
- A table that fails ends as `failed`. Every table downstream of it ends as `upstream_failed` and never starts.
- Independent branches keep running.

Table statuses: `succeeded`, `retried→succeeded`, `failed`, `upstream_failed`, `skipped`. A table outside `--select` is `skipped` and does not count against the run.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | No table is `failed` or `upstream_failed` |
| 1 | At least one is, or the config or graph is invalid (the error is printed) |
| 2 | Wrong command line usage |

From Python, `run_pipeline(...)` returns the digest; `digest.exit_code` is the same 0 or 1.
