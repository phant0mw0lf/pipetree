---
title: Tags
description: Label tables in the YAML and run, graph or exclude them by tag.
sidebar:
  order: 4
slug: 0.1/concepts/tags
---

A table can carry tags. A run can then pick tables by tag, for example everything that is refreshed daily.

```yaml validate
systems:
  crm:
    type: csv
    path: data/customer.csv

bronze:
  tables:
    customer:
      source:
        system: crm
        object: account
      business_key: [accountid]
      strategy: scd1
      tags: [daily, crm]
```

A tag is lowercase letters, digits, `-` and `_`, starts with a letter or digit and has at most 40 characters. A table cannot list the same tag twice. Tags are not inherited: set them on each table.

## Running by tag

```bash
pipetree run --config pipeline.yaml --tag daily
pipetree run --config pipeline.yaml --tag daily --tag crm        # daily OR crm
pipetree run --config pipeline.yaml --tag daily --exclude-tag crm
pipetree run --config pipeline.yaml --tag reporting --with-ancestors
pipetree validate --config pipeline.yaml --list-tags               # each tag and its table count
pipetree graph --config pipeline.yaml --tag reporting --format html  # what would run
```

Values may also be comma separated: `--tag "daily, crm"`. From Python, pass lists: `run_pipeline(path, tags=["daily"], exclude_tags=["crm"])`.

The options are applied in this order:

| Step | Option | Effect |
| --- | --- | --- |
| 1 | `--select` | Starts with the named tables. Without it, every table. |
| 2 | `--tag` | Keeps the tables with at least one of the tags. Several `--tag` options are an OR. |
| 3 | `--with-dependents`, `--with-ancestors` | Extend the result, exactly as for `--select`. |
| 4 | `--exclude-tag` | Removes every table carrying the tag. It wins over step 3. |

A tag that no table carries is an error that lists the known tags, and so is a selection that ends up empty. A typo never turns into a successful run of nothing.

## Inputs that are not refreshed

When a table runs but a table it reads does not, the table gets a note for each such input. The run still succeeds and the exit code is unchanged. This applies to every partial run, `--select` as well as tags.

```text
Run 261011112006356: SUCCEEDED (exit code 0)
  skipped            bronze.customer                attempts=0 0ms
  skipped            bronze.employee                attempts=0 0ms
  skipped            bronze.orders                  attempts=0 0ms
  succeeded          gold.dim_customer              attempts=1 1832ms  notes: input not refreshed: silver.customer_enriched
  succeeded          gold.fact_sales                attempts=1 769ms  notes: input not refreshed: silver.orders
  skipped            silver.customer_enriched       attempts=0 0ms
  skipped            silver.orders                  attempts=0 0ms
```

That is `pipetree run --tag reporting` on the example pipeline. The same facts show up as a `WARNING` log line per table at the start of the run, as an amber badge with a tooltip in the [graph](../notes-and-run-log/), and in the `notes` column of the digest. In the graph a table outside the run is `skipped` with the hover text `not selected`, and the hover text of every table lists its tags.

The tags requested for a run are written to the `run_tags` column of the [run log](../notes-and-run-log/#run-log).
