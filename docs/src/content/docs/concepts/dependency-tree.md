---
title: Dependency tree
description: How depends_on is derived, waves, run order and cycles.
sidebar:
  order: 2
---

```yaml validate
silver:
  tables:
    orders:
      logic: notebooks/silver/orders.sql
      business_key: [order_id]
      strategy: replace
      depends_on: auto        # read FROM / JOIN from the file

gold:
  tables:
    fact_sales:
      logic: notebooks/gold/fact_sales.py
      business_key: [order_id]
      strategy: replace
      depends_on: [silver.orders, dim_customer]   # or list the parents
```

`depends_on` is `auto` or a list. Names match a table by fqn (`silver.orders`) or by bare name if only one table has it.

## What `auto` reads

| File | Found |
| --- | --- |
| `.sql` | the name after every `FROM` and `JOIN`, as `name` or `schema.name` |
| `.py` | `spark.table("name")` and `spark.sql("...")` with a literal string; `FROM`/`JOIN` inside that SQL |

Limits:

- It is a text scan, not a parser. A table name built at run time (f-string, variable) is not seen. List those in `depends_on`.
- Names that match no table are ignored: they are read as external tables. A typo in a name therefore removes an edge instead of failing. `depends_on: auto` needs a `logic` file.
- `catalog.schema.table` is not matched as a whole.
- Other file types fail with an error.
- A table that reads itself is not a cycle; the edge is dropped.

An explicit list is strict: a name that matches no table (or several) is an error.

## Waves and run order

A table's **wave** is the length of its longest chain of parents. Wave 0 are the tables without parents. Tables in one wave never depend on each other, so they can run in parallel, up to `--max-workers`.

The executor does not run wave by wave. A table starts the moment its own parents have succeeded. Waves are how the graph is drawn.

## Cycles

A cycle fails before anything runs, with the path: `dependency cycle: a.x → a.y → a.x`.

See the graph with `pipetree graph --config pipeline.yaml` (`--format text`, `mermaid` or `html`).
