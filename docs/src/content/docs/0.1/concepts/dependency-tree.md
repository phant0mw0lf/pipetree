---
title: Dependency tree
description: How depends_on is derived, waves, run order and cycles.
sidebar:
  order: 2
slug: 0.1/concepts/dependency-tree
---

```yaml validate
silver:
  tables:
    orders:
      logic: notebooks/silver/orders.sql
      business_key: [order_id]
      strategy: replace
      depends_on: auto        # read the tables from the logic file

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

`.sql` files are parsed as Spark SQL, `.py` files as Python.

| Source | Counts as a dependency |
| --- | --- |
| SQL | every table in `FROM`, any `JOIN`, subquery, set operation, `LATERAL`, `TABLE(name)`; the target and sources of `CREATE TABLE ... AS`, `INSERT`, `MERGE` and `LIKE` / `CLONE` |
| PySpark | `spark.table`, `spark.read.table`, `spark.readStream.table`, `DeltaTable.forName`, and the SQL in `spark.sql` |

Names can be a string literal, literals joined by `+` or implicitly, or an f-string over module-level string constants (`LAYER = "silver"`).

Not a dependency:

* CTE names, views created in the same file (`CREATE TEMP VIEW`, `createOrReplaceTempView`), table functions (`range`, `explode`), `VALUES`, path tables (``delta.`/mnt/x` ``, `spark.read.parquet(...)`).
* A table that reads or writes itself: the edge is dropped, not a cycle.
* Names that match no table are external sources and are ignored.

Errors, naming the file and the expression:

* A name only known at run time: `spark.table(f"{layer}.x")` with a parameter, `.format(...)`, a function result. Declare `depends_on` explicitly.
* SQL that does not parse, or T-SQL such as `CROSS APPLY`; a Python syntax error.
* A bare name that several tables share (`orders` in `bronze` and `silver`). Write `schema.table`.

How a name matches a table: `catalog.schema.table` and `schema.table` match the table `schema.table` (the catalog is added when the platform runs). A bare `table` matches the one table with that name in any layer. Case is ignored.

Reads through another variable (`reader = spark.read; reader.table("x")`) are not seen; list those in `depends_on`. `depends_on: auto` needs a `logic` file, and other file types fail with an error.

An explicit list is strict: a name that matches no table (or several) is an error.

## Waves and run order

A table's **wave** is the length of its longest chain of parents. Wave 0 are the tables without parents. Tables in one wave never depend on each other, so they can run in parallel, up to `--max-workers`.

The executor does not run wave by wave. A table starts the moment its own parents have succeeded. Waves are how the graph is drawn.

## Cycles

A cycle fails before anything runs, with the path: `dependency cycle: a.x → a.y → a.x`.

See the graph with `pipetree graph --config pipeline.yaml` (`--format text`, `mermaid` or `html`).
