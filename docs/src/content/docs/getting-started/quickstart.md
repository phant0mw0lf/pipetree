---
title: Quickstart
description: Run the example pipeline on local Spark.
sidebar:
  order: 2
---

From a clone of the [repository](https://github.com/phant0mw0lf/pipetree):

```bash
uv sync --extra spark --dev
uv run pipetree validate --config examples/pipeline.yaml
uv run pipetree graph --config examples/pipeline.yaml
uv run pipetree run --config examples/pipeline.yaml
```

`graph` prints the tree:

```text
bronze.customer
bronze.employee
bronze.orders
silver.customer_enriched  (depends on: bronze.customer)
gold.dim_customer  (depends on: silver.customer_enriched)
silver.orders  (depends on: bronze.orders)
gold.fact_sales  (depends on: gold.dim_customer, silver.orders)
```

`run` reads three CSV files into bronze, builds silver and gold from SQL and PySpark files, writes Delta tables under `spark-warehouse/` and prints a digest, one line per table:

```text
Run 261010132021461: SUCCEEDED (exit code 0)
  succeeded          bronze.customer                attempts=1 5678ms
  succeeded          gold.fact_sales                attempts=1 1043ms
  ...
```

`graph --format html` writes the picture of the tree. `run --live-html run.html` keeps it up to date while the run goes.

## Editor completion

Add the JSON Schema to the top of a config:

```yaml
# yaml-language-server: $schema=https://pipetree.dev/pipetree.schema.json
```

Editors with the YAML language server (VS Code, others) then complete and check keys. See the [YAML reference](../../reference/yaml/).

## More

- [`examples/quickstart.ipynb`](https://github.com/phant0mw0lf/pipetree/blob/main/examples/quickstart.ipynb): the same run as a notebook, with outputs.
- [With problems](../with-problems/): what pipetree reports when the data misbehaves.
