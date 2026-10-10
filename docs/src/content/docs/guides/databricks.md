---
title: Databricks
description: Run a pipeline as a Databricks job with a Databricks Asset Bundle.
sidebar:
  order: 1
---

:::caution
Not verified against a real workspace yet. The platform code is unit-tested against a mocked `dbutils`. Treat the example as a starting point.
:::

```bash
cd examples/databricks
databricks bundle deploy -t dev
databricks bundle run pipetree_example -t dev
```

`databricks.yml` defines a job with a `spark_python_task` that runs `run_on_databricks.py`. The script builds a `DatabricksPlatform` and a `SparkAdapter` on the active cluster session and calls `run_pipeline`:

```python
platform = DatabricksPlatform(dbutils=dbutils, catalog=CATALOG, secret_scope=SECRET_SCOPE)
adapter = SparkAdapter(
    spark, systems=config.systems, base_dir=CONFIG_PATH.parent, platform=platform
)
digest = run_pipeline(CONFIG_PATH, adapter=adapter)
```

It takes three parameters: catalog, secret scope, config path. The bundle uses the `databricks` CLI's default auth profile.

## Check first

- **`dbutils`** is assumed to be injected for a `spark_python_task`. If not, use `DBUtils(spark)` from `pyspark.dbutils`.
- **Secret scope**: `{ secret: name }` reads from a Unity Catalog-backed scope (default `pipetree`), not a legacy Key Vault-backed one: `databricks secrets create-scope --scope pipetree --scope-backend-type UC`. To read Key Vault directly, pass `secret_resolver` to `DatabricksPlatform`.
- **Catalog**: table names stay `schema.table`. Set the default catalog before the run (`USE CATALOG`).
- **Runtime and node type** in `databricks.yml` are examples: pin them to your workspace.
- Token-based auth (`auth.mode: aad_token`) uses a Unity Catalog service credential per resource, Databricks Runtime 16.2+.

See also [AUTO CDC](../autocdc/) for running scd1/scd2 tables as a Lakeflow pipeline instead.
