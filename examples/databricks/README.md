# pipetree on Databricks

A Databricks Asset Bundle that deploys `run_on_databricks.py` as a
`spark_python_task`, running the same example pipeline as
`examples/run_demo.py`, against a real Databricks cluster and Unity
Catalog instead of a laptop.

**Status: not verified against a real workspace yet.** This is Phase C's
starting point - built and unit-tested against mocked `dbutils`
(`tests/platform/test_databricks.py`), but nothing here has actually run
on Databricks. Treat it as a first draft to deploy and fix, not a
finished thing.

## What to check first

- **`dbutils` availability.** `run_on_databricks.py` assumes `dbutils` is
  injected into the global namespace for a `spark_python_task`, the same
  way it is in a notebook. If that's wrong, the fix is probably
  `from pyspark.dbutils import DBUtils; dbutils = DBUtils(spark)` instead.
- **The Databricks Runtime version** in `databricks.yml`
  (`spark_version: 15.4.x-scala2.12`) - pin to whatever's current in your
  workspace.
- **The node type** (`Standard_DS3_v2`) is an Azure VM SKU - swap for an
  AWS/GCP equivalent if you're not on Azure.
- **The secret scope.** `secret_scope` defaults to `pipetree` and must be
  **Unity-Catalog-backed**, not a legacy Azure-Key-Vault-backed scope:
  ```bash
  databricks secrets create-scope --scope pipetree --scope-backend-type UC
  ```
  The example pipeline doesn't actually need a secret (its one source is
  a local CSV), so this only matters once a real system config uses one.
  To read Key Vault directly instead of a secret scope at all - via an
  Access Connector for Azure Databricks - pass `secret_resolver` to
  `DatabricksPlatform` in `run_on_databricks.py` instead of
  `dbutils`/`secret_scope`.
- **Unity Catalog naming.** `DatabricksPlatform.qualify_table_name()`
  exists (`catalog.schema.table`) but isn't force-applied to every write -
  `run_on_databricks.py` doesn't call `spark.catalog.setCurrentCatalog()`
  or `USE CATALOG` either. Do one of those first, so the pipeline's plain
  `schema.table` names resolve against the right catalog by default; see
  `NOTES-for-blog.md` for why this seemed better than rewriting every
  table reference.

## Deploy and run

```bash
cd examples/databricks
databricks bundle deploy -t dev
databricks bundle run pipetree_example -t dev
```

No `workspace.host` is set in `databricks.yml` - it relies on your
`databricks` CLI's default auth profile. Run `databricks auth login`
first if you haven't authenticated this machine yet.

## Comparing against the local run

Same pipeline, same fault-free path (no `FaultInjectingAdapter` here -
that's for the local demo only). The interesting comparison for part 3
isn't whether it works, it's *what's different*: cluster startup time,
Unity Catalog vs. a local Hive metastore, real distributed execution vs.
`local[*]`. Notes on that comparison belong in part 3, once this has
actually run.
