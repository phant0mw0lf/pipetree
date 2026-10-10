---
title: Databricks
description: Run a pipeline as a Databricks job with a Databricks Asset Bundle.
sidebar:
  order: 1
---

```bash
cd examples/databricks
databricks bundle deploy -t dev
databricks bundle run pipetree_example -t dev
```

`databricks.yml` defines a job with a `spark_python_task` that runs `run_on_databricks.py`. The script builds a `DatabricksPlatform` and a `SparkAdapter` on the active cluster session and calls `run_pipeline`:

```python
platform = DatabricksPlatform(
    dbutils=dbutils,
    catalog=CATALOG,
    key_vault_url="https://my-vault.vault.azure.net",
    key_vault_credential="pipetree-kv",  # a Unity Catalog service credential
)
adapter = SparkAdapter(
    spark, systems=config.systems, base_dir=CONFIG_PATH.parent, platform=platform
)
digest = run_pipeline(CONFIG_PATH, adapter=adapter)
```

The job takes three parameters: `catalog`, `key_vault_url` and `key_vault_credential`. The last two are empty for a pipeline without secrets. The bundle uses the `databricks` CLI's default auth profile.

## Secrets

`{ secret: name }` is read from Azure Key Vault with the identity of a Unity Catalog service credential. Provision once:

1. Create an Access Connector for Azure Databricks (a managed identity).
2. Give its identity `Key Vault Secrets User` on the vault.
3. Create a Unity Catalog service credential from the connector.
4. Grant `ACCESS` on the credential to the principal that runs the job.

Nothing is stored in Databricks, and no shared Entra application needs access to the vault. Secret names are Key Vault names: letters, digits and hyphens. Needs `pipetree-meta[azure]` on the cluster and Databricks Runtime 16.2+ (or 15.4 LTS). Service credentials are Python only, driver-side, and not available on SQL warehouses. To use another store, pass `secret_resolver` to `DatabricksPlatform`.

## Notes

- `dbutils` is assumed to be injected for a `spark_python_task`. If not, use `DBUtils(spark)` from `pyspark.dbutils`.
- Table names stay `schema.table`. Set the default catalog before the run (`USE CATALOG`).
- Runtime and node type in `databricks.yml` are examples: use values available in your workspace.
- Token-based auth (`auth.mode: aad_token`) uses a Unity Catalog service credential per resource, configured as `service_credentials`.

See also [AUTO CDC](../autocdc/) for running scd1/scd2 tables as a Lakeflow pipeline instead.
