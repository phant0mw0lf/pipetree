---
title: Databricks
description: Run a pipeline as a Databricks job with a Databricks Asset Bundle.
sidebar:
  order: 1
slug: 0.1/guides/databricks
---

```bash
cd examples/databricks
databricks bundle deploy -t dev
databricks bundle run pipetree_example -t dev
```

Install `pipetree-meta` on the cluster, with `[azure]` for Key Vault secrets or the failure email, and without `[spark]`: the runtime has its own PySpark and Delta.

`databricks.yml` defines a job with a `notebook_task` that runs `run_on_databricks.py`, a source-format notebook. It builds a `DatabricksPlatform` and a `SparkAdapter` on the cluster session and calls `run_pipeline` with a live dependency tree under the run cell:

```python
platform = DatabricksPlatform(
    dbutils=dbutils,
    catalog=catalog,
    key_vault_url=key_vault_url,
    key_vault_credential=key_vault_credential,  # a Unity Catalog service credential
)
adapter = SparkAdapter(
    spark, systems=config.systems, base_dir=config_path.parent, platform=platform
)
digest = run_pipeline(config_path, adapter=adapter, observer=LiveGraphView(), tags=tags or None)
```

The job parameters are `catalog`, `key_vault_url`, `key_vault_credential`, `tags` and the optional `acs_credential`. The vault parameters are empty for a pipeline without secrets. `tags` (comma separated) limits the run to the tables with one of those tags. `acs_credential` names the service credential that sends the [failure email](../notifications/). A failed table fails the task. The bundle uses the `databricks` CLI's default auth profile. See the folder's README for the input files and the cluster settings.

## Secrets

`{ secret: name }` is read from Azure Key Vault with the identity of a Unity Catalog service credential. Provision once:

1. Create an Access Connector for Azure Databricks (a managed identity).
2. Give its identity `Key Vault Secrets User` on the vault.
3. Create a Unity Catalog service credential from the connector.
4. Grant `ACCESS` on the credential to the principal that runs the job.

Nothing is stored in Databricks, and no shared Entra application needs access to the vault. Secret names are Key Vault names: letters, digits and hyphens. Needs `pipetree-meta[azure]` on the cluster and Databricks Runtime 16.2+ (or 15.4 LTS). Service credentials are Python only, driver-side, and not available on SQL warehouses. To use another store, pass `secret_resolver` to `DatabricksPlatform`.

## Notes

* `dbutils` and `spark` are injected into a notebook. In a Python file task, use `DBUtils(spark)` from `pyspark.dbutils`.
* Table names stay `schema.table`. Set the default catalog before the run (`USE CATALOG`).
* Runtime and node type in `databricks.yml` are examples: use values available in your workspace.
* Token-based auth (`auth.mode: aad_token`) uses a Unity Catalog service credential per resource, configured as `service_credentials`.

See also [AUTO CDC](../autocdc/) for running scd1/scd2 tables as a Lakeflow pipeline instead.
