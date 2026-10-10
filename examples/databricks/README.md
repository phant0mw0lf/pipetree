# pipetree on Databricks

A Databricks Asset Bundle that deploys `run_on_databricks.py` as a
`spark_python_task`, running the same example pipeline as
`examples/run_demo.py`, against a Databricks cluster and Unity Catalog
instead of a laptop.

## Deploy and run

```bash
cd examples/databricks
databricks bundle deploy -t dev
databricks bundle run pipetree_example -t dev
```

No `workspace.host` is set in `databricks.yml` - it relies on your
`databricks` CLI's default auth profile. Run `databricks auth login` first
if you haven't authenticated this machine yet.

The job takes these parameters (bundle variables): `catalog` (default
`main`), `key_vault_url` and `key_vault_credential` (both empty by default).

## Secrets from Key Vault

`{secret: name}` values are read from Azure Key Vault with the identity of a
Unity Catalog service credential. Nothing is stored in Databricks. The
example pipeline needs no secret, so skip this until a system config uses
one. Provision once:

1. Create an Access Connector for Azure Databricks (a managed identity).
2. Grant its identity `Key Vault Secrets User` on the vault.
3. Create a Unity Catalog service credential from the connector.
4. Grant `ACCESS` on the credential to the principal that runs the job.

Then set `key_vault_url` (for example `https://my-vault.vault.azure.net`) and
`key_vault_credential` (the credential's name). Secret names must be Key
Vault names: letters, digits and hyphens. Service credentials need
Databricks Runtime 16.2+ (or 15.4 LTS), Python only, on the driver; they are
not available on SQL warehouses. `azure-keyvault-secrets` (the `azure`
extra) must be installed on the cluster.

## Notes

- **`dbutils`** is taken from the globals of the `spark_python_task`, as in
  a notebook. `DBUtils(spark)` from `pyspark.dbutils` is the alternative.
- **Runtime and node type** in `databricks.yml` are examples
  (`15.4.x-scala2.12`, `Standard_DS3_v2`, an Azure VM SKU): use values
  available in your workspace and cloud.
- **Unity Catalog naming.** `DatabricksPlatform.qualify_table_name()` exists
  (`catalog.schema.table`) but isn't applied to every write, and
  `run_on_databricks.py` doesn't call `USE CATALOG`. Set the default catalog
  (`spark.catalog.setCurrentCatalog(...)` or `USE CATALOG`) so the pipeline's
  plain `schema.table` names resolve against the right catalog.
