# pipetree on Databricks

A Databricks Asset Bundle that deploys `run_on_databricks.py` as a
notebook task (a source-format notebook), running the same example pipeline as
`examples/run_demo.py`, against a Databricks cluster and Unity Catalog
instead of a laptop.

## Deploy and run

The example reads three CSV files. `DatabricksPlatform` resolves their paths
in the Volume `/Volumes/<catalog>/pipetree/files/`, so copy them there first
(the schema `pipetree` and the Volume `files` must exist in the catalog):

```bash
for f in customer orders employee; do
  databricks fs cp examples/data/$f.csv dbfs:/Volumes/<catalog>/pipetree/files/data/$f.csv
done
```

```bash
cd examples/databricks
databricks bundle deploy -t dev --var catalog=<catalog>
databricks bundle run pipetree_example -t dev --var catalog=<catalog>
```

No `workspace.host` is set in `databricks.yml` - it relies on your
`databricks` CLI's default auth profile. Run `databricks auth login` first
if you haven't authenticated this machine yet.

The job takes these parameters (bundle variables): `catalog` (default
`main`), `key_vault_url` and `key_vault_credential` (both empty by default),
`tags` (comma separated, empty runs every table) and `spark_version` and `node_type_id` of the single-node job cluster. The
defaults are examples: pick a runtime and a VM size your subscription has
quota for. The tables are written to `catalog`; use a catalog without tables
named `bronze.customer`, `gold.dim_customer` and so on, because the pipeline
merges into them.

To run only the tables tagged `reporting`:

```bash
databricks bundle run pipetree_example -t dev --var catalog=<catalog> --var tags=reporting
```

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

- **`dbutils`** is a notebook global here. `DBUtils(spark)` from `pyspark.dbutils` is the alternative.
- **Runtime and node type** are the bundle variables `spark_version` and
  `node_type_id`. The defaults (`15.4.x-scala2.12`, `Standard_DS3_v2`, an Azure
  VM SKU) are examples: use values available in your workspace and cloud.
- **Single-node cluster.** `num_workers: 0` alone is not a single-node
  cluster; the bundle also sets `spark.databricks.cluster.profile: singleNode`,
  `spark.master: local[*]` and the tag `ResourceClass: SingleNode`.
- **Unity Catalog naming.** `run_on_databricks.py` runs `USE CATALOG` with the
  `catalog` parameter, so the pipeline's plain `schema.table` names resolve
  against that catalog.
- **Live graph.** The notebook shows the dependency tree under the run cell
  and redraws it as tables finish (`pipetree.notebook.LiveGraphView`); the
  final picture stays in the run's output. A failed table makes the notebook
  raise, so the task and the job run fail.
