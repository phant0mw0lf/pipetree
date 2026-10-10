---
title: YAML structure
description: Systems, layers and tables, defaults and secrets.
sidebar:
  order: 1
---

```yaml validate
defaults:
  schema_policy: evolve

systems:
  hr_sql:
    type: sqlserver
    host: { secret: hr-sql-host }
    database: hr
    auth:
      mode: password
      user: { secret: hr-sql-user }
      password: { secret: hr-sql-password }

bronze:
  tables:
    employee:
      source: { system: hr_sql, object: dbo.Employee }
      business_key: [employee_id]
      strategy: scd1

silver:
  tables:
    employee_clean:
      logic: notebooks/silver/employee_clean.sql
      business_key: [employee_id]
      strategy: replace
      depends_on: auto
```

Three kinds of top-level keys:

- `defaults`: values for every table. Today `schema_policy` (`evolve`, `fail`, `ignore`).
- `systems`: where data comes from. `type` picks the reader, the other keys go to it. See the [YAML reference](../../reference/yaml/).
- Every other key is a **layer** with a `tables` map. The layer is a namespace and the default schema of its tables (`bronze.customer`). It is not an execution barrier: order comes from the [dependency tree](../dependency-tree/).

A table has either a `source` (a system and an object) or a `logic` file (`.sql` or `.py`, path relative to the config), never both. `strategy` is required: [`replace`](../../merge/replace/), [`append`](../../merge/append/), [`scd1`](../../merge/scd1/) or [`scd2`](../../merge/scd2/). A bare table name works in `depends_on` and `--select` as long as only one table has it.

## Secrets

Any system value can be `{ secret: name }`. The platform resolves it at run time, so the YAML is the same everywhere:

| Platform | Resolved from |
| --- | --- |
| Local | environment variable: the name upper-cased, `-` and space replaced by `_` (`hr-sql-host` is `HR_SQL_HOST`) |
| Databricks | a Unity Catalog-backed secret scope, or a resolver you pass in |
| Fabric | `notebookutils.credentials` (Key Vault) |

## Validate

`pipetree validate --config pipeline.yaml` checks the structure and rules without running anything. Errors name the key path, for example `silver.tables.orders.merge.delete_mode: ...`.
