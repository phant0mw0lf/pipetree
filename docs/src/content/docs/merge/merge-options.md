---
title: Merge options
description: sequence_by, delete_when, delete_mode, ignore_columns, init, NULL keys, unknown_member and surrogate_key.
sidebar:
  order: 5
---

```yaml validate
systems:
  crm:
    type: csv
    path: data/customer.csv

gold:
  tables:
    dim_customer:
      source: { system: crm, object: account }
      business_key: [accountid]
      strategy: scd2
      unknown_member: true
      surrogate_key: customer_sk
      merge:
        sequence_by: [versionnumber]
        delete_when: "isdelete = true"
        delete_mode: soft
        ignore_columns: [modifiedon]
```

`merge` applies to scd1 and scd2. `unknown_member`, `surrogate_key` and `business_key` are table keys.

| Option | Values | Meaning |
| --- | --- | --- |
| `sequence_by` | list of columns | Orders the versions of a key. Highest wins, NULL is lowest. Used within a batch and across batches. |
| `delete_when` | Spark SQL expression | True for a delete row. A NULL result is not a delete. |
| `delete_mode` | `soft` (default), `hard`, `ignore` | `soft`: keep the row, mark `_is_deleted`. `hard`: remove the row (scd1 only). `ignore`: delete rows are dropped from the batch before the dedupe. |
| `ignore_columns` | list of columns | Changes in only these columns update in place: no scd2 version, no `_execution_id` bump. |

The effects per strategy are on the [scd1](../scd1/) and [scd2](../scd2/) pages.

## NULL business keys

| Strategy | Row with a NULL key |
| --- | --- |
| scd1, scd2, single column key | dropped and noted |
| scd1, scd2, composite key, NULL in every column | dropped and noted |
| scd1, scd2, composite key, NULL in some columns | an ordinary value: keys match null-safely, so the same key finds the same row in every batch |
| replace, append | kept: a NULL key is data |
| any strategy with `unknown_member` | dropped if any key column is NULL |

## Full reload

`pipetree run --init` seeds every selected table from scratch instead of merging.

| Strategy | `--init` |
| --- | --- |
| replace | no difference |
| append | starts the table over with this run's rows |
| scd1 | the table is rewritten from the batch |
| scd2 | a fresh history: every key starts at version 1 |

On a table with `surrogate_key` the table is kept, so the identity counter keeps counting. A `storage_stream` source clears its checkpoint and reads everything again.

## Unknown member

`unknown_member: true` seeds one row whose key columns hold a sentinel, so a fact can point at it when a foreign key is missing.

| Key type | Sentinel |
| --- | --- |
| numeric | `-1` |
| string | `NULL` |
| date, timestamp | `1900-01-01` |

Other key types are rejected. A composite key gets one sentinel per column. Seeding is idempotent. Source rows with a NULL in any key column are dropped, since NULL is reserved. A fact reaches a `NULL`-keyed unknown member with a null-safe join (`<=>`). It requires `business_key`.

## Surrogate key

`surrogate_key: product_sk` adds a generated integer column, first in the table.

```sql
SELECT f.sale_id, COALESCE(d.product_sk, -1) AS product_sk
FROM silver.sales f
LEFT JOIN gold.dim_product d
  ON f.product_code = d.product_code AND d._is_current = true
```

- Only for scd1 and scd2 with a `business_key`. The name cannot be a key column or a reserved audit column.
- scd1: a new key gets a new value, an update keeps it. scd2: every new version gets a new value, closing a version never changes one.
- The unknown member gets `-1`. Values are unique and increasing, with gaps.
- The source must not have a column with that name.
- It is a Delta identity column: Delta 3.3+ or Databricks Runtime 10.4+. Fabric Runtime 1.3 has no identity columns, so `surrogate_key` is unsupported there. A table created before `surrogate_key` was set has to be dropped and recreated.
- The [AUTO CDC translation](../../guides/autocdc/) refuses it.
