---
title: AUTO CDC
description: Translate scd1 and scd2 tables to Databricks AUTO CDC flows.
sidebar:
  order: 4
---

```python
from pipetree.declarative.autocdc import render_sql, translate_table

flow = translate_table(config.tables["bronze.customer"], source="crm_customer_raw")
print(render_sql(flow))
```

For `bronze.customer` of the example (scd2, `sequence_by: [versionnumber]`, `delete_when: "isdelete = true"`, `ignore_columns: [modifiedon]`) this prints:

```sql
CREATE OR REFRESH STREAMING TABLE bronze.customer;

CREATE FLOW bronze_customer_flow AS AUTO CDC INTO
  bronze.customer
FROM STREAM crm_customer_raw
KEYS
  (accountid)
APPLY AS DELETE WHEN
  isdelete = true
SEQUENCE BY
  versionnumber
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (modifiedon);
```

It is a translation, not an executor. AUTO CDC runs only inside a Lakeflow Declarative Pipeline, where the platform owns the dependency graph. Every statement ends with `;`, so rendered tables can be joined into one pipeline file. `source` is the upstream streaming table you name: the flow reads `FROM STREAM`, because Databricks requires a streaming source.

| Pipetree option | AUTO CDC |
| --- | --- |
| `business_key` | `KEYS` |
| `sequence_by` | `SEQUENCE BY` |
| `delete_when` | `APPLY AS DELETE WHEN` |
| `strategy: scd1` / `scd2` | `STORED AS SCD TYPE 1` / `2` |
| `ignore_columns` (scd2) | `TRACK HISTORY ON * EXCEPT (...)` |

Only scd1 and scd2 translate. `replace` and `append` raise an error: a plain streaming table or materialized view does the job. A table with no `business_key`, or with `surrogate_key`, is refused.

## Differences

| | pipetree | AUTO CDC |
| --- | --- | --- |
| scd2 history columns | `_valid_from`, `_valid_to`, `_is_current` | `__START_AT`, `__END_AT`, filled from the `SEQUENCE BY` values |
| Audit columns | `_inserted_at`, `_execution_id`, ... | none |
| Duplicates of a key in one micro-batch | reduced to one row first | each becomes a history version |
| Older sequence | by sequence, when `sequence_by` is set | by sequence: scd1 keeps the higher, scd2 inserts an earlier version |
| `delete_mode` | `soft`, `hard` (scd1), `ignore` | only `ignore` is modeled (delete signal dropped). Other modes follow AUTO CDC's own behavior for the SCD type: check the Databricks docs. |
