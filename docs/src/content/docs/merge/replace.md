---
title: replace
description: Overwrite the table with the result on every run.
sidebar:
  order: 1
---

```yaml validate
silver:
  tables:
    orders:
      logic: notebooks/silver/orders.sql
      business_key: [order_id]
      strategy: replace
      depends_on: auto
```

Every run overwrites the table with the result of the source or logic file. Nothing from earlier runs stays, and the schema is whatever the result says (no [schema policy](../../concepts/notes-and-run-log/#schema-policy)).

| Topic | Rule |
| --- | --- |
| Rows | Kept as they come: no dedupe, no NULL-key drop. A NULL key is data here. |
| Audit columns | `_inserted_at`, `_updated_at`, `_is_deleted`, `_execution_id`, `_source_system` are stamped on every row. |
| `--init` | Changes nothing: a run already rewrites the table. |
| `unknown_member` | Re-added after each rewrite. Rows with a NULL key column are dropped, see [Merge options](../merge-options/#unknown-member). |
| `merge` options | Not used. |

Use it for derived tables that are cheap to rebuild: silver and gold tables that select from other tables.
