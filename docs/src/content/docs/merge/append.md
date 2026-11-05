---
title: append
description: Add the result to the table on every run.
sidebar:
  order: 2
---

```yaml validate
bronze:
  tables:
    events:
      source: { system: crm, object: events }
      strategy: append

systems:
  crm:
    type: csv
    path: data/events.csv
```

Every run adds the rows of the source to the table. Existing rows are never changed or removed.

| Topic | Rule |
| --- | --- |
| Rows | Kept as they come: no dedupe, no key needed, no NULL-key drop. A repeated batch is appended again. |
| Audit columns | The same five as `replace`, so `_execution_id` says which run added a row. |
| Retries | A retried run first deletes the rows of its own `_execution_id`, so a partial write is not duplicated. |
| `--init` | Starts the table over with this run's rows. |
| Schema | No schema policy is enforced. |
| `merge` options | Not used. |

Use it for logs and event data where every delivery is new. If a key can arrive twice, use [scd1](../scd1/).
