---
title: scd1
description: Keep one current row per key; changes overwrite.
sidebar:
  order: 3
---

```yaml validate
systems:
  crm:
    type: csv
    path: data/customer.csv

bronze:
  tables:
    customer:
      source: { system: crm, object: account }
      business_key: [id]
      strategy: scd1
      merge:
        sequence_by: [seq]
        delete_when: "op = 'D'"
        delete_mode: soft
```

One row per `business_key`. A new key is inserted, a changed row is updated in place, no history is kept. Three runs of this table, `seq` is the `sequence_by` column:

| Run | Source rows | Table after the run |
| --- | --- | --- |
| 1 | `1 Berlin seq 1`, `2 Bonn seq 1` | `1 Berlin`, `2 Bonn` |
| 2 | `1 Hamburg seq 2`, `3 Köln seq 1` | `1 Hamburg`, `2 Bonn`, `3 Köln` |
| 3 | `1 Paris seq 1` (older) | unchanged: seq 1 is lower than the stored seq 2 |
| 4 | `2 D seq 2` (delete) | `2 Bonn` with `_is_deleted = true`, other values kept |
| 5 | `2 Bonn seq 3` | `2 Bonn` with `_is_deleted = false` |

Rules:

- **Duplicates in a batch**: the row with the highest `sequence_by` wins, the rest are dropped and noted. Without `sequence_by` the winner is arbitrary.
- **Across batches**: with `sequence_by`, a row older than the stored one is ignored, delete rows included. Equal or newer wins, so on a tie the later batch wins. NULL sorts lowest. Without `sequence_by` the last batch wins.
- **Same batch twice**: no change, no `_execution_id` bump.
- **Deletes**: `soft` marks the row deleted, `hard` removes it, `ignore` drops delete rows. A delete for an unknown key does nothing. A soft-deleted key that comes back is active again, even with identical values. A hard-deleted key that comes back is a new insert, and a late older row for it simply inserts too.
- **`ignore_columns`**: if only those columns changed, the row is updated in place and `_execution_id` stays.
- **NULL keys**: a row whose key is NULL (all key columns NULL for a composite key) is dropped and noted. See [NULL business keys](../merge-options/#null-business-keys).
- **Schema**: `schema_policy` applies.

Options in detail: [Merge options](../merge-options/).
