---
title: scd2
description: Keep the history of a key as versions.
sidebar:
  order: 4
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
      strategy: scd2
      merge:
        sequence_by: [seq]
        delete_when: "op = 'D'"
        ignore_columns: [modified_on]
```

One row per version. The current version has `_is_current = true`. A change in a tracked column closes it and opens a new one. `scd2` also adds `_valid_from` and `_valid_to`. Runs for key 1, `seq` is the `sequence_by` column:

| Run | Source row | Versions after the run, oldest first |
| --- | --- | --- |
| 1 | `Berlin seq 2` | `Berlin` current |
| 2 | `Hamburg seq 3` | `Berlin` closed, `Hamburg` current |
| 3 | `Paris seq 1` (late) | `Paris` closed, `Berlin` closed, `Hamburg` current |
| 4 | delete, `seq 3` | `Hamburg` closed and `_is_deleted = true` |
| 5 | `Rom seq 4` | a new current `Rom` version, the deleted one stays closed |

Rules:

- **Order**: with `sequence_by`, versions are ordered by it, not by arrival. A tie across batches: the later batch is newer. NULL sorts lowest.
- **Late rows**: a row older than the latest version is inserted as a closed version after the version valid at its sequence (first, if none is older). If it equals that version in the tracked columns it is a no-op. A late delete row is ignored. No other version changes, and the version with the highest sequence stays current.
- **Late versions have an empty validity interval.** `_valid_from` and `_valid_to` are processing times, and a late version was never valid in processing time. It shows in the history but a point-in-time query never returns it.
- **Unchanged row**: no new version. Same batch twice: no change.
- **`ignore_columns`**: if only those columns changed, the current version is updated in place, no new version.
- **Deletes**: `soft` closes the current version and marks it deleted. A returning key opens a new version. `hard` is rejected for scd2 when the config is loaded, because it would destroy history. `ignore` drops delete rows.
- **NULL keys** and duplicates in a batch behave as in [scd1](../scd1/).
- **Limit**: the sequence of a delete row is not stored. After a delete, a row with a sequence at or above the deleted version's reopens the key, even if the delete was newer.

Read the current state with `WHERE _is_current = true`. Options: [Merge options](../merge-options/).
