---
title: Snapshot merge
description: The batch is the full current state of the source; keys it no longer has are deleted.
sidebar:
  order: 5
---

```yaml validate
systems:
  erp:
    type: csv
    path: data/product.csv

bronze:
  tables:
    product:
      source: { system: erp, object: product }
      business_key: [id]
      strategy: scd2
      merge:
        snapshot: true
        delete_mode: soft
        snapshot_max_missing_ratio: 0.5
```

Some sources deliver a full extract on every run and no delete flag. With `merge.snapshot: true` the batch counts as the complete current state, so a key that is active in the table but not in the batch is deleted, as if the source had sent a delete row for it. scd1 and scd2 only. Runs of this table:

| Run | Source rows | Table after the run |
| --- | --- | --- |
| 1 | `1`, `2`, `3`, `4` | four current versions |
| 2 | `1`, `2`, `3` | `4` closed and `_is_deleted = true` |
| 3 | `1`, `2`, `3`, `4` | a new current version of `4`, the deleted one stays closed |
| 4 | `1` | fails: 3 of 4 active keys are missing, more than `0.5`. Nothing is written. |

Rules:

- **Active keys**: scd1 rows with `_is_deleted = false`, scd2 keys with a current version that is not deleted. The unknown member is never one and is never touched.
- **A missing key** is handled like a delete row for it, following `delete_mode`. scd1 `soft` marks the row deleted, `hard` removes it. scd2 closes the current version and marks it deleted, no new version. The audit columns change as for a delete row. With `ignore` missing keys stay active and the guard does not apply.
- **`sequence_by`**: a missing key has no sequence. It is deleted whatever sequence is stored, and scd1 keeps the stored sequence values. A key that returns later follows the usual rules of [scd1](../scd1/) and [scd2](../scd2/), so on scd1 a returning row with a lower sequence than the stored one is ignored.
- **`delete_when`** still works: a delete row for a key counts as present and is an ordinary delete.
- **Batch preparation first**: NULL keys are dropped, `delete_mode: ignore` rows are removed and duplicates are resolved before the missing keys are determined.
- **First load and `--init`** delete nothing and skip the guard.
- **Note**: the table gets the note `snapshot: n key(s) not in the source were deleted` and the run log column `snapshot_deleted`. See [Notes and run log](../../concepts/notes-and-run-log/).

## Guard

A half-delivered extract would delete most of the table. Before anything is written the table fails with a `SnapshotGuardError` when:

| Case | Option |
| --- | --- |
| The batch is empty and the table has active keys | `snapshot_allow_empty` (default `false`). With `true` every active key is missing and the ratio below still applies. |
| More than this share of the active keys is missing | `snapshot_max_missing_ratio` (default `0.5`, from 0 to 1). A share exactly at the limit passes. |

To delete every key with an empty batch set `snapshot_allow_empty: true` and `snapshot_max_missing_ratio: 1`.

Options in detail: [Merge options](../merge-options/).
