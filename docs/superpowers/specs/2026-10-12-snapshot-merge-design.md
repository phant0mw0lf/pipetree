# Snapshot merge for scd1 / scd2 (target: 0.2.0)

Status: draft, decided by the maintainer on 2026-10-12: after 0.1.0, as 0.2.0.

## Problem

A source that delivers no delta (a full extract every run) and no delete flag. The maintainer wants the same
result as if the source sent a change feed: an scd2 table with the history (or an scd1 table) that downstream
layers (Power BI, incremental loads) can read incrementally.

What exists today: scd1/scd2 merge the batch against the target by `business_key` and compare every tracked
column, so inserts and updates from a full snapshot already produce the right rows and versions. What is
missing: a key that is in the target but NOT in the new snapshot stays active (merge.py has no "not matched
by source" branch). Deletes are only recognised through `merge.delete_when`.

## Proposal

New merge option, scd1 and scd2 only:

```yaml
merge:
  snapshot: true          # the batch is the complete current state of the source
  delete_mode: soft       # soft (default) | hard (scd1 only) | ignore (= keep missing keys active)
  snapshot_guard:         # protection against a half-delivered extract
    max_missing_ratio: 0.5   # fail the table if more than this share of the active keys is missing
    allow_empty: false       # an empty batch is an error, not "delete everything"
```

Semantics: after the usual batch preparation (NULL keys dropped, duplicates resolved), every ACTIVE key of
the target (scd1: not `_is_deleted`; scd2: has a current, not-deleted version) that is not in the batch is
handled exactly like a delete row for that key: soft = marked deleted (scd2: current version closed, deleted),
hard = removed (scd1). A key that returns later is revived as today. Changes of present keys: unchanged
(all tracked columns, `ignore_columns` excluded).

The result equals a source that sent updates plus deletes, so `_is_deleted`, `_is_current`, `_valid_from/_to`
and `_execution_id` look the same as with a CDC source.

## Decisions to make before building

1. `sequence_by` with `snapshot`: a missing-key delete has no sequence. Proposal: it applies to the current
   state regardless of sequence (a snapshot is "now"); document it.
2. `snapshot` together with `delete_when`: allow (explicit deletes in the snapshot still apply) or reject.
3. Guard defaults: `max_missing_ratio` default and whether a violated guard fails the table or only warns
   (proposal: fails, the table is `failed` and nothing is written; the note names the counts).
4. First load and `--init`: no deletes (nothing in the target). A table that was empty: no guard.
5. Partial runs (`--select`, tags): unchanged, the guard and the deletes are per table.
6. Downstream "changes": nothing new is needed for scd2 (`_valid_from`, `_execution_id`, `_is_current`);
   optionally enable Delta Change Data Feed on request (`delta.enableChangeDataFeed`), a separate small option.
7. Run-log notes: `snapshot: N key(s) not in the source were deleted`.

## Work

- Model + loader validation (scd1/scd2 only, `delete_mode` combinations), JSON schema and YAML reference.
- merge.py (scd1 and scd2) and the reference model (`merge_model.py`, independent code), generator (snapshot
  cases: a history of full snapshots whose key sets shrink and grow), coverage names, quirks model.
- Guard with a clear error and a note.
- Docs: merge/snapshot (or a section in merge-options), short.
- Databricks gate on a dev tier: a table with a full-snapshot source over several runs, deletes and returns;
  cost a few USD.
