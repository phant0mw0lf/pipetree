# pipetree.testing

Test tooling for pipetree's merge strategies. Not a stable API.

- `merge_model` - pure-Python reference model of `replace`, `append`, `scd1`,
  `scd2` (the documented merge rules). No pyspark import. With `sequence_by`, scd1
  also orders rows across batches: an incoming row older than the stored row is
  ignored (delete rows too), equal or newer wins; NULL sorts lowest. Without
  `sequence_by` (any strategy), and for replace/append, the last batch wins. With `sequence_by`, scd2
  orders the versions of a key by their sequence tuple (NULL lowest; a tie across batches:
  the later batch is the newer). A row older than the key's latest version is a late
  arrival: a delete row is ignored; a data row equal (tracked columns, `sequence_by`
  columns included) to the live version valid at its sequence is a no-op; any other becomes
  a closed earlier version; no other version changes and the current version stays the one
  with the highest sequence. Validity (`_valid_from`/`_valid_to`) is processing time and a
  late version never was valid in it, so it has an empty interval (`_valid_from` =
  `_valid_to` = the `_valid_from` of the next later version, of the first version if it is
  older than all) and appears in history order only; a point-in-time query never returns
  it. Known limit: a delete row's own sequence is not stored, so after a delete a row with
  a sequence >= the deleted version's reopens the key. Known limit: a
  hard delete removes the row, so a later older row for that key simply inserts.
- `merge_gen` - seeded generator of valid table configs and batch histories
  (`generate_case(seed, strategy)`, `coverage(case)`). No pyspark import.
- `merge_harness` - runs a case through `SparkAdapter.run_table` on a given
  Spark session and compares the table with a model after every batch, plus
  model-independent invariants (`run_case`, `record_case`,
  `compare_recording`, `attribution=True` to keep comparing after a divergence).

The pytest wrappers are in `tests/merge_conformance/` (deviations and their
regeneration: `known_divergences.py`, `quirks.py`, `regen.py`).

## Runtime

A case takes about 4-6 s on local Spark + Delta (2-6 batches of about 1 s each,
plus an idempotence re-run). With the default `PIPETREE_CONFORMANCE_SEEDS=25`
the 100 generated cases add about 8-10 minutes to the test suite;
`PIPETREE_CONFORMANCE_SEEDS=0` turns them off, and `pytest -m "not conformance"`
skips every slow conformance test. 300 seeds per strategy take about
1 h 40 min serially; run several strategies in parallel only on a machine with
spare cores (four local JVMs on 16 cores were slower than one).
