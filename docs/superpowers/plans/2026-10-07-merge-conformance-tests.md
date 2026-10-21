# Merge-strategy conformance tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove that pipetree's four merge strategies (`replace`, `append`, `scd1`, `scd2`) and their options are correct: an independent pure-Python reference model, fed random multi-batch histories, must equal the table pipetree produces after every batch; every divergence is a finding that ends as a pipetree fix or a deliberate model change.

**Architecture:** `pipetree.testing` (test tooling inside the package, no new runtime dependency): `merge_model` (pure Python, the rules of the blog + the user's 8 decisions), `merge_gen` (seeded random generator of valid table configs and batch histories), `merge_harness` (runs a history through the real `SparkAdapter.run_table` on a given `spark`, normalises the table, compares it with the model, checks independent invariants, returns divergences with a reproducing seed). pytest wrappers live in `tests/merge_conformance/`; a Databricks job in scale-bench (Task 4) imports the same modules from the pinned wheel and runs the same seeds on DBR.

**Tech Stack:** Python 3.12, PySpark 4.0 + Delta (local `build_local_session`), seeded `random` (no Hypothesis), pytest.

**Spec:** `docs/superpowers/specs/2026-10-07-merge-conformance-tests-design.md` (decisions 1-8 are binding; the model comments cite them as `# decision N`).

## Global Constraints

- Repo `~/dev/pipetree`, branch `feat/merge-conformance` in a worktree (never on `main`). `src/pipetree/testing/` is documented as test tooling, not a stable API; it must import nothing from `pyspark` at import time of `merge_model` and `merge_gen` (the harness may import pyspark lazily) so the model can be used and unit-tested without Spark.
- `uv run ruff check .` and `uv run ruff format --check .` clean; run the full `uv run pytest -q > file 2>&1; echo $?` once before each commit (about 4 minutes; never judge through a pipe). On this sandbox Spark tests need `SPARK_LOCAL_IP=127.0.0.1`, otherwise they skip with a BindException.
- Commits signed via the 1Password SSH agent, which flakes: retry, NEVER bypass. Every commit message ends with `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`. Never add `docs/superpowers` or `.superpowers` to a commit. No push by implementers.
- **Decisions 1-8 (binding):** (1) scd1 soft-deleted key returns with identical values -> active again, `_is_deleted=false`, `_execution_id` moves; (2) scd2 returning deleted key -> NEW current version, closed version stays closed; (3) late/older `sequence_by` in a later batch: last batch wins; (4) NULL key without `unknown_member`: single-column key NULL -> dropped (WARNING + note), composite key: NULL in one component is a value (null-safe match), dropped only when the WHOLE key is NULL; `unknown_member` tables drop a row with a NULL in ANY key column (unchanged); (5) `delete_mode: ignore`: delete rows removed BEFORE dedupe; (6) delete for an absent key: no-op; (7) scd1 hard delete then return: fresh insert (new `_inserted_at`, new surrogate key); (8) `ignore_columns` changing alone: update the current row in place, no new scd2 version, `_updated_at`/`_execution_id` NOT bumped.
- Where the existing code differs from a decision, **the model wins and the divergence is a finding**; the harness test for that seed is marked `xfail(strict=True)` with the finding id in `tests/merge_conformance/known_divergences.py` until the fix lands (Task 3), so the suite stays green and a fixed bug turns the xfail into a failure that forces removal of the entry.

## Review Focus

- The model must not be a transcription of `merge.py`: it is written from the rules; reviewers compare it with the spec text, not the code.
- Generated histories must really reach the interesting cases (resurrection, deletes of absent keys, duplicates with equal/different sequence values, ignored-column-only changes, empty batches, repeated batches, NULL keys in single and composite keys); a coverage counter in the harness proves it (a generator that never produces a resurrection would pass vacuously).
- A tie in `sequence_by` or several rows per key without `sequence_by` is only generated between IDENTICAL rows (otherwise the winner is arbitrary and the comparison meaningless).
- Failures must be reproducible from one line (`seed`, config, batches) and a divergence report must show the first batch where model and table differ, the model rows and the table rows.
- Timestamps are never compared by value (only ordering/relations); `_execution_id` IS compared (the harness passes known increasing ids).

---

### Task 1: The reference model (pure Python)

**Files:**
- Create: `src/pipetree/testing/__init__.py` (docstring: test tooling, not a stable API), `src/pipetree/testing/merge_model.py`
- Test: `tests/merge_conformance/__init__.py`, `tests/merge_conformance/test_merge_model.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class ModelTable`: `strategy: str` (`replace|append|scd1|scd2`), `key: tuple[str, ...]`, `columns: tuple[str, ...]` (every non-audit column incl. key columns and `op`), `sequence_by: tuple[str, ...] = ()`, `ignore: tuple[str, ...] = ()`, `delete_mode: str = "soft"` (`soft|hard|ignore`), `has_delete: bool = False` (a source row with `op == "D"` is a delete; non-delete rows carry `op == "U"`), `unknown_member: bool = False`, `sentinels: Mapping[str, Any] = {}` (unknown-member value per key column: `-1` for int keys, `None` for string keys).
  - `@dataclass class BatchStats`: `duplicates_dropped: int | None` (None for replace/append), `null_keys_dropped: int`.
  - `apply_batch(table: ModelTable, state: list[dict], batch: list[dict], execution_id: int, *, init: bool = False) -> tuple[list[dict], BatchStats]` - pure; `state` rows are dicts of the table columns plus `_is_deleted: bool`, `_execution_id: int`, and for scd2 `_is_current: bool`; scd2 rows of one key are kept in version order (oldest first). Returns a NEW state (never mutates the input).
  - `UNKNOWN = "__unknown_member__"` helper: `is_unknown_member(table, row) -> bool` (key equals the sentinels and `_is_deleted` is false).

- [ ] **Step 1: Write the failing tests** `tests/merge_conformance/test_merge_model.py` (pure Python, no Spark). Use a helper `T(strategy, **kw)` building a `ModelTable` with `key=("id",)`, `columns=("id","v","w","seq","op")`, and `B(*rows)` building batch rows with `op="U"` unless given. One test per rule, each asserting the exact resulting state (`_execution_id` values are the batch numbers 1, 2, 3 used as execution ids):
  1. `test_replace_is_the_last_batch` / `test_append_accumulates_and_init_resets`.
  2. `test_scd1_insert_update_untouched`: batch1 `{id:1,v:"a"}`; batch2 `{id:1,v:"b"}` -> v "b", `_execution_id` 2; batch3 same `{id:1,v:"b"}` -> `_execution_id` stays 2.
  3. `test_scd1_dedupe_picks_highest_sequence_and_counts` (rows `seq` 1 and 2 for one key -> the seq-2 row, `duplicates_dropped == 1`; None sequence loses; three rows -> 2 dropped); `test_dedupe_without_sequence_by_only_identical_rows_in_generator_but_model_takes_first`.
  4. `test_scd1_soft_delete_marks_and_keeps_row`; `test_scd1_hard_delete_removes_row`; `test_delete_for_absent_key_is_a_noop` (decision 6, soft and hard, scd1 and scd2).
  5. `test_scd1_soft_deleted_key_returns_active_again_even_with_identical_values` (decision 1).
  6. `test_scd1_hard_delete_then_return_is_a_fresh_insert` (decision 7: `_execution_id` of the return batch; row count 1).
  7. `test_delete_mode_ignore_removes_delete_rows_before_dedupe` (decision 5): batch with a delete row (seq 5) and a normal row (seq 1) for the same key -> the normal row wins and is inserted; a key with only a delete row is not inserted; `duplicates_dropped` counts only the remaining duplicates.
  8. `test_scd2_change_closes_and_opens`, `test_scd2_unchanged_row_untouched`, `test_scd2_soft_delete_closes_without_new_version` (closed row `_is_deleted True`, no current row), `test_scd2_deleted_key_returns_as_new_current_version` (decision 2: 2 rows: the closed deleted one and a new current one with `_is_deleted False`).
  9. `test_ignore_columns_alone_update_in_place_without_bump` (decision 8, scd1 and scd2: changed ignored column -> value updated, `_execution_id` unchanged, scd2 row count unchanged; changed tracked + ignored column -> a new scd2 version carrying the new ignored value).
  10. `test_late_older_sequence_still_overwrites` (decision 3).
  11. NULL keys (decision 4): `test_single_key_null_row_is_dropped_and_counted`; `test_composite_key_with_one_null_component_is_a_value` (inserted once, matched again on the next batch -> updated/untouched, NOT duplicated); `test_composite_key_all_null_row_is_dropped`; `test_unknown_member_table_drops_a_row_with_any_null_key_column`.
  12. `test_unknown_member_row_is_seeded_once_and_never_touched` (int sentinel -1 and string sentinel None; present after replace re-creates the table; never matched by source rows; scd2 row is `_is_current True`).
  13. `test_init_rebuilds_from_the_batch_only` (every strategy; scd2: no closed history; delete rows in an init batch produce no rows).
  14. `test_apply_batch_does_not_mutate_its_input`.
- [ ] **Step 2: Run to verify they fail** (`uv run pytest tests/merge_conformance/test_merge_model.py -q > /tmp/mm.log 2>&1; echo $?` -> non-zero).
- [ ] **Step 3: Implement `merge_model.py`.** Algorithm (all steps pure, in this order; comment each with its rule and decision number):
  1. `init`: start from an empty state for every strategy (a full reload is the batch applied to nothing; for append/replace the same).
  2. Null-key rule (decision 4): `unknown_member` table -> drop rows with ANY key column None; else single key -> drop rows with None key; composite -> drop rows whose key columns are ALL None. Count `null_keys_dropped`.
  3. `scd1`/`scd2` only: if `has_delete and delete_mode == "ignore"` remove rows with `op == "D"` (decision 5). Then dedupe: group by key tuple (None is a value; `None == None`); winner = row with the greatest `sequence_by` tuple where None sorts lowest (`desc_nulls_last`); with no `sequence_by` take the first row of the group; `duplicates_dropped = rows_before - winners`. `replace`/`append`: no dedupe, `duplicates_dropped=None`.
  4. Apply per strategy to a copy of the state:
     - `replace`: state = the rows (plus audit `_is_deleted False`, `_execution_id n`); `append`: state + rows.
     - `scd1` for each winner (key k): delete (`has_delete and op == "D"`): soft -> if k exists set `_is_deleted True`, `_execution_id n`; hard -> remove; (ignore mode already removed) ; absent key -> no-op (decision 6). Non-delete: absent -> insert (`_is_deleted False`, `_execution_id n`); present -> `tracked = any(existing[c] != new[c] for c in columns if c not in key and c not in ignore)`; if `tracked or existing._is_deleted` -> overwrite all non-key columns, `_is_deleted False`, `_execution_id n` (decision 1); elif any ignored column differs -> overwrite only the ignored columns (decision 8, no bump); else untouched.
     - `scd2` per winner: current = the row of k with `_is_current`; delete: current exists -> `_is_current False`, `_is_deleted True`, `_execution_id n`; absent -> no-op. Non-delete: no current (new key or returning, decision 2) -> append a new current row (`_is_deleted False`, `_is_current True`, `_execution_id n`); current exists -> if tracked columns differ: `_is_current False`, `_execution_id n` on the old row (its `_is_deleted` stays False) and append the new current version (`_execution_id n`); elif only ignored columns differ -> overwrite the ignored columns on the current row in place, no bump (decision 8); else untouched.
  5. `unknown_member` tables: after the strategy, if no unknown-member row exists, add one (key columns = `sentinels`, every other column None, `_is_deleted False`, `_execution_id n`, scd2 `_is_current True`); an existing one is never changed (for `replace` the table was rebuilt, so it is re-added with the new `_execution_id`).
- [ ] **Step 4: Run the tests** -> exit 0; `uv run ruff check . && uv run ruff format --check .`.
- [ ] **Step 5: Commit** (`feat(testing): reference model for the merge strategies`).

---

### Task 2: Generator, harness, invariants, first run (findings)

**Files:**
- Create: `src/pipetree/testing/merge_gen.py`, `src/pipetree/testing/merge_harness.py`
- Create: `tests/merge_conformance/known_divergences.py`, `tests/merge_conformance/test_conformance.py`, `tests/merge_conformance/test_generator.py`
- Test: those test files

**Interfaces:**
- Consumes (Task 1): `ModelTable`, `apply_batch`, `BatchStats`.
- Produces `merge_gen`:
  - `@dataclass(frozen=True) class Case`: `seed: int`, `table: ModelTable`, `delete_when: str | None` (`"op = 'D'"` when `has_delete`), `surrogate_key: str | None`, `batches: list[list[dict]]` (each batch a list of source rows), `inits: frozenset[int]` (batch indexes run with `init=True`), `repr_line() -> str` (a one-line reproduction: seed + config).
  - `generate_case(seed: int, strategy: str) -> Case` - deterministic from `(seed, strategy)`.
  - `coverage(case: Case) -> set[str]` - the interesting cases a case contains (names: `duplicate_rows`, `duplicate_with_equal_sequence`, `delete_row`, `delete_for_absent_key`, `resurrection`, `ignored_only_change`, `tracked_change`, `empty_batch`, `repeated_batch`, `null_key`, `partial_null_key`, `late_older_sequence`, `init_midway`).
- Produces `merge_harness`:
  - `@dataclass class Divergence`: `case: Case`, `batch_index: int`, `kind: str` (`rows|stats|invariant`), `detail: str`; `__str__` prints the repro line, the batch index, model rows and table rows.
  - `run_case(spark, case: Case, schema: str = "mc") -> list[Divergence]` - builds the pipetree `Table` (`Table.model_validate` as `tests/adapters/spark/test_merge.py::make_table`), a `SparkAdapter(spark, systems={...}, base_dir=..., read_source=<fn returning spark.createDataFrame(batch)>)` and calls `adapter.run_table(table, execution_id=i+1, init=(i in case.inits))` for batch i (execution ids 1..n); after each batch normalises the table (see below), runs `apply_batch` on the model state and compares; runs the invariants; returns every divergence (stops at the first diverging batch of a case: later batches would only echo it).
  - `normalise(df, model_table) -> list[dict]`: business columns + `_is_deleted` + `_execution_id` (+ `_is_current` for scd2); rows sorted by key tuple (None first), for scd2 then by `_valid_from`; unknown-member row compared WITHOUT its `_execution_id`.
  - invariants (independent of the model): scd2: at most one current row per key; per key the versions are ordered and non-overlapping (`_valid_to` of version i == `_valid_from` of version i+1 for closed rows, `_valid_to` NULL only on the current row); closed versions never change between batches (same business values, same `_valid_from`/`_valid_to`); scd1/scd2: `_inserted_at` of a surviving row never changes and `_updated_at >= _inserted_at`; unknown-member row exists at most once and never changes; with a `surrogate_key`: sids unique, a row keeps its sid across batches, unknown member sid is -1; idempotence: running a batch twice in a row changes nothing (checked on every case by re-applying the last batch once more for scd1/scd2/replace and comparing with the state before - decision-3 compatible because the same batch has the same content).
  - `run_many(spark, strategy, seeds) -> list[Divergence]`.

- [ ] **Step 1: Failing tests.** `test_generator.py` (no Spark): determinism (`generate_case(s, "scd2") == generate_case(s, "scd2")`), validity (every case's table has a key subset of columns, `sequence_by`/`ignore` subsets of columns and disjoint from the key, scd2 never with `delete_mode == "hard"`, `delete_mode` only set when `has_delete`, no row repeats a key with different content and equal sequence, no key `-1` in int keys, batches 2-6, rows per batch 0-8 over 3-8 keys), and COVERAGE: over seeds 0-199 for each of scd1 and scd2 the union of `coverage()` contains every name listed above (a generator that never reaches `resurrection` must fail this test). `test_conformance.py` (Spark): the harness smoke test with one hand-built `Case` per strategy (scd1 insert/update/soft-delete/resurrect; scd2 change/delete/return) asserting `run_case(...) == []` against the CURRENT pipetree where the decisions agree with it and listing the known divergence otherwise via `known_divergences`; a liveness test: monkeypatch the model's `apply_batch` to return a wrong state and assert `run_case` reports a `rows` divergence with a reproducing repr line.
- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement the generator.** Config space per case: key `("id",)` int | `("id",)` str | composite `("a","b")` ints; `columns = key + ("v","w","seq","op")` (+`("seen",)` as the volatile ignorable column when `ignore` is chosen); `sequence_by` none | `("seq",)` | `("seq","w")`; `ignore` none | `("seen",)`; for scd1: `delete_mode` soft|hard|ignore with `has_delete` true in 60% of cases, scd2: soft|ignore; `unknown_member` 25% (sentinels `-1` for int key columns, `None` for string); `surrogate_key` 50% of unknown_member scd1/scd2 cases. Histories: key space 3-8 keys; each batch 0-8 rows, each row: chosen key (reuse recent keys 70%), `v` from a small string set, `w` small int or None, `seq` increasing within the history 60% / random 40% (late/older), `seen` random, `op` `"D"` with 20% when `has_delete` else `"U"`; deliberate injections with 15% each: a duplicate of a row (identical, or the same key with different `v` and DIFFERENT `seq` when `sequence_by` is set; same key different content only when sequence values differ), a delete for an absent key, a NULL key row (all-NULL, partial-NULL for composite), a verbatim repeated batch, an empty batch, an `init` at a random batch index (10%). All randomness from `random.Random(f"{strategy}:{seed}")`.
- [ ] **Step 4: Implement the harness** as specified; note pipetree specifics the implementer must read from the code: `SparkAdapter.run_table` strips reserved columns, drops NULL keys on `unknown_member` tables, dedupes, merges and seeds the unknown member; `Table.model_validate` field names (`business_key`, `merge: {sequence_by, delete_when, delete_mode, ignore_columns}`, `unknown_member`, `surrogate_key`, `strategy`, `table_schema`, `fqn`) as in `tests/adapters/spark/test_merge.py::make_table`; every case gets its own table name (`case_<strategy>_<seed>`) in schema `mc` and drops it afterwards.
- [ ] **Step 5: First run and findings.** Run `SPARK_LOCAL_IP=127.0.0.1 PIPETREE_CONFORMANCE_SEEDS=300 uv run pytest tests/merge_conformance -q -x --no-header > /tmp/conf.log 2>&1` per strategy (replace, append, scd1, scd2; seeds 0-299; env var read by `test_conformance.py`, default 25 in CI). For EVERY diverging seed, minimise by hand if cheap (drop batches/rows while it still diverges), classify the divergence against the decisions and the spec rules, and write one entry per DISTINCT root cause in `tests/merge_conformance/known_divergences.py` as `{finding_id: (description, [list of (strategy, seed)])}`; `test_conformance.py` marks those (strategy, seed) pairs `xfail(strict=True, reason=<finding id + description>)`. Every divergence must be explained: either "pipetree differs from decision N" (finding, fix in Task 3) or "model bug" (fix the model and its Task 1 test, rerun). **Report the table of findings** (id, decision, minimal repro, seeds) in the report file; do not fix pipetree in this task.
- [ ] **Step 6: ruff, full suite (`SPARK_LOCAL_IP=127.0.0.1`), commits** (`feat(testing): case generator and conformance harness with invariants`, `test: conformance suite and known divergences (findings F1..Fn)`).

---

### Task 3: Fix the findings (controller writes the task list after Task 2)

The findings of Task 2 define this task. Expected candidates (from reading the code; to be confirmed or refuted by the first run): decision 1 (scd1 returning soft-deleted key stays deleted: `merge_scd1` change condition ignores `_is_deleted`), decision 4 (NULL keys re-inserted on tables without `unknown_member`; composite partial-NULL keys not null-safe), decision 5 (`ignore` mode currently dedupes before removing delete rows), decision 8 (ignored column alone never updates), and the schema note wording for `schema_policy: ignore` (reports `added` for a column that is not written). After Task 2 the controller turns each distinct finding into one fix task (TDD: the divergence repro becomes a failing regression test in `tests/adapters/spark/test_merge.py`, then the fix in `src/pipetree/adapters/spark/merge.py` / `dedupe.py` / `schema/`, then the `xfail` entry is removed), reviewed like any task, in a separate pipetree PR per behaviour change so each can be reviewed and reverted alone.

---

### Task 4: Databricks job (scale-bench)

**Prerequisite:** the pipetree PRs of Tasks 1-3 are merged and the scale-bench pin points to the merge commit. Branch from `feat/live-notebook-runner` (PR #1 branch) in a scale-bench worktree; read `runners/databricks/databricks.yml`, `scripts/edge_cases/gen_job.py`, `runners/databricks/run_edge_case_phase_notebook.py` first and follow their conventions.

**Files:**
- Create: `scripts/merge_conformance/__init__.py`, `scripts/merge_conformance/run.py` (`run_strategy(spark, strategy, seeds, schema) -> list[str]` using `pipetree.testing`), `runners/databricks/run_merge_conformance_notebook.py` (widgets `catalog`, `strategy`, `seeds`; markdown cell explaining the strategy and its rules; one cell runs the seeds with a progress line per 50 seeds; results table `edge_meta.conformance_results` (strategy, seeds, failures, finished_at, divergences array<string>); raises after recording if any divergence)
- Modify: `runners/databricks/databricks.yml` (job `pipetree_merge_conformance`: one task per strategy `replace`, `append`, `scd1`, `scd2`, no `depends_on` (they use different tables, so they can run in parallel on separate single-node clusters or sequentially on one shared single-node cluster - use ONE shared job cluster and a `depends_on` chain to stay inside quota), then `summary` with `run_if: ALL_DONE`), README + RUNBOOK sections, `tests/runners/test_databricks_bundle.py`.
- [ ] Tests first (bundle structure: tasks == strategies + summary, widgets == base_parameters, notebook exec test with a fake `run_strategy` for pass and fail paths incl. record-before-raise and unexpected-error path, as in the edge-case job tests), then implementation, docs, `bundle validate -t dev` (validate only; the controller deploys and runs).
- Controller (not a subagent): deploy, `bundle run pipetree_merge_conformance -t dev --params seeds=300`, read `edge_meta.conformance_results`, report divergences found on DBR's Delta versus local; each runtime-specific divergence becomes a finding.

---

## Self-review (against the spec)

- Spec rules + decisions 1-8: model Task 1 (tests 1-14 map to rules/decisions); generator coverage test guarantees the histories reach them; harness invariants cover scd2 history integrity, immutability of closed versions, unknown member, surrogate keys, idempotence, init.
- Both targets (local + Databricks): Tasks 2 and 4; Fabric Spark 4.1 later through the same `pipetree.testing` modules.
- Known unknowns are explicit: Task 3 is defined by the first run; every divergence is classified, none silently absorbed.
