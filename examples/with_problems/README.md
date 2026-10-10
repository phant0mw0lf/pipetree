# The example pipeline, with problems

The same pipeline as `examples/pipeline.yaml` (same seven tables), run over
data that misbehaves on purpose, to show what pipetree reports for each
problem. Every problem is one that *succeeds* quietly in most tools; here
each becomes a note on the table.

```bash
uv run python examples/with_problems/run.py --html graph.html
```

`run.py` copies this folder to a temp dir (the checked-in files stay as
they are) and runs the pipeline twice:

1. **Run 1** loads `data/`, which is clean.
2. **Run 2** replaces the source files with `data_next/`, a second delivery
   that contains the problems below. `--html` writes run 2's final graph.

The exit code is 1 on purpose, see problem 4.

| Problem | Where | What pipetree reports |
| --- | --- | --- |
| Duplicate rows for one key | `bronze.customer`: account 2 arrives twice in one batch, as versions 2 and 3 | `WARNING` log line, `notes: 2 duplicate row(s) dropped` in the digest, a `⚠` badge on the table. The row with the highest `versionnumber` (the table's `sequence_by`) wins. |
| A NULL business key | `bronze.employee`: one row has no `employee_id` | `WARNING` log line, `notes: 1 NULL-key row(s) dropped`, a `⚠` badge. A row without a key cannot identify anything, so it is not loaded. |
| Schema drift | `bronze.orders`: the second delivery has a new `channel` column | `notes: schema: added: channel (string)`, a `⚠` badge. With the default `schema_policy: evolve` the column is added to the table. |
| A failing table | `gold.dim_customer` selects a column (`region`) that does not exist | The table is `failed` with the Spark error in the digest, and `gold.fact_sales`, which reads it, is `upstream_failed` and never starts. The other five tables finish. |

The numbers are also in the run log (`duplicates_dropped`,
`null_keys_dropped`, `schema_changes`). The picture in the top-level README
is this run's graph.
