# Build order

`pipetree` is built in phases so that part 2 of the blog series (the
executor: multithreading, retries, failure handling) is complete and
demoable on its own before the platform-specific work lands. Nothing built
in a later phase changes the public shape of an earlier one — later phases
add adapters and features behind seams the core already defines.

- [x] **Phase A — core (part 2)**: config loader, typed model, graph
      builder, executor, Spark/Delta adapter, fault injection, example
      project and demo run.
- [x] **Phase B — graph features**: table selection (`--select`), the
      `--with-dependents` subtree closure, `--init` (full reload), tree
      rendering (text + Mermaid) via `pipetree graph`.
- [x] **Phase C — platforms**: `Platform` seam (local / Databricks /
      Fabric) built and unit-tested against mocked `dbutils`/
      `notebookutils`; wheel packaging verified; a Databricks Asset Bundle
      and a Fabric notebook entrypoint. **Not yet verified against a real
      workspace** - see `examples/databricks/README.md` and
      `examples/fabric/README.md` for exactly what to check. That
      verification round (deploy both, report back what breaks) is next,
      and doesn't block Phase D.
- [x] **Phase D — sources**: `SourceReader` registry (csv/json/parquet,
      `sqlserver`, `kusto`, `storage_stream` + its `d365_export`/
      `synapse_link` aliases, `custom` dotted-path loading). `SparkAdapter`
      now dispatches through it instead of hardcoding local-file reads.
      `storage_stream`'s non-Databricks path (`Trigger.AvailableNow`,
      checkpointing, `--init` resetting it) is verified for real, locally;
      its Auto Loader branch, and `sqlserver`/`kusto`'s option-building,
      are tested against hand-written fakes - there's no real SQL Server,
      Kusto cluster, or Databricks storage account here to connect to.
- [x] **Phase E — schema drift**: inference (`pipetree.schema.infer`),
      diff (added/removed/retyped/nullability), and the three policies
      (`evolve` - safe numeric widenings + `withSchemaEvolution()` for a
      genuinely new column, never dropping a removed one; `fail` - any
      drift raises before anything is written; `ignore` - the intersection
      only) wired into `merge_scd1`/`merge_scd2`. Every non-empty diff is
      recorded in the run log's `schema_changes` column. Verified for real
      locally (real Spark, real Delta schema evolution) - not something
      that needed a mock.
- [x] **Phase F — declarative pipelines**: `pipetree.declarative.autocdc`
      translates an scd1/scd2 `Table` into a Databricks AUTO CDC flow -
      `KEYS`, `SEQUENCE BY`, `APPLY AS DELETE WHEN`, `STORED AS SCD TYPE`,
      `TRACK HISTORY ON * EXCEPT` - exactly the comparison part 1 draws.
      A pure translation (`Table` → `AutoCdcFlow` → SQL text), since AUTO
      CDC only runs inside a Lakeflow Declarative Pipeline and pipetree
      has nothing to execute there, only something to compile to.
      `replace`/`append` are rejected - they need no CDC apparatus, just a
      plain streaming table or materialized view. The exact current
      Python API (`dlt.create_auto_cdc_flow`, previously
      `dlt.apply_changes`) isn't verified; the rendered SQL is checked
      against part 1's own worked example instead, the more stable target.

This closes the build order from the original plan. What's still open:
the **Databricks/Fabric verification round** from Phase C (deploy
`examples/databricks/` and `examples/fabric/` for real, report back what
breaks) and the two `NOTES-for-blog.md` gaps flagged along the way (a
production-scale approach for scd2's driver-side "changed rows"
materialization; `append` schema-policy enforcement). Token-based
(`aad_token`) auth was added to `sqlserver`/`kusto` and the platform seam
for the pipetree-scale-bench project's secretless-connections requirement -
real-workspace verification of `acquire_token` (the UC service credential
call, the Fabric workspace-identity token call) is part of that project's
own rollout, not this repo's test suite.

Each phase is small commits, tests first. README and `NOTES-for-blog.md`
were written during Phase A rather than held for the end, since that phase
had to be publishable on its own; both get revisited as later phases land.
