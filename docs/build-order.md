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
- [ ] **Phase D — sources**: `SourceReader` registry, `sqlserver`,
      `storage_stream` (Structured Streaming with `Trigger.AvailableNow`,
      Auto Loader on Databricks), `kusto`, `custom` class-by-name loader.
- [ ] **Phase E — schema drift**: inference, diff, `evolve | fail | ignore`
      policies.
- [ ] **Phase F — declarative pipelines**: AUTO CDC translation for
      Databricks declarative pipelines.

Each phase is small commits, tests first. README and `NOTES-for-blog.md`
were written during Phase A rather than held for the end, since that phase
had to be publishable on its own; both get revisited as later phases land.
