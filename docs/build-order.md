# Build order

`pipetree` is built in phases so that part 2 of the blog series (the
executor: multithreading, retries, failure handling) is complete and
demoable on its own before the platform-specific work lands. Nothing built
in a later phase changes the public shape of an earlier one — later phases
add adapters and features behind seams the core already defines.

- [ ] **Phase A — core (part 2)**: config loader, typed model, graph
      builder, executor, Spark/Delta adapter, fault injection, example
      project and demo run.
- [ ] **Phase B — graph features**: table selection, `--with-dependents`
      subtree closure, tree rendering (text + Mermaid).
- [ ] **Phase C — platforms**: `Platform` seam (local / Databricks /
      Fabric), wheel packaging, a Databricks Asset Bundle, a Fabric
      notebook entrypoint.
- [ ] **Phase D — sources**: `SourceReader` registry, `sqlserver`,
      `storage_stream` (Structured Streaming with `Trigger.AvailableNow`,
      Auto Loader on Databricks), `kusto`, `custom` class-by-name loader.
- [ ] **Phase E — schema drift**: inference, diff, `evolve | fail | ignore`
      policies.
- [ ] **Phase F — declarative + docs**: AUTO CDC translation for Databricks
      declarative pipelines, README, `NOTES-for-blog.md`.

Each phase is small commits, tests first. See `NOTES-for-blog.md` (added in
Phase A) for the design decisions and trade-offs made along the way.
