# pipetree on Microsoft Fabric

`run_pipeline.ipynb` runs the same example pipeline as
`examples/run_demo.py`, against a Fabric lakehouse instead of a laptop.

**Status: not verified against a real workspace yet.** Built and
unit-tested against a mocked `notebookutils`
(`tests/platform/test_fabric.py`), but nothing here has actually run in
Fabric. Treat it as a first draft to import and fix, not a finished thing.

## What to check first

- **`notebookutils` availability and API shape.** The notebook assumes
  `notebookutils` is available as a global (Fabric's documented behaviour)
  and that `notebookutils.credentials.getSecret(vault_url, secret_name)`
  is the right call - confirmed from Microsoft's docs, not from a real
  run. The example pipeline doesn't actually need a secret (its one
  source is a local CSV), so this only matters once a real system config
  uses one.
- **The `Files/` mount path.** `resolve_path()` on `FabricPlatform`
  returns `Files/<relative_path>`, assuming the pipeline's default
  lakehouse is already attached to the notebook. `CONFIG_PATH` in the
  notebook uses the fully-mounted form (`/lakehouse/default/Files/...`)
  instead, which is the documented way to reference the default lakehouse
  from Python file APIs - the two conventions look similar but aren't the
  same string, so don't mix them up if you touch this.
- **Uploading the example project.** `examples/pipeline.yaml`,
  `examples/data/`, and `examples/notebooks/` need to land under this
  lakehouse's `Files/` section before the config's relative paths resolve.
- **Installing `pipetree`.** Either as a custom library on a Fabric
  **environment** attached to this notebook (recommended - persists
  across sessions), or via the commented-out `%pip install` cell pointing
  at an uploaded `.whl`.

## Comparing against the local run and Databricks

Same pipeline, no fault injection (that's the local demo's job). Part 4's
interesting comparison is Fabric's lakehouse + OneLake model against
Unity Catalog, and Spark vs. Fabric's Declarative Pipelines - notes on
that belong in part 4, once this has actually run.
