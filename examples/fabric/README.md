# pipetree on Microsoft Fabric

`run_pipeline.ipynb` runs the same example pipeline as
`examples/run_demo.py`, against a Fabric lakehouse instead of a laptop.

## Setup

1. Upload `examples/pipeline.yaml`, `examples/data/`, and
   `examples/notebooks/` to the lakehouse `Files/` section, so the config's
   relative paths resolve.
2. Install `pipetree`, either as a custom library on a Fabric **environment**
   attached to the notebook (recommended - persists across sessions), or via
   the commented-out `%pip install` cell pointing at an uploaded `.whl`.
3. Attach the lakehouse as the notebook's default and run the notebook.

## Notes

- **`notebookutils`** is a global in a Fabric notebook. Secrets
  (`{secret: name}`) are read with
  `notebookutils.credentials.getSecret(vault_url, secret_name)`. The example
  pipeline needs no secret (its one source is a local CSV).
- **The `Files/` mount path.** `resolve_path()` on `FabricPlatform` returns
  `Files/<relative_path>` for the default lakehouse attached to the notebook.
  `CONFIG_PATH` in the notebook uses the fully-mounted form
  (`/lakehouse/default/Files/...`), which is how Python file APIs reference
  the default lakehouse. The two strings differ, so don't mix them up.
