---
title: Fabric
description: Run a pipeline from a Microsoft Fabric notebook.
sidebar:
  order: 2
---

:::caution
Not verified against a real workspace yet. The platform code is unit-tested against a mocked `notebookutils`. Treat the example as a starting point.
:::

`examples/fabric/run_pipeline.ipynb` runs the example pipeline against a Fabric lakehouse. Steps:

1. Install pipetree as a custom library on a Fabric environment attached to the notebook, or with `%pip install` of an uploaded wheel.
2. Upload `examples/pipeline.yaml`, `examples/data/` and `examples/notebooks/` to the lakehouse `Files/` section, so the relative paths resolve.
3. Attach the lakehouse as the notebook's default and run the notebook.

## Check first

- `notebookutils` is assumed to be a global, with `notebookutils.credentials.getSecret(vault_url, secret_name)` for secrets.
- `FabricPlatform` resolves paths to `Files/<relative path>`, which needs the default lakehouse attached. The notebook's `CONFIG_PATH` uses the mounted form `/lakehouse/default/Files/...`. They are different strings.
- Token auth (`auth.mode: aad_token`) runs as the notebook's executing identity and supports a limited set of audiences (`storage`, `pbi`, `keyvault`, `kusto`, Kusto cluster URIs).
- `surrogate_key` needs Delta identity columns, which Fabric Runtime 1.3 lacks.

To follow a run in the notebook, pass an observer: `run_pipeline("pipeline.yaml", observer=LiveGraphView())` from `pipetree.notebook` redraws the graph in place.
