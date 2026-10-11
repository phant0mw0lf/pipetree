---
title: Custom sources
description: Read from anything with your own class.
sidebar:
  order: 3
---

```yaml validate
systems:
  graph_api:
    type: custom
    class: sources.graph.SharePointListSource

bronze:
  tables:
    projects:
      source: { system: graph_api, object: Projects }
      business_key: [id]
      strategy: scd1
```

```python
# sources/graph.py
from pyspark.sql import DataFrame
from pipetree.sources.base import SourceContext


class SharePointListSource:
    def read(self, ctx: SourceContext) -> DataFrame:
        list_name = ctx.table.source.object  # "Projects"
        base_url = ctx.system.base_url  # extra keys of the system
        return ctx.spark.createDataFrame(...)
```

`class` is a dotted path to a class. pipetree imports it and calls it **without arguments**, then calls `read(ctx)` for each table with this system. The class must have a `read(ctx)` method that returns a Spark DataFrame, or loading fails with a `TypeError`. A missing module or class gives an `ImportError`. The module must be importable, so on a cluster install it as a library.

`ctx` is a `SourceContext`:

| Field | Content |
| --- | --- |
| `spark` | the Spark session |
| `table`, `system` | the table and system config. Other keys of the system are attributes of `system`. |
| `platform` | resolves secrets and paths: `resolve_value(value, ctx.platform)` handles `{ secret: name }` |
| `base_dir` | the directory of the config file |
| `init` | `True` on a `--init` run |

Built-in types, for comparison: `csv`, `json`, `parquet`, `sqlserver`, `kusto`, `storage_stream` (and its aliases `d365_export`, `synapse_link`).
