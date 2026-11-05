---
title: Compared to
description: How pipetree differs from metadata-driven copy, dbt and AUTO CDC.
sidebar:
  order: 1
---

| Tool | What it does | Difference |
| --- | --- | --- |
| [Metadata-driven copy](https://learn.microsoft.com/en-us/azure/data-factory/copy-data-tool-metadata-driven) (Azure Data Factory, Fabric) | Control tables drive copy activities. | Copy-centric, it stops at ingestion. pipetree's YAML covers ingestion through modeling. |
| [dbt](https://www.getdbt.com/) | Config-driven SQL transformations. | Transformation only, it does not extract. pipetree has source readers and SCD merge strategies. |
| [AUTO CDC](https://docs.databricks.com/aws/en/ldp/cdc) | `KEYS`, `SEQUENCE BY`, `STORED AS SCD TYPE` in SQL. | Runs inside Lakeflow pipelines only. pipetree runs on any Spark with Delta and can [translate to AUTO CDC](../../guides/autocdc/). |

The design is described in the [Pipeline Engineering](https://datadave.dev/tags/pipeline-engineering/) blog series.
