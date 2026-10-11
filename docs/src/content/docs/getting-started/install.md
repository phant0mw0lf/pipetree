---
title: Install
description: Install pipetree with pip or uv.
sidebar:
  order: 1
---

```bash
pip install pipetree-meta            # config loader, graph, executor, CLI
pip install "pipetree-meta[spark]"   # + PySpark and Delta Lake for local runs
pip install "pipetree-meta[azure]"   # + azure-identity for token-based auth
```

With uv: `uv add "pipetree-meta[spark]"`.

The package is `pipetree-meta`. The import and the command are `pipetree`.

| Extra | Adds | Needed for |
| --- | --- | --- |
| none | pydantic, pyyaml, click | `validate`, `graph`, loading configs |
| `spark` | PySpark 4.0, delta-spark 4.0 | `run` on a laptop |
| `azure` | azure-identity | `auth.mode: aad_token` on a laptop |

Python 3.11 or newer. Local Spark needs a JDK, for example JDK 17:

```bash
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
```

On Databricks and Fabric Spark is already there. See the [Databricks](../../guides/databricks/) and [Fabric](../../guides/fabric/) guides.
