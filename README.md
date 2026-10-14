<p align="center">
  <img src="https://raw.githubusercontent.com/phant0mw0lf/pipetree/main/docs/src/assets/logo.svg" alt="pipetree" width="120">
</p>

# pipetree

[![CI](https://github.com/phant0mw0lf/pipetree/actions/workflows/ci.yml/badge.svg)](https://github.com/phant0mw0lf/pipetree/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/phant0mw0lf/pipetree/graph/badge.svg)](https://codecov.io/gh/phant0mw0lf/pipetree)

Metadata-driven pipeline orchestration: describe *what* to move in declarative
YAML, and a small package turns it into a runnable dependency tree.

`YAML → config loader → typed model → graph builder → executor`

Documentation: **https://pipetree.dev**. The package is the companion to the
[Pipeline Engineering](https://datadave.dev/tags/pipeline-engineering/) blog series.

![pipetree's dependency graph after a run: seven tables in four waves, all succeeded](https://raw.githubusercontent.com/phant0mw0lf/pipetree/main/docs/images/graph-clean.png)

## What it does

- **One YAML block per table:** a source or a SQL/PySpark file, a business key and a merge
  strategy. `depends_on: auto` reads the parents from the code.
- **A dependency tree that runs:** a table starts the moment its parents have succeeded. Transient
  errors are retried, a failed table marks its dependents `upstream_failed` and the rest finishes.
- **Merge strategies on Delta:** `replace`, `append`, `scd1` and `scd2`, with `sequence_by`
  ordering across batches, soft or hard deletes, [snapshot merges](https://pipetree.dev/merge/snapshot/)
  for sources that deliver a full extract, unknown members and surrogate keys.
- **Notes for what a merge quietly changed:** dropped duplicates, NULL keys, schema drift, deleted
  keys. In the log, the digest, the graph and the run log.
- **A live picture of the run** in a notebook or as an HTML file.
- **Tags and partial runs:** `--select`, `--with-dependents`, `--with-ancestors`, `--tag`.
- **Databricks and Fabric:** the same config runs on local Spark, Databricks and Fabric; secrets
  from Azure Key Vault without a secret, a failure email through Azure Communication Services, and
  a translation of scd tables to Databricks' AUTO CDC.

## Installation

```bash
pip install pipetree-meta            # core: config loader, graph, executor, CLI
pip install "pipetree-meta[spark]"   # + PySpark and Delta Lake for local runs
pip install "pipetree-meta[azure]"   # + Key Vault secrets, token auth, failure email
```

Python 3.11 or newer. The import package and the command are `pipetree`. Local Spark needs a JDK
(for example `brew install openjdk@17`). On Databricks and Fabric install without `[spark]`, the
runtime brings its own Spark and Delta.

## Quick start

```yaml
# pipeline.yaml
systems:
  crm:
    type: csv
    path: data/customer.csv
bronze:
  tables:
    customer:
      source: { system: crm, object: account }
      business_key: [accountid]
      strategy: scd1
```

```bash
mkdir -p data && printf 'accountid,name\n1,Contoso\n2,Fabrikam\n' > data/customer.csv   # a tiny source file
pipetree validate --config pipeline.yaml   # check the config, nothing runs
pipetree graph --config pipeline.yaml      # print the dependency tree
pipetree run --config pipeline.yaml        # run it and print a digest
```

The [quickstart](https://pipetree.dev/getting-started/quickstart/) runs a seven-table example from
a clone of the repository. [`examples/quickstart.ipynb`](https://github.com/phant0mw0lf/pipetree/blob/main/examples/quickstart.ipynb)
is the same run as a notebook with its outputs, and
[`examples/with_problems/`](https://github.com/phant0mw0lf/pipetree/blob/main/examples/with_problems/README.md)
shows what pipetree reports when the data misbehaves.

## Documentation

- [Install](https://pipetree.dev/getting-started/install/) and [Quickstart](https://pipetree.dev/getting-started/quickstart/)
- [Concepts](https://pipetree.dev/concepts/yaml-structure/): YAML structure, the dependency tree, running, tags, notes and the run log, seeing the run
- [Merge strategies](https://pipetree.dev/merge/merge-options/): replace, append, scd1, scd2, snapshot merge, merge options
- [Guides](https://pipetree.dev/guides/databricks/): Databricks, Fabric, notifications, AUTO CDC, custom sources
- [Reference](https://pipetree.dev/reference/yaml/): every YAML key and every CLI option
- [Architecture](https://pipetree.dev/about/architecture/): the parts of the package

`examples/databricks/` (a Databricks Asset Bundle) and `examples/fabric/` (a notebook) run the
example pipeline against a real workspace.

## Development

```bash
uv sync --all-extras --dev
uv run pytest              # full suite
uv run pytest -m "not spark"  # skip the JVM-backed integration tests
uv run ruff check . && uv run ruff format --check .
uv run pyright
```

See [`CONTRIBUTING.md`](https://github.com/phant0mw0lf/pipetree/blob/main/CONTRIBUTING.md).

## License

MIT, see [`LICENSE`](https://github.com/phant0mw0lf/pipetree/blob/main/LICENSE).
