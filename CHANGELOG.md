# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project uses [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-10-10

### Bug fixes

- Databricks/Fabric acquire_token and Kusto reader match the real APIs ([#2](https://github.com/phant0mw0lf/pipetree/pull/2))
- strip reserved audit columns from incoming sources ([#3](https://github.com/phant0mw0lf/pipetree/pull/3))
- scd1 soft delete - a returning key is active again, a repeated delete is a no-op ([#11](https://github.com/phant0mw0lf/pipetree/pull/11))
- delete_mode ignore - delete rows are removed before the duplicate resolution ([#12](https://github.com/phant0mw0lf/pipetree/pull/12))
- ignore_columns - a change in an ignored column alone updates the row in place ([#13](https://github.com/phant0mw0lf/pipetree/pull/13))
- NULL business keys - whole-NULL keys dropped on scd1/scd2, partially NULL composite keys match null-safely ([#14](https://github.com/phant0mw0lf/pipetree/pull/14))
- AUTO CDC translation renders FROM STREAM and terminated statements ([#16](https://github.com/phant0mw0lf/pipetree/pull/16))
- scd1 - the higher sequence_by wins across batches ([#17](https://github.com/phant0mw0lf/pipetree/pull/17))
- scd2 - versions follow sequence_by, a late row becomes an earlier version ([#18](https://github.com/phant0mw0lf/pipetree/pull/18))

### Build

- publish as pipetree-meta, add package metadata and install docs

### Features

- config loader and typed model
- graph builder - dependency parsing, topo sort, cycle detection
- executor - ready-queue scheduler, retries, failure handling
- run log collector, writer, and notifier
- Spark/Delta adapter - the four merge strategies
- SparkAdapter - the Adapter protocol wired to Spark/Delta
- fault-injecting adapter for the demo run
- top-level run_pipeline() API and Delta run log writer
- pipetree CLI (run, validate)
- real log output from the executor
- example project and a fault-injecting demo run
- table selection, subtree closure, init, tree rendering
- the Platform seam, Databricks Asset Bundle, Fabric notebook
- the SourceReader registry
- schema inference and drift policies
- AUTO CDC translation for declarative pipelines
- secretless auth (Platform.acquire_token, aad_token mode) ([#1](https://github.com/phant0mw0lf/pipetree/pull/1))
- type-appropriate unknown-member keys (NULL for string keys), null-safe ([#4](https://github.com/phant0mw0lf/pipetree/pull/4))
- surrogate_key - Delta identity surrogate keys for scd1/scd2 dimensions ([#5](https://github.com/phant0mw0lf/pipetree/pull/5))
- live dependency graph - progress events, HTML render, notebook live view ([#7](https://github.com/phant0mw0lf/pipetree/pull/7))
- --with-ancestors - run a table with everything it reads ([#8](https://github.com/phant0mw0lf/pipetree/pull/8))
- data-quality notes - dropped duplicates/NULL keys/schema drift visible in log, graph, digest, run log ([#9](https://github.com/phant0mw0lf/pipetree/pull/9))

### Tests

- merge-strategy conformance tests - reference model and generated histories ([#10](https://github.com/phant0mw0lf/pipetree/pull/10))

