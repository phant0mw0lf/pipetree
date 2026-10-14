# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project uses [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-10-14

### Bug fixes

- Databricks/Fabric acquire_token and Kusto reader match the real APIs ([#2](https://github.com/phant0mw0lf/pipetree/pull/2)) by @phant0mw0lf
- strip reserved audit columns from incoming sources ([#3](https://github.com/phant0mw0lf/pipetree/pull/3)) by @phant0mw0lf
- scd1 soft delete - a returning key is active again, a repeated delete is a no-op ([#11](https://github.com/phant0mw0lf/pipetree/pull/11)) by @phant0mw0lf
- delete_mode ignore - delete rows are removed before the duplicate resolution ([#12](https://github.com/phant0mw0lf/pipetree/pull/12)) by @phant0mw0lf
- ignore_columns - a change in an ignored column alone updates the row in place ([#13](https://github.com/phant0mw0lf/pipetree/pull/13)) by @phant0mw0lf
- NULL business keys - whole-NULL keys dropped on scd1/scd2, partially NULL composite keys match null-safely ([#14](https://github.com/phant0mw0lf/pipetree/pull/14)) by @phant0mw0lf
- AUTO CDC translation renders FROM STREAM and terminated statements ([#16](https://github.com/phant0mw0lf/pipetree/pull/16)) by @phant0mw0lf
- scd1 - the higher sequence_by wins across batches ([#17](https://github.com/phant0mw0lf/pipetree/pull/17)) by @phant0mw0lf
- scd2 - versions follow sequence_by, a late row becomes an earlier version ([#18](https://github.com/phant0mw0lf/pipetree/pull/18)) by @phant0mw0lf
- **examples:** the Databricks example runs as written and shows the live graph ([#31](https://github.com/phant0mw0lf/pipetree/pull/31)) by @phant0mw0lf
- **config:** reject scd2 with delete_mode hard in the model, not only in the loader ([#37](https://github.com/phant0mw0lf/pipetree/pull/37)) by @phant0mw0lf
- **merge:** a NULL delete_when result is not a delete, the row is merged ([#34](https://github.com/phant0mw0lf/pipetree/pull/34)) by @phant0mw0lf
- **merge:** NaN in a sequence column no longer crashes scd2 ([#36](https://github.com/phant0mw0lf/pipetree/pull/36)) by @phant0mw0lf
- **merge:** scd2 picks the latest version by sequence and run, not by the wall clock ([#40](https://github.com/phant0mw0lf/pipetree/pull/40)) by @phant0mw0lf
- **merge:** scd1 soft delete keeps its sequence ([#38](https://github.com/phant0mw0lf/pipetree/pull/38)) by @phant0mw0lf
- **merge:** a delete wins a sequence tie within a batch ([#39](https://github.com/phant0mw0lf/pipetree/pull/39)) by @phant0mw0lf
- **docs:** the CLI reference is the same text on every Python version ([#50](https://github.com/phant0mw0lf/pipetree/pull/50)) by @phant0mw0lf

### Build

- release setup - pipetree-meta 0.1.0, git-cliff changelog, release and PR-title workflows, community files ([#22](https://github.com/phant0mw0lf/pipetree/pull/22)) by @phant0mw0lf
- leave the commits from before the first pull request out of the changelog ([#54](https://github.com/phant0mw0lf/pipetree/pull/54)) by @phant0mw0lf

### CI

- sync labels from labels.yml and label PRs by path and title ([#23](https://github.com/phant0mw0lf/pipetree/pull/23)) by @phant0mw0lf
- faster pull request checks - parallel tests, skipped heavy tests on non-code PRs, nightly conformance, Codecov ([#25](https://github.com/phant0mw0lf/pipetree/pull/25)) by @phant0mw0lf
- show project coverage in the Codecov status and pull request comment ([#28](https://github.com/phant0mw0lf/pipetree/pull/28)) by @phant0mw0lf
- the pull request conformance run uses seeds that reach every coverage case ([#43](https://github.com/phant0mw0lf/pipetree/pull/43)) by @phant0mw0lf
- set codecov/patch on pull requests without code changes ([#47](https://github.com/phant0mw0lf/pipetree/pull/47)) by @phant0mw0lf
- run the test suite on Python 3.11, 3.12 and 3.13 ([#52](https://github.com/phant0mw0lf/pipetree/pull/52)) by @phant0mw0lf
- release notes compare with the previous final release, pre-releases are marked as such ([#58](https://github.com/phant0mw0lf/pipetree/pull/58)) by @phant0mw0lf

### Documentation

- README graph pictures, examples/with_problems and an executed quickstart notebook ([#21](https://github.com/phant0mw0lf/pipetree/pull/21)) by @phant0mw0lf
- documentation site with Astro Starlight, generated YAML and CLI reference ([#24](https://github.com/phant0mw0lf/pipetree/pull/24)) by @phant0mw0lf
- describe the Databricks example as a notebook task ([#33](https://github.com/phant0mw0lf/pipetree/pull/33)) by @phant0mw0lf
- a column added by schema evolution is NULL in unchanged rows ([#35](https://github.com/phant0mw0lf/pipetree/pull/35)) by @phant0mw0lf
- install without spark and azure by default, Databricks and Fabric install without spark ([#45](https://github.com/phant0mw0lf/pipetree/pull/45)) by @phant0mw0lf
- surrogate_key needs Fabric Runtime 2.0, Fabric as a whole is not unsupported ([#51](https://github.com/phant0mw0lf/pipetree/pull/51)) by @phant0mw0lf
- snapshot merge on the start page and in the README, wording and counts fixed ([#53](https://github.com/phant0mw0lf/pipetree/pull/53)) by @phant0mw0lf
- GitHub social preview image (1280x640) ([#57](https://github.com/phant0mw0lf/pipetree/pull/57)) by @phant0mw0lf
- README images and example links with absolute URLs so PyPI renders them ([#59](https://github.com/phant0mw0lf/pipetree/pull/59)) by @phant0mw0lf
- a short README, the long sections move to the documentation site ([#60](https://github.com/phant0mw0lf/pipetree/pull/60)) by @phant0mw0lf
- the README quick start creates its source file ([#62](https://github.com/phant0mw0lf/pipetree/pull/62)) by @phant0mw0lf

### Features

- secretless auth (Platform.acquire_token, aad_token mode) ([#1](https://github.com/phant0mw0lf/pipetree/pull/1)) by @phant0mw0lf
- type-appropriate unknown-member keys (NULL for string keys), null-safe ([#4](https://github.com/phant0mw0lf/pipetree/pull/4)) by @phant0mw0lf
- surrogate_key - Delta identity surrogate keys for scd1/scd2 dimensions ([#5](https://github.com/phant0mw0lf/pipetree/pull/5)) by @phant0mw0lf
- live dependency graph - progress events, HTML render, notebook live view ([#7](https://github.com/phant0mw0lf/pipetree/pull/7)) by @phant0mw0lf
- --with-ancestors - run a table with everything it reads ([#8](https://github.com/phant0mw0lf/pipetree/pull/8)) by @phant0mw0lf
- data-quality notes - dropped duplicates/NULL keys/schema drift visible in log, graph, digest, run log ([#9](https://github.com/phant0mw0lf/pipetree/pull/9)) by @phant0mw0lf
- **databricks:** read secrets from Key Vault through a Unity Catalog service credential ([#26](https://github.com/phant0mw0lf/pipetree/pull/26)) by @phant0mw0lf
- **graph:** depends_on auto parses SQL with sqlglot and PySpark with ast, unresolvable names are an error ([#29](https://github.com/phant0mw0lf/pipetree/pull/29)) by @phant0mw0lf
- table tags - tags in the YAML, --tag and --exclude-tag, input-not-refreshed note ([#30](https://github.com/phant0mw0lf/pipetree/pull/30)) by @phant0mw0lf
- **notify:** email a failed run through Azure Communication Services, without a secret ([#32](https://github.com/phant0mw0lf/pipetree/pull/32)) by @phant0mw0lf
- **merge:** snapshot merge for scd1 and scd2, keys missing from a full extract are deletes ([#49](https://github.com/phant0mw0lf/pipetree/pull/49)) by @phant0mw0lf
- **config:** reject unknown keys and a missing business_key, AUTO CDC refuses snapshot ([#55](https://github.com/phant0mw0lf/pipetree/pull/55)) by @phant0mw0lf

### Miscellaneous

- drop a stray attribution from a test comment ([#20](https://github.com/phant0mw0lf/pipetree/pull/20)) by @phant0mw0lf

### Tests

- merge-strategy conformance tests - reference model, generated histories, attributed expected failures ([#10](https://github.com/phant0mw0lf/pipetree/pull/10)) by @phant0mw0lf
- **merge:** the conformance generator reaches more sequence types and options ([#41](https://github.com/phant0mw0lf/pipetree/pull/41)) by @phant0mw0lf
- **merge:** the conformance harness compares every audit column ([#42](https://github.com/phant0mw0lf/pipetree/pull/42)) by @phant0mw0lf
- **merge:** the conformance generator produces NaN sequence values ([#48](https://github.com/phant0mw0lf/pipetree/pull/48)) by @phant0mw0lf

