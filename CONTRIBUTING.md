# Contributing to pipetree

Thanks for your interest. Bug reports, fixes, and ideas are welcome. Please
follow the [Code of Conduct](CODE_OF_CONDUCT.md). Security problems go through
[private reporting](SECURITY.md), not public issues.

## Development setup

You need Python 3.11+, [`uv`](https://docs.astral.sh/uv/), and a JDK 17 for the
PySpark tests (for example `brew install openjdk@17`).

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
uv sync --extra spark --dev
uv run pre-commit install        # ruff on commit, Conventional Commit check on commit messages
```

Tests that need Spark are skipped when no JVM is available.

## Checks

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

The generated merge-conformance tests run against many random histories. The
number of seeds per case is set with `PIPETREE_CONFORMANCE_SEEDS` (default 25,
`0` turns them off):

```bash
PIPETREE_CONFORMANCE_SEEDS=0 uv run pytest     # fast run
PIPETREE_CONFORMANCE_SEEDS=100 uv run pytest   # more thorough
```

CI runs the same checks on every pull request.

## Pull requests

- Keep a PR focused on one change and add or update tests for behaviour changes.
- Changes to Databricks or Fabric behaviour that you have tested on the real
  platform are very welcome; say so in the PR. For outside contributors this is
  optional.
- The repository uses **squash merges**: the PR title and description become
  the commit message on `main`, and the changelog is generated from them. The
  PR title must therefore be a [Conventional Commit](https://www.conventionalcommits.org/):

  ```
  type(scope): short summary
  ```

  Allowed types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`,
  `ci`, `chore`, `revert`. The scope is optional. Append `!` to the type for a
  breaking change (`feat!: ...`). The `pr-title` check enforces this.

## Releases (maintainers)

Versions follow [Semantic Versioning](https://semver.org/). While the project
is 0.x, a breaking change bumps the minor version and a feature bumps the
patch version. The changelog is generated with
[git-cliff](https://git-cliff.org/) (`cliff.toml`) and then cleaned up by hand.

1. Branch from an up-to-date `main`. Preview the next version and notes:

   ```bash
   uvx git-cliff --bump --unreleased
   ```

2. Update the changelog and bump the version in `pyproject.toml` and in
   `src/pipetree/__init__.py` (`__version__`) to the version git-cliff proposes:

   ```bash
   uvx git-cliff --bump -o CHANGELOG.md
   ```

   Edit `CHANGELOG.md` by hand where a commit message reads badly for users.
3. Open a PR titled `chore(release): vX.Y.Z` and merge it once checks pass.
4. Tag the merge commit and push the tag:

   ```bash
   git switch main && git pull
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

5. The `Release` workflow builds the distributions, publishes to TestPyPI, then
   (after approval of the `pypi` environment) to PyPI, and finally creates the
   GitHub Release with the notes from `uvx git-cliff --latest --strip header`.
   The workflow refuses to run if the tag, `pyproject.toml` and `__version__`
   disagree. A manual run of the workflow (Actions tab, `Release`, "Run
   workflow") publishes to TestPyPI only.

### One-time setup checklist

- PyPI and TestPyPI: add a trusted publisher to the `pipetree-meta` project
  (for the first release, register it as a pending publisher): owner
  `phant0mw0lf`, repository `pipetree`, workflow `release.yml`, environment
  `pypi` on PyPI and `testpypi` on TestPyPI.
- GitHub environments `testpypi` and `pypi` (Settings, Environments), with
  required reviewers.
- Branch ruleset for `main`: pull request required, required status checks
  `test` and `pr-title`, no force pushes, no deletions.
- Settings, Actions, General: require approval for first-time contributors'
  workflow runs.
- Settings, Code security: enable private vulnerability reporting, secret
  scanning, and push protection.
