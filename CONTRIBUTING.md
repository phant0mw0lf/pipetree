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

CI runs the same checks on every pull request (see [CI](#ci)).

Workflow files are audited with [zizmor](https://docs.zizmor.sh/) (the `zizmor`
check). To run it locally:

```bash
GITHUB_TOKEN=$(gh auth token) uvx zizmor .github
```

Fix findings where possible; if one is intentional, add an inline
`# zizmor: ignore[rule]` with a comment explaining why.

## CI

Pull requests run these jobs (`.github/workflows/ci.yml`):

- `lint`: `ruff check`, `ruff format --check` and `pyright`. It is quick, so
  formatting and type errors show up within a few minutes.
- `tests`: the full test suite with coverage, run in parallel with
  `pytest-xdist` (`-n 4`; every worker has its own local Spark session). It
  runs the generated conformance cases with 25 seeds per strategy when the pull
  request touches the merge machinery (`src/pipetree/adapters/`,
  `src/pipetree/testing/`, `tests/merge_conformance/`, `pyproject.toml`,
  `uv.lock`, `tests/conftest.py`, `ci.yml`) and with 5 seeds otherwise; the
  fixed regression corpus and the hand-built cases always run. It is skipped when a pull request only changes files the
  tests do not read (markdown, images, notebooks, `docs/` apart from the pages, scripts and the generated schema, other workflows, ...).
  A push to `main` always runs it. Coverage goes to Codecov. It starts after
  `lint` and not at all if `lint` failed, and stops after 5 failed tests
  (`--maxfail=5`), so a broken change fails fast. The Spark tests must run in
  CI: `PIPETREE_REQUIRE_SPARK=1` makes a missing Spark session fail instead of
  skip (set it locally to check the same).
- `test`: a final job that passes when `lint` and `tests` passed (or `tests`
  was skipped because no code changed). It always runs, so the required check
  has a result on every pull request.

The required status checks are `test`, `pr-title` and `zizmor`. Codecov adds
`codecov/patch` (new lines should be at least 80% covered) and
`codecov/project` (total coverage must not drop by more than 1%).

The full merge-conformance run (300 seeds per strategy, all four strategies)
is too slow for every pull request. The `Conformance` workflow
(`.github/workflows/conformance.yml`) runs it every night and can be started
by hand from the Actions tab with the `seeds` input, or called from another
workflow, for example before a release. To run it locally:

```bash
PIPETREE_CONFORMANCE_SEEDS=300 uv run pytest tests/merge_conformance -n 4
```

## Documentation

The documentation site lives in `docs/` (Astro Starlight, see `docs/README.md`). The YAML and CLI
reference pages and `docs/public/pipetree.schema.json` are generated from the code: after changing
`src/pipetree/model.py` or `src/pipetree/cli.py`, run
`uv run python docs/scripts/gen_reference.py` and commit the result. A test fails when they are out
of date. Complete YAML examples in the pages are marked `yaml validate` and are checked by a test.

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

## Labels

Issues and pull requests use these label groups:

- **Type** (blue): `bug`, `enhancement`, `documentation`, `question`, plus
  `good first issue`, `help wanted`, `wontfix` and `duplicate`.
- **Area** (grey, `area: ...`): the part of the code a change touches, such as
  `area: merge` or `area: executor`.
- **Platform** (green, `platform: ...`): `databricks`, `fabric` or `local`.
- **Release impact** (red): `breaking change` and `needs docs`.
- **Triage** (yellow): `needs triage`, `needs reproduction` and `blocked`.
- **Infrastructure**: `dependencies`, `ci` and `release`.

You do not need to set labels on a pull request. Area and platform labels are
added from the files you change, and the type label (and `breaking change`) from
the PR title. New issues get `needs triage` until a maintainer has looked at them.

The list of labels lives in `.github/labels.yml`, which is authoritative: when
a change to it is merged to `main`, the repository labels are synced to match,
and labels that are not in the file are deleted.

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
3. Open a PR titled `chore(release): ...` (git-cliff leaves such commits out of the
   changelog) and merge it once checks pass. Merge it last: whatever lands on `main`
   afterwards is missing from `CHANGELOG.md`, while the release notes (generated from
   the history at the tag) would contain it.
4. Tag the merge commit and push the tag. Signing is optional, a signed tag shows as
   "Verified" on GitHub:

   ```bash
   git switch main && git pull
   git tag -s vX.Y.Z -m "pipetree X.Y.Z"    # or: git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

5. The `Release` workflow builds the distributions, publishes to TestPyPI, then
   (after approval of the `pypi` environment) to PyPI, and finally creates the
   GitHub Release with the notes from `uvx git-cliff --latest --strip header`.
   The workflow refuses to run if the tag, `pyproject.toml` and `__version__`
   disagree. A manual run of the workflow (Actions tab, `Release`, "Run
   workflow") publishes to TestPyPI only; TestPyPI takes a file name once, so a
   second run with the same version is skipped.

   Every entry of the notes and of `CHANGELOG.md` carries its pull request and its
   author ("by @user", from the GitHub API: the release job gives git-cliff the
   token).

**Pre-releases.** A tag with `a`, `b` or `rc` after the number (`v0.2.0rc1`, the
version in `pyproject.toml` and `__version__` must be the same PEP 440 string
`0.2.0rc1`) is published as a pre-release on PyPI and on GitHub (never "Latest"). Its
notes cover the changes since the tag before it, the notes of the final version those
since the previous final version.

**Immutable releases** are enabled for the repository: once a GitHub Release is
published, its assets and its tag can no longer be changed, and the release cannot be
deleted. Its title and notes stay editable (`gh release edit vX.Y.Z --notes-file ...`).
PyPI files are immutable anyway. A mistake in the files is fixed with a new version
(`X.Y.Z+1`).

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
