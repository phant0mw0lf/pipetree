# pipetree documentation

The site is [Astro Starlight](https://starlight.astro.build/). Pages are Markdown/MDX in `src/content/docs/`. This README and `build-order.md` are not published.

## Work on it

```bash
cd docs
npm ci
npm run dev        # http://localhost:4321
npm run build      # builds dist/ and checks every internal link
npm run preview    # serves dist/
```

Node 24 or newer.

## The reference is generated

`src/content/docs/reference/yaml.mdx`, `reference/cli.mdx` and `public/pipetree.schema.json` come from the code (`src/pipetree/model.py`, `src/pipetree/cli.py`). Do not edit them. After changing a model field or a CLI option:

```bash
uv run python docs/scripts/gen_reference.py
```

Field descriptions are the `description=` of the pydantic fields. `tests/test_docs_reference.py` fails when the committed files differ from the generated ones.

## Examples are checked

A fenced block marked `yaml validate` is a complete config and must pass the config loader (`tests/test_docs_examples.py`). Use plain `yaml` for fragments.

## Hosting: Cloudflare (Workers with static assets)

Cloudflare builds and deploys the site from the GitHub repository, and builds a preview for every pull request from this repository. `.github/workflows/docs.yml` (`docs-build`) runs the same build and the link check for any pull request that touches `docs/`, including pull requests from forks, which Cloudflare does not build. `docs/wrangler.jsonc` tells Cloudflare to serve `dist/`.

Settings when creating the project (Workers & Pages, Create, Import a repository):

| Setting | Value |
| --- | --- |
| Repository | `phant0mw0lf/pipetree` |
| Project name | `pipetree` (must equal `name` in `wrangler.jsonc`) |
| Production branch | `docs-live` (see "Versions"; `main` then builds as a preview) |
| Root directory | `docs` |
| Build command | `npm ci && npm run build` |
| Deploy command | `npx wrangler deploy` |
| Non-production branch deploy command | `npx wrangler preview` |
| Node version | from `.nvmrc` (24); optional variable `NODE_VERSION` = `24` |
| Custom domain | `pipetree.dev` (Settings, Domains & Routes, Add, Custom domain) |

`SITE_URL` already defaults to `https://pipetree.dev` and `BASE_PATH` to `/`, so no other variable is needed. The `*.workers.dev` address keeps working as the fallback. A redirect from `www.pipetree.dev` is not possible in `_redirects` (it cannot redirect across hosts): add it as a Redirect Rule in Cloudflare if wanted.

The Cloudflare build runs Node only, with no Python. That works because the generated reference files and `public/pipetree.schema.json` are committed.

`public/_headers` sets the security headers. The Content-Security-Policy allows inline scripts and styles (Starlight needs them) and `wasm-unsafe-eval` (search).

## Versions

`pipetree.dev` documents the newest release at the regular URLs, so every link without a version shows the latest docs. The other versions sit in folders, picked with the version select ([starlight-versions](https://starlight-versions.vercel.app/)):

| URL | Content | Select label |
| --- | --- | --- |
| `pipetree.dev/...` | the newest release | `vX.Y.Z (latest)` |
| `pipetree.dev/next/...` | `main`, unreleased; not indexed by search engines | `next (unreleased)` |
| `pipetree.dev/0.1/...` | an archived minor line, one per line | `v0.1` |

A minor version is where pipetree breaks configs (0.x), so one entry per minor line is enough. Each version also has its schema, for example `pipetree.dev/0.1/pipetree.schema.json`.

**Write docs in the feature branch.** The pages at the root of `src/content/docs/` describe `main`, so a feature pull request changes them along with the code. Its Cloudflare preview shows them at the root with a banner saying they are unreleased. After the merge they appear under `/next/`. Do not edit the archive folders, except to fix an error in an old version.

**Archives are made in the release pull request.** After the version bump and `gen_reference.py`:

```bash
uv run python docs/scripts/archive_docs.py
```

It snapshots the root pages as the archive of the version's minor line (0.2 for 0.2.3) and replaces an older archive of the same line, so a patch release refreshes it. `tests/test_docs_versions.py` fails when the newest archive does not match the version in the code.

**The production site is assembled.** The plugin always puts the pages of the build itself at the root, so the site Cloudflare builds as production is put together first, on the branch `docs-live`, by `docs/scripts/build_release_site.py`:

- the root pages, their images and `/pipetree.schema.json` come from the newest release tag (`vX.Y.Z`),
- `next/` is a snapshot of the root pages of `main`,
- everything else (config, components, archives) comes from `main`, without the archive of the release's own line, which is at the root.

`.github/workflows/docs-live.yml` runs the script and commits the result to `docs-live`. It runs after every docs change on `main`, after every release (the `Release` workflow calls it), and by hand. Only that workflow writes `docs-live`. Its commits hold the assembled `docs/` directory and nothing else; leaving `.github/` out keeps the push within what the workflow token may do. A fix to the docs of the latest release therefore goes live with the next release, and at once under `/next/`.

The `docs-build` check builds both: the pull request as it is, and the release site assembled from it. To assemble and build the release site locally (needs the tags):

```bash
python3 docs/scripts/build_release_site.py /tmp/site
cd /tmp/site/docs && npm ci && DOCS_CHANNEL=release npm run build
```

## Another domain or a sub-path

`astro.config.mjs` reads `SITE_URL` and `BASE_PATH`. Set them as build environment variables in Cloudflare, and update the schema URL in `src/content/docs/getting-started/quickstart.md`. Internal links are relative, so they survive any base path. To check a sub-path build: `BASE_PATH=/pipetree npm run build`.
