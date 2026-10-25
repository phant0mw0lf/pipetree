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

The site has a version select. Each entry is one minor line, because a minor version is where pipetree breaks configs (0.x).

| Where | What the root pages show | Select label |
| --- | --- | --- |
| `pipetree.dev` (branch `docs-live`) | the newest release | `vX.Y.Z (latest)` |
| `main`, pull request previews, `npm run dev` | the next, unreleased version, with a banner saying so | `next (unreleased)` |

Every other entry is an archived line, a folder such as `src/content/docs/0.1/`. The plugin is [starlight-versions](https://starlight-versions.vercel.app/). `astro.config.mjs` tells the two kinds of build apart by the branch Cloudflare builds (`WORKERS_CI_BRANCH`). It reads the released version from the generated CLI reference.

**Write docs in the feature branch.** Pages at the root describe `main`, so a feature pull request changes them along with the code. The Cloudflare preview of the pull request shows them as "next". Do not edit the archive folders, except to fix an error in an old version.

**Archives are made in the release pull request.** After the version bump and `gen_reference.py`:

```bash
uv run python docs/scripts/archive_docs.py
```

It snapshots the root pages as the archive of the version's minor line (0.2 for 0.2.3) and replaces an older archive of the same line. A patch release therefore refreshes it. It also copies the JSON Schema to `public/<line>/`, so configs written for that line keep a schema that matches. `tests/test_docs_versions.py` fails when the newest archive does not match the version in the code.

**Production follows the release.** Cloudflare builds `pipetree.dev` from the branch `docs-live`, which only `.github/workflows/docs-live.yml` writes. Each of its commits holds the `docs/` directory of one ref and nothing else. It leaves out the archive of the ref's own minor line, which the site shows at the root. The `.github/` directory is left out deliberately, because the workflow token may not push commits that change workflow files.

- **After a release** the `Release` workflow calls `docs-live.yml` with the tag. It moves the branch only for the newest final version. A pre-release such as `v0.2.0rc1`, or a patch for an older line such as `v0.1.2` after `v0.2.0`, leaves the site where it is.
- **A docs fix without a release** goes to `main` as usual. To publish it at once, run `docs-live` by hand with `main` or a commit as `ref`. That also publishes every other docs change on `main` since the release, so check the diff first:

  ```bash
  git diff vX.Y.Z main -- docs/src/content/docs
  ```

The `docs-build` check builds both kinds: once as `next`, and once as the release site without the newest archive, the way `docs-live` builds it. For a local check, run `DOCS_CHANNEL=release npm run build`; it still includes the archive of the newest line.

## Another domain or a sub-path

`astro.config.mjs` reads `SITE_URL` and `BASE_PATH`. Set them as build environment variables in Cloudflare, and update the schema URL in `src/content/docs/getting-started/quickstart.md`. Internal links are relative, so they survive any base path. To check a sub-path build: `BASE_PATH=/pipetree npm run build`.
