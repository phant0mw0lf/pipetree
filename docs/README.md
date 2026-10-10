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

## Hosting: Cloudflare Pages

Cloudflare builds and deploys the site from the GitHub repository. `.github/workflows/docs.yml` (`docs-build`) only checks that a change builds and that all internal links are valid.

Settings when creating the project (Workers & Pages, Create, Pages, Connect to Git):

| Setting | Value |
| --- | --- |
| Repository | `phant0mw0lf/pipetree` |
| Project name | `pipetree` |
| Production branch | `main` |
| Root directory | `docs` |
| Build command | `npm ci && npm run build` |
| Build output directory | `dist` |
| Environment variable | `NODE_VERSION` = `24` (also pinned in `.nvmrc` and `engines`) |
| Preview deployments | on, for all branches and pull requests |
| Custom domain | `pipetree.dev` (Custom domains, Set up a domain) |

`SITE_URL` already defaults to `https://pipetree.dev` and `BASE_PATH` to `/`, so no other variable is needed. The `pipetree.pages.dev` address keeps working as the fallback and hosts the previews. A redirect from `www.pipetree.dev` is not possible in `_redirects` (it cannot redirect across hosts): add it as a Redirect Rule in Cloudflare if wanted.

The Cloudflare build runs Node only, with no Python. That works because the generated reference files and `public/pipetree.schema.json` are committed.

`public/_headers` sets the security headers. The Content-Security-Policy allows inline scripts and styles (Starlight needs them) and `wasm-unsafe-eval` (search).

## Another domain or a sub-path

`astro.config.mjs` reads `SITE_URL` and `BASE_PATH`. Set them as build environment variables in Cloudflare, and update the schema URL in `src/content/docs/getting-started/quickstart.md`. Internal links are relative, so they survive any base path. To check a sub-path build: `BASE_PATH=/pipetree npm run build`.
