// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { pluginLanguageBadge } from 'expressive-code-language-badge';
import starlightLinksValidator from 'starlight-links-validator';
import starlightVersions from 'starlight-versions';
import fs from 'node:fs';

// Hosted on Cloudflare at https://pipetree.dev, at the root, built from the branch docs-live (see docs/README.md).
const site = process.env.SITE_URL || 'https://pipetree.dev';
const base = process.env.BASE_PATH || '/';

// Which docs this build is (see docs/README.md, "Versions"):
// - `release`: the branch docs-live, production (pipetree.dev): the newest release at the root,
//   main's unreleased docs under /next/.
// - `next`: everything else (main, pull request previews, local builds): unreleased docs at the root.
// Cloudflare Workers Builds sets WORKERS_CI_BRANCH; DOCS_CHANNEL overrides it for a local check.
const channel =
  process.env.DOCS_CHANNEL || (process.env.WORKERS_CI_BRANCH === 'docs-live' ? 'release' : 'next');
if (channel !== 'release' && channel !== 'next') {
  throw new Error(`DOCS_CHANNEL must be "release" or "next", not "${channel}"`);
}

// The released version: the generated CLI reference carries it (docs/scripts/gen_reference.py).
const cliPage = fs.readFileSync(new URL('./src/content/docs/reference/cli.mdx', import.meta.url), 'utf8');
const release = cliPage.match(/\(pipetree ([0-9][^)]*)\)/)?.[1];
if (!release) throw new Error('reference/cli.mdx does not name the pipetree version');
const releaseLine = release.split('.').slice(0, 2).join('.');

// Versions besides the root pages, each a folder in src/content/docs/: the archived minor lines,
// newest first (docs/scripts/archive_docs.py). The release site also has `next`, the unreleased
// docs of main, and leaves out the archive of its own line, which it shows at the root
// (docs/scripts/build_release_site.py).
const archived = JSON.parse(fs.readFileSync(new URL('./versions.json', import.meta.url), 'utf8'));
const versions = archived
  .filter((slug) => channel === 'next' || slug !== releaseLine)
  .map((slug) => ({ slug, label: slug === 'next' ? 'next (unreleased)' : `v${slug}` }));
const versioned = versions.length > 0;

export default defineConfig({
  site,
  base,
  integrations: [
    starlight({
      title: 'pipetree',
      logo: { src: './src/assets/logo.svg', alt: 'pipetree' },
      favicon: '/favicon.svg',
      description: 'Metadata-driven pipeline orchestration: YAML in, a runnable dependency tree out.',
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/phant0mw0lf/pipetree' },
      ],
      editLink: {
        baseUrl: 'https://github.com/phant0mw0lf/pipetree/edit/main/docs/',
      },
      head: [
        { tag: 'meta', attrs: { property: 'og:site_name', content: 'pipetree' } },
        { tag: 'link', attrs: { rel: 'icon', href: '/favicon.ico', sizes: '48x48' } },
        { tag: 'link', attrs: { rel: 'apple-touch-icon', href: '/apple-touch-icon.png' } },
        { tag: 'meta', attrs: { property: 'og:image', content: new URL('/og.png', site).href } },
        { tag: 'meta', attrs: { name: 'twitter:card', content: 'summary_large_image' } },
        { tag: 'meta', attrs: { name: 'twitter:image', content: new URL('/og.png', site).href } },
      ],
      expressiveCode: {
        plugins: [pluginLanguageBadge()],
        styleOverrides: {
          languageBadge: {
            fontSize: '0.65rem',
            fontWeight: '600',
            fontColor: 'var(--sl-color-gray-2)',
            background: 'var(--sl-color-gray-6)',
            borderColor: 'var(--sl-color-gray-5)',
            borderWidth: '1px',
            opacity: '0.9',
          },
        },
      },
      // starlight-versions renders the version select and the notices; it needs at least one
      // archived version.
      components: channel === 'next' && versioned ? { Banner: './src/components/NextBanner.astro' } : {},
      plugins: [
        ...(versioned
          ? [
              starlightVersions({
                current: { label: channel === 'release' ? `v${release} (latest)` : 'next (unreleased)' },
                versions,
                // Not tied to a version: always the newest text, and not archived.
                exclude: ['about/**'],
              }),
            ]
          : []),
        starlightLinksValidator({ errorOnRelativeLinks: false }),
      ],
      sidebar: [
        { label: 'Getting started', items: [{ autogenerate: { directory: 'getting-started' } }] },
        { label: 'Concepts', items: [{ autogenerate: { directory: 'concepts' } }] },
        { label: 'Merge strategies', items: [{ autogenerate: { directory: 'merge' } }] },
        { label: 'Guides', items: [{ autogenerate: { directory: 'guides' } }] },
        { label: 'Reference', items: [{ autogenerate: { directory: 'reference' } }] },
        { label: 'About', items: [{ autogenerate: { directory: 'about' } }] },
      ],
    }),
  ],
});
