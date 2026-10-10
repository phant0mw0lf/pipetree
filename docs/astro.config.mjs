// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';

// Hosted on Cloudflare Pages at https://pipetree.dev, at the root (see docs/README.md).
const site = process.env.SITE_URL || 'https://pipetree.dev';
const base = process.env.BASE_PATH || '/';

export default defineConfig({
  site,
  base,
  integrations: [
    starlight({
      title: 'pipetree',
      description: 'Metadata-driven pipeline orchestration: YAML in, a runnable dependency tree out.',
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/phant0mw0lf/pipetree' },
      ],
      editLink: {
        baseUrl: 'https://github.com/phant0mw0lf/pipetree/edit/main/docs/',
      },
      head: [
        { tag: 'meta', attrs: { property: 'og:site_name', content: 'pipetree' } },
      ],
      plugins: [starlightLinksValidator({ errorOnRelativeLinks: false })],
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
