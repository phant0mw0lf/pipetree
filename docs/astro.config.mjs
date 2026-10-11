// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { pluginLanguageBadge } from 'expressive-code-language-badge';
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
