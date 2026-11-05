import { defineCollection } from 'astro:content';
import { docsLoader } from '@astrojs/starlight/loaders';
import { docsSchema, i18nSchema } from '@astrojs/starlight/schema';
import { docsVersionsLoader } from 'starlight-versions/loader';

import { channel } from '../channel.mjs';

// On `next` builds the root pages are unreleased, so the plugin's "switch to the latest version"
// (a link to the root) would point at unreleased docs. These strings name the link for what it is.
// The release site keeps the plugin's wording.
const nextTranslations = {
  'starlightVersions.link.latest': 'Switch to the <a href={{link}}>next, unreleased version</a>.',
  'starlightVersions.search.link.latest':
    'Switch to the <a href={{link}}>next, unreleased version</a> to search it.',
};

export const collections = {
  docs: defineCollection({ loader: docsLoader(), schema: docsSchema() }),
  // Sidebar and excluded pages of each archived version (src/content/versions/<slug>.json).
  versions: defineCollection({ loader: docsVersionsLoader() }),
  i18n: defineCollection({
    loader: () => (channel === 'next' ? [{ id: 'en', ...nextTranslations }] : []),
    schema: i18nSchema(),
  }),
};
