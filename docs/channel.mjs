// Which docs a build is (see docs/README.md, "Versions"), shared by astro.config.mjs and
// src/content.config.ts:
// - `release`: the branch docs-live, production (pipetree.dev): the newest release at the root,
//   main's unreleased docs under /next/.
// - `next`: everything else (main, pull request previews, local builds): unreleased docs at the root.
// Cloudflare Workers Builds sets WORKERS_CI_BRANCH; DOCS_CHANNEL overrides it for a local check.
export const channel =
  process.env.DOCS_CHANNEL || (process.env.WORKERS_CI_BRANCH === 'docs-live' ? 'release' : 'next');
if (channel !== 'release' && channel !== 'next') {
  throw new Error(`DOCS_CHANNEL must be "release" or "next", not "${channel}"`);
}
