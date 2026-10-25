"""Archive the docs of the version being released, for the version select.

    uv run python docs/scripts/archive_docs.py

Run it in the release pull request, after the version bump and
`gen_reference.py`. It snapshots the pages at the root of
docs/src/content/docs/ (which describe the version being released) as the
archived version of its minor line, for example 0.2 for 0.2.3:

- docs/src/content/docs/<line>/          the pages (made by starlight-versions)
- docs/src/content/versions/<line>.json  their sidebar (made by starlight-versions)
- docs/src/assets/<line>/                the images they use (made by starlight-versions)
- docs/public/<line>/pipetree.schema.json the JSON Schema of that line
- docs/versions.json                      the list of archived lines, newest first

An existing archive of the same line is replaced, so a patch release refreshes
it. The snapshot is taken by `npm run build` (the plugin copies the root pages
when versions.json names a version that has no folder yet), so `npm ci` must
have run in docs/. tests/test_docs_versions.py fails when the newest archive
does not match the version in the code. build_release_site.py uses
`snapshot()` to make the `next` folder of the release site.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

DOCS = Path(__file__).resolve().parent.parent
SCHEMA_URL = "https://pipetree.dev/pipetree.schema.json"
FINAL = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


def line_of(version: str) -> str:
    """The minor line of a final version: 0.2 for 0.2.3."""
    match = FINAL.match(version)
    if not match:
        raise SystemExit(
            f"pipetree {version} is not a final version: archive the docs at the final release"
        )
    return f"{match.group(1)}.{match.group(2)}"


def order(slugs: set[str]) -> list[str]:
    """`next` first, then the minor lines, newest first."""
    lines = sorted(
        (s for s in slugs if s != "next"), key=lambda s: tuple(map(int, s.split("."))), reverse=True
    )
    return (["next"] if "next" in slugs else []) + lines


def snapshot(docs: Path, slug: str) -> None:
    """Copy the root pages of `docs` into the version `slug`, replacing an older copy."""
    pages = docs / "src" / "content" / "docs"
    sidebars = docs / "src" / "content" / "versions"
    public = docs / "public"
    if not (docs / "node_modules").is_dir():
        raise SystemExit(f"run `npm ci` in {docs} first")

    for path in (pages / slug, docs / "src" / "assets" / slug, public / slug):
        shutil.rmtree(path, ignore_errors=True)
    (sidebars / f"{slug}.json").unlink(missing_ok=True)
    versions = docs / "versions.json"
    archived = set(json.loads(versions.read_text())) | {slug}
    versions.write_text(json.dumps(order(archived), indent=2) + "\n")

    # The plugin snapshots the root pages into pages/<slug> while it sets up the build.
    subprocess.run(
        ["npm", "run", "build"], cwd=docs, check=True, env={**os.environ, "DOCS_CHANNEL": "next"}
    )
    if not (pages / slug).is_dir() or not (sidebars / f"{slug}.json").is_file():
        raise SystemExit(f"the build did not create the version {slug}")

    # Editors of a config written for this version should keep this version's schema.
    (public / slug).mkdir(parents=True)
    shutil.copy2(public / "pipetree.schema.json", public / slug / "pipetree.schema.json")
    versioned_url = SCHEMA_URL.replace("/pipetree.schema.json", f"/{slug}/pipetree.schema.json")
    for page in (pages / slug).rglob("*.md*"):
        text = page.read_text()
        if SCHEMA_URL in text:
            page.write_text(text.replace(SCHEMA_URL, versioned_url))


def main() -> None:
    from pipetree import __version__

    line = line_of(__version__)
    snapshot(DOCS, line)
    print(f"archived the docs of pipetree {__version__} as {line}", file=sys.stderr)


if __name__ == "__main__":
    main()
