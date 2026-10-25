"""Archive the docs of the version being released, for the version select.

    uv run python docs/scripts/archive_docs.py

Run it in the release pull request, after the version bump and
`gen_reference.py`. It snapshots the pages at the root of
docs/src/content/docs/ (which describe the version being released) as the
archived version of its minor line, for example 0.2 for 0.2.3:

- docs/src/content/docs/<line>/        the pages (made by starlight-versions)
- docs/src/content/versions/<line>.json their sidebar (made by starlight-versions)
- docs/src/assets/<line>/              the images they use (made by starlight-versions)
- docs/public/<line>/pipetree.schema.json the JSON Schema of that line
- docs/versions.json                    the list of archived lines, newest first

An existing archive of the same line is replaced, so a patch release refreshes
it. The snapshot is taken by `npm run build` (the plugin copies the root pages
when versions.json names a line that has no folder yet), so `npm ci` must have
run in docs/. tests/test_docs_versions.py fails when the newest archive does
not match the version in the code.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from pipetree import __version__

DOCS = Path(__file__).resolve().parent.parent
PAGES = DOCS / "src" / "content" / "docs"
SIDEBARS = DOCS / "src" / "content" / "versions"
ASSETS = DOCS / "src" / "assets"
PUBLIC = DOCS / "public"
VERSIONS = DOCS / "versions.json"
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


def _key(line: str) -> tuple[int, int]:
    major, minor = line.split(".")
    return int(major), int(minor)


def main() -> None:
    line = line_of(__version__)
    if not (DOCS / "node_modules").is_dir():
        raise SystemExit("run `npm ci` in docs/ first")

    archived: list[str] = json.loads(VERSIONS.read_text())
    for path in (PAGES / line, ASSETS / line, PUBLIC / line):
        shutil.rmtree(path, ignore_errors=True)
    (SIDEBARS / f"{line}.json").unlink(missing_ok=True)
    archived = sorted({*archived, line}, key=_key, reverse=True)
    VERSIONS.write_text(json.dumps(archived, indent=2) + "\n")

    # The plugin snapshots the root pages into PAGES/<line> while it sets up the build.
    subprocess.run(
        ["npm", "run", "build"], cwd=DOCS, check=True, env={**os.environ, "DOCS_CHANNEL": "next"}
    )
    if not (PAGES / line).is_dir() or not (SIDEBARS / f"{line}.json").is_file():
        raise SystemExit(f"the build did not create the archive of {line}")

    # Editors of a config written for this line should keep this line's schema.
    (PUBLIC / line).mkdir(parents=True)
    shutil.copy2(PUBLIC / "pipetree.schema.json", PUBLIC / line / "pipetree.schema.json")
    versioned_url = SCHEMA_URL.replace("/pipetree.schema.json", f"/{line}/pipetree.schema.json")
    for page in (PAGES / line).rglob("*.md*"):
        text = page.read_text()
        if SCHEMA_URL in text:
            page.write_text(text.replace(SCHEMA_URL, versioned_url))

    print(f"archived the docs of pipetree {__version__} as {line}", file=sys.stderr)


if __name__ == "__main__":
    main()
