"""Assemble the release site, pipetree.dev (branch docs-live).

    python3 docs/scripts/build_release_site.py OUT [--latest vX.Y.Z]

Writes OUT/docs, the docs/ directory Cloudflare builds as pipetree.dev:

- pages at the root, their images and /pipetree.schema.json: the newest final
  release tag, so links without a version always show the latest release
- next/: a snapshot of the root pages of the current checkout (main), the
  unreleased docs
- config, components, other public files and the archived lines: the current
  checkout, minus the archive of the release's own line (it is at the root)

Runs `npm ci` and one build in OUT/docs (the snapshot), so it needs Node and
the network. .github/workflows/docs-live.yml commits the result to docs-live;
the docs-build check builds it on every pull request.
"""

from __future__ import annotations

import argparse
import io
import json
import re
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from archive_docs import line_of, order, snapshot  # noqa: E402

FINAL_TAG = re.compile(r"^v\d+\.\d+\.\d+$")
# Directories whose top-level entries are per version: root pages, or version folders.
PER_VERSION = (Path("src/content/docs"), Path("src/assets"))


def git(*args: str) -> bytes:
    return subprocess.run(["git", *args], check=True, capture_output=True).stdout


def newest_release() -> str:
    tags = [t for t in git("tag", "--list", "v*").decode().split() if FINAL_TAG.match(t)]
    if not tags:
        raise SystemExit("no final release tag (vX.Y.Z) found; fetch the tags")
    return max(tags, key=lambda t: tuple(map(int, t[1:].split("."))))


def extract(ref: str, dest: Path, *paths: str) -> None:
    """Write `paths` of `ref` below `dest`, keeping their repository paths."""
    archive = git("archive", "--format=tar", ref, *paths)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(dest, filter="data")


def remove(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("out", type=Path, help="empty or missing directory")
    parser.add_argument("--latest", help="release tag at the root (default: the newest vX.Y.Z)")
    args = parser.parse_args()
    out: Path = args.out.resolve()
    if out.exists() and any(out.iterdir()):
        raise SystemExit(f"{out} is not empty")
    latest: str = args.latest or newest_release()
    line = line_of(latest.removeprefix("v"))

    # The current checkout, with `next` snapshotted from its root pages.
    extract("HEAD", out, "docs")
    site = out / "docs"
    subprocess.run(["npm", "ci", "--no-audit", "--no-fund"], cwd=site, check=True)
    snapshot(site, "next")
    versions = set(json.loads((site / "versions.json").read_text()))

    # The root pages of the release replace those of the checkout.
    tag = out / ".tag"
    extract(latest, tag, *(f"docs/{p}" for p in PER_VERSION), "docs/public/pipetree.schema.json")
    tag_versions_file = git("ls-tree", "--name-only", latest, "docs/versions.json").strip()
    tag_versions = (
        set(json.loads(git("show", f"{latest}:docs/versions.json"))) if tag_versions_file else set()
    )
    for rel in PER_VERSION:
        for entry in (site / rel).iterdir():
            if entry.name not in versions:
                remove(entry)
        source = tag / "docs" / rel
        for entry in source.iterdir() if source.is_dir() else ():
            if entry.name not in tag_versions:
                shutil.move(entry, site / rel / entry.name)
    shutil.copy2(tag / "docs/public/pipetree.schema.json", site / "public/pipetree.schema.json")

    # The release's own line is at the root; its archive would be listed twice.
    for path in (
        site / "src/content/docs" / line,
        site / "src/assets" / line,
        site / "src/content/versions" / f"{line}.json",
    ):
        remove(path)
    (site / "versions.json").write_text(json.dumps(order(versions - {line}), indent=2) + "\n")

    for path in (tag, site / "node_modules", site / "dist", site / ".astro"):
        remove(path)
    print(f"{site}: {latest} at the root, next from HEAD", file=sys.stderr)


if __name__ == "__main__":
    main()
