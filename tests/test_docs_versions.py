"""The archived docs versions (docs/versions.json) must be complete and current.

Each release pull request runs `uv run python docs/scripts/archive_docs.py`, which
snapshots the docs of the version being released as the archive of its minor line.
If a test here fails, run it and commit the result.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from pipetree import __version__

DOCS = Path(__file__).resolve().parent.parent / "docs"
PAGES = DOCS / "src" / "content" / "docs"
ARCHIVED: list[str] = json.loads((DOCS / "versions.json").read_text())
FINAL = re.compile(r"^(\d+)\.(\d+)\.\d+$")


def _key(line: str) -> tuple[int, int]:
    major, minor = line.split(".")
    return int(major), int(minor)


def test_archived_lines_are_unique_and_newest_first():
    assert all(re.fullmatch(r"\d+\.\d+", line) for line in ARCHIVED)
    assert sorted(set(ARCHIVED), key=_key, reverse=True) == ARCHIVED


@pytest.mark.parametrize("line", ARCHIVED)
def test_archive_is_complete(line: str):
    assert (PAGES / line / "index.mdx").is_file()
    assert (DOCS / "src" / "content" / "versions" / f"{line}.json").is_file()
    assert (DOCS / "public" / line / "pipetree.schema.json").is_file()


def test_the_released_version_is_archived():
    match = FINAL.match(__version__)
    if not match:
        pytest.skip(f"{__version__} is a pre-release; its docs are archived at the final release")
    line = f"{match.group(1)}.{match.group(2)}"
    assert ARCHIVED and ARCHIVED[0] == line, (
        f"pipetree {__version__} is not archived: run `uv run python docs/scripts/archive_docs.py`"
    )
    cli = (PAGES / line / "reference" / "cli.mdx").read_text()
    assert f"(pipetree {__version__})" in cli, (
        f"the archive of {line} is older than {__version__}: "
        "run `uv run python docs/scripts/archive_docs.py`"
    )
