"""Complete YAML configs in the docs must pass the config loader.

A fenced block marked ```yaml validate is a complete config; plain ```yaml blocks are
fragments and are not checked.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from pipetree.config.loader import load_config
from pipetree.model import PipelineConfig

PAGES = Path(__file__).resolve().parent.parent / "docs" / "src" / "content" / "docs"
_BLOCK = re.compile(r"^```yaml validate[^\n]*\n(.*?)^```", re.DOTALL | re.MULTILINE)


def _blocks() -> list[tuple[str, int, str]]:
    found = []
    for page in sorted(PAGES.rglob("*.md*")):
        text = page.read_text()
        for i, match in enumerate(_BLOCK.finditer(text), start=1):
            found.append((str(page.relative_to(PAGES)), i, match.group(1)))
    return found


BLOCKS = _blocks()


def test_the_docs_have_validated_examples():
    assert len(BLOCKS) >= 9


@pytest.mark.parametrize(("page", "index", "text"), BLOCKS, ids=[f"{p}#{i}" for p, i, _ in BLOCKS])
def test_yaml_example_is_a_valid_config(tmp_path: Path, page: str, index: int, text: str):
    path = tmp_path / "pipeline.yaml"
    path.write_text(text)
    PipelineConfig.from_validated_raw(load_config(path))
