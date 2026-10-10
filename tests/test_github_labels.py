"""Consistency checks for the GitHub label definitions under .github/."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

GITHUB = Path(__file__).resolve().parent.parent / ".github"

pytestmark = pytest.mark.skipif(not GITHUB.is_dir(), reason=".github is not available")


def _load(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def label_entries() -> list[dict[str, Any]]:
    entries = _load(GITHUB / "labels.yml")
    assert isinstance(entries, list)
    return entries


@pytest.fixture(scope="module")
def label_names(label_entries: list[dict[str, Any]]) -> set[str]:
    return {entry["name"] for entry in label_entries}


def test_labels_have_required_fields(label_entries: list[dict[str, Any]]) -> None:
    for entry in label_entries:
        assert set(entry) == {"name", "color", "description"}, entry
        assert re.fullmatch(r"[0-9a-fA-F]{6}", str(entry["color"])), entry
        assert isinstance(entry["description"], str) and entry["description"].strip(), entry
        # GitHub rejects descriptions longer than 100 characters.
        assert len(entry["description"]) <= 100, entry


def test_label_names_are_unique(label_entries: list[dict[str, Any]]) -> None:
    names = [entry["name"].lower() for entry in label_entries]
    assert len(names) == len(set(names))


def test_removed_labels_are_not_defined(label_names: set[str]) -> None:
    assert not label_names & {"invalid", "accessibility"}


def test_labeler_config_only_uses_defined_labels(label_names: set[str]) -> None:
    config = _load(GITHUB / "labeler.yml")
    assert config
    assert set(config) <= label_names


def test_issue_templates_only_use_defined_labels(label_names: set[str]) -> None:
    templates = sorted((GITHUB / "ISSUE_TEMPLATE").glob("*.yml"))
    assert templates
    for template in templates:
        labels = _load(template).get("labels", [])
        assert set(labels) <= label_names, template.name


def test_pr_type_label_workflow_only_uses_defined_labels(label_names: set[str]) -> None:
    source = (GITHUB / "workflows" / "pr-type-label.yml").read_text(encoding="utf-8")
    block = re.search(r"// labels:begin(.*?)// labels:end", source, re.DOTALL)
    assert block
    used = set(re.findall(r"'([^']+)'", block.group(1)))
    assert used
    assert used <= label_names
