"""The generated reference pages and the JSON Schema must match the code.

If this fails, run `uv run python docs/scripts/gen_reference.py` and commit the result.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "docs" / "scripts" / "gen_reference.py"


def _load_generator() -> ModuleType:
    spec = importlib.util.spec_from_file_location("gen_reference", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def generator() -> ModuleType:
    return _load_generator()


@pytest.mark.parametrize("key", ["yaml", "cli", "schema"])
def test_committed_reference_matches_the_code(generator: ModuleType, key: str):
    expected = generator.render_all()[key]
    path: Path = generator.OUTPUTS[key]
    assert path.read_text() == expected, (
        f"{path.relative_to(ROOT)} is stale: run `uv run python docs/scripts/gen_reference.py`"
    )


def test_every_model_field_has_a_description(generator: ModuleType):
    schema = json.loads(generator.OUTPUTS["schema"].read_text())
    for model, definition in schema["$defs"].items():
        for name, prop in definition.get("properties", {}).items():
            assert prop.get("description"), f"{model}.{name} has no description"
