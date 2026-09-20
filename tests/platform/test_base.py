import sys
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import ModuleType

import pytest

from pipetree.platform.base import Platform, detect, resolve_value


def _fake_module(name: str) -> ModuleType:
    """A module fake enough for importlib.util.find_spec to see it as
    genuinely importable, not just present in sys.modules."""
    module = ModuleType(name)
    module.__spec__ = ModuleSpec(name, loader=None)
    return module


class _FakePlatform:
    name = "fake"

    def resolve_secret(self, name: str) -> str:
        return f"secret:{name}"

    def qualify_table_name(self, fqn: str) -> str:
        return fqn

    def resolve_path(self, relative_path: str, base_dir: Path) -> str:
        return str(base_dir / relative_path)

    def run_metadata(self) -> dict[str, str]:
        return {}


def test_platform_protocol_is_satisfied_by_duck_typing():
    assert isinstance(_FakePlatform(), Platform)


def test_resolve_value_returns_a_literal_string_unchanged():
    assert resolve_value("erp-landing-path", _FakePlatform()) == "erp-landing-path"


def test_resolve_value_resolves_a_secret_reference_via_the_platform():
    assert resolve_value({"secret": "erp-landing-path"}, _FakePlatform()) == (
        "secret:erp-landing-path"
    )


def test_resolve_value_rejects_anything_else():
    with pytest.raises(TypeError):
        resolve_value(123, _FakePlatform())


def test_detect_returns_databricks_when_the_runtime_env_var_is_set(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")
    monkeypatch.delitem(sys.modules, "notebookutils", raising=False)

    assert detect() == "databricks"


def test_detect_returns_fabric_when_notebookutils_is_importable(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.setitem(sys.modules, "notebookutils", _fake_module("notebookutils"))

    assert detect() == "fabric"


def test_detect_returns_local_otherwise(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.delitem(sys.modules, "notebookutils", raising=False)

    assert detect() == "local"


def test_detect_prefers_databricks_over_fabric_if_both_signals_present(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")
    monkeypatch.setitem(sys.modules, "notebookutils", _fake_module("notebookutils"))

    assert detect() == "databricks"
