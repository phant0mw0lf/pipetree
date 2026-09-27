from typing import Any

import pytest

from pipetree.sources.custom import load_custom_reader


class FakeCustomReader:
    # ctx/return both Any: a minimal sanity check that dynamic loading and
    # invocation work, not a type-correct SourceReader usage.
    def read(self, ctx: Any) -> Any:
        return f"custom-read:{ctx}"


class NotAReader:
    pass


def test_loads_a_reader_by_dotted_class_path():
    # Untyped on purpose: load_custom_reader() is declared to return the
    # real SourceReader protocol (read(ctx: SourceContext) -> DataFrame),
    # but this fake takes/returns plain strings to keep the test simple.
    reader: Any = load_custom_reader("tests.sources.test_custom.FakeCustomReader")

    assert reader.read("ctx") == "custom-read:ctx"


def test_raises_a_clear_error_for_a_malformed_path():
    with pytest.raises(ValueError, match="dotted"):
        load_custom_reader("not_a_dotted_path")


def test_raises_a_clear_error_for_a_missing_module():
    with pytest.raises(ImportError):
        load_custom_reader("tests.sources.does_not_exist.SomeReader")


def test_raises_a_clear_error_for_a_missing_attribute():
    with pytest.raises(ImportError, match="SomeReader"):
        load_custom_reader("tests.sources.test_custom.SomeReader")


def test_raises_a_clear_error_when_the_class_does_not_implement_source_reader():
    with pytest.raises(TypeError, match="SourceReader"):
        load_custom_reader("tests.sources.test_custom.NotAReader")
