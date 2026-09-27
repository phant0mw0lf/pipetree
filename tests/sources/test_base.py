from typing import Any

import pytest

from pipetree.model import System, Table
from pipetree.platform.local import LocalPlatform
from pipetree.sources.base import SourceContext, SourceReader, get_reader, register_reader

pytestmark = pytest.mark.spark


def make_context(spark, tmp_path, system_type: str = "csv") -> SourceContext:
    table = Table.model_validate(
        {
            "name": "customer",
            "layer": "bronze",
            "table_schema": "bronze",
            "fqn": "bronze.customer",
            "strategy": "scd1",
            "source": {"system": "s1", "object": "customer"},
        }
    )
    system = System.model_validate({"type": system_type})
    return SourceContext(
        spark=spark, table=table, system=system, platform=LocalPlatform(), base_dir=tmp_path
    )


class _FakeReader:
    # Returns Any, not the protocol's real DataFrame - this fake exists to
    # prove the registry dispatches to it, not to produce real data.
    def read(self, ctx: SourceContext) -> Any:
        return f"read:{ctx.system.type}"


def test_source_reader_protocol_is_satisfied_by_duck_typing():
    assert isinstance(_FakeReader(), SourceReader)


def test_get_reader_returns_the_registered_reader_for_a_type(spark, tmp_path):
    register_reader("_test_type_a", _FakeReader())
    ctx = make_context(spark, tmp_path, system_type="_test_type_a")

    reader = get_reader("_test_type_a")

    assert reader.read(ctx) == "read:_test_type_a"


def test_get_reader_raises_a_clear_error_for_an_unknown_type():
    with pytest.raises(KeyError, match="_totally_unknown_type"):
        get_reader("_totally_unknown_type")


def test_register_reader_can_alias_one_type_to_another(spark, tmp_path):
    reader = _FakeReader()
    register_reader("_test_type_b", reader)
    register_reader("_test_type_b_alias", reader)

    assert get_reader("_test_type_b") is get_reader("_test_type_b_alias")
