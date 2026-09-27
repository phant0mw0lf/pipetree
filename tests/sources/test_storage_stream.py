import dataclasses

import pytest

from pipetree.model import System, Table
from pipetree.platform.local import LocalPlatform
from pipetree.sources.base import SourceContext
from pipetree.sources.storage_stream import StorageStreamSource

pytestmark = pytest.mark.spark


def make_table(fqn: str = "bronze.events") -> Table:
    return Table.model_validate(
        {
            "name": fqn.split(".")[-1],
            "layer": fqn.split(".")[0],
            "table_schema": fqn.split(".")[0],
            "fqn": fqn,
            "strategy": "append",
            "source": {"system": "landing", "object": "events"},
        }
    )


def make_ctx(spark, tmp_path, *, init: bool = False, fqn: str = "bronze.events") -> SourceContext:
    (tmp_path / "landing").mkdir(exist_ok=True)
    system = System.model_validate({"type": "storage_stream", "path": "landing", "format": "csv"})
    return SourceContext(spark, make_table(fqn), system, LocalPlatform(), tmp_path, init=init)


def write_landing_file(tmp_path, name: str, rows: list[tuple[int, str]]) -> None:
    (tmp_path / "landing").mkdir(exist_ok=True)
    lines = "id,name\n" + "\n".join(f"{i},{n}" for i, n in rows) + "\n"
    (tmp_path / "landing" / name).write_text(lines)


def test_reads_files_present_on_the_first_run(spark, tmp_path):
    write_landing_file(tmp_path, "day1.csv", [(1, "Alice")])
    ctx = make_ctx(spark, tmp_path)

    df = StorageStreamSource().read(ctx)

    assert {r["name"] for r in df.collect()} == {"Alice"}


def test_a_second_run_only_returns_newly_arrived_files(spark, tmp_path):
    write_landing_file(tmp_path, "day1.csv", [(1, "Alice")])
    ctx = make_ctx(spark, tmp_path, fqn="bronze.events2")
    StorageStreamSource().read(ctx)

    write_landing_file(tmp_path, "day2.csv", [(2, "Bob")])
    df = StorageStreamSource().read(ctx)

    assert {r["name"] for r in df.collect()} == {"Bob"}


def test_a_run_with_no_new_files_returns_an_empty_dataframe(spark, tmp_path):
    write_landing_file(tmp_path, "day1.csv", [(1, "Alice")])
    ctx = make_ctx(spark, tmp_path, fqn="bronze.events3")
    StorageStreamSource().read(ctx)

    df = StorageStreamSource().read(ctx)

    assert df.count() == 0


def test_init_reprocesses_everything_from_scratch(spark, tmp_path):
    write_landing_file(tmp_path, "day1.csv", [(1, "Alice")])
    ctx = make_ctx(spark, tmp_path, fqn="bronze.events4")
    StorageStreamSource().read(ctx)
    write_landing_file(tmp_path, "day2.csv", [(2, "Bob")])
    StorageStreamSource().read(ctx)

    init_ctx = dataclasses.replace(ctx, init=True)
    df = StorageStreamSource().read(init_ctx)

    assert {r["name"] for r in df.collect()} == {"Alice", "Bob"}


def test_raises_a_clear_error_when_format_is_missing(spark, tmp_path):
    (tmp_path / "landing").mkdir()
    system = System.model_validate({"type": "storage_stream", "path": "landing"})
    ctx = SourceContext(spark, make_table(), system, LocalPlatform(), tmp_path)

    with pytest.raises(ValueError, match="format"):
        StorageStreamSource().read(ctx)
