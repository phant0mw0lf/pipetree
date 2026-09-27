import pytest

from pipetree.model import System, Table
from pipetree.platform.local import LocalPlatform
from pipetree.sources.base import SourceContext
from pipetree.sources.local_file import LocalFileSource

pytestmark = pytest.mark.spark


def make_table(fqn: str = "bronze.customer") -> Table:
    return Table.model_validate(
        {
            "name": fqn.split(".")[-1],
            "layer": fqn.split(".")[0],
            "table_schema": fqn.split(".")[0],
            "fqn": fqn,
            "strategy": "scd1",
            "source": {"system": "s1", "object": "customer"},
        }
    )


def test_reads_a_csv_file_with_header_and_inferred_schema(spark, tmp_path):
    (tmp_path / "customer.csv").write_text("id,name\n1,Alice\n2,Bob\n")
    system = System.model_validate({"type": "csv", "path": "customer.csv"})
    ctx = SourceContext(spark, make_table(), system, LocalPlatform(), tmp_path)

    df = LocalFileSource().read(ctx)

    assert {r["name"] for r in df.collect()} == {"Alice", "Bob"}


def test_reads_a_json_file(spark, tmp_path):
    (tmp_path / "customer.json").write_text('{"id": 1, "name": "Alice"}\n')
    system = System.model_validate({"type": "json", "path": "customer.json"})
    ctx = SourceContext(spark, make_table(), system, LocalPlatform(), tmp_path)

    df = LocalFileSource().read(ctx)

    assert df.collect()[0]["name"] == "Alice"


def test_resolves_the_path_through_the_platform(spark, tmp_path):
    (tmp_path / "customer.csv").write_text("id,name\n1,Alice\n")

    class RecordingPlatform(LocalPlatform):
        def __init__(self):
            self.calls = []

        def resolve_path(self, relative_path, base_dir):
            self.calls.append((relative_path, base_dir))
            return super().resolve_path(relative_path, base_dir)

    platform = RecordingPlatform()
    system = System.model_validate({"type": "csv", "path": "customer.csv"})
    ctx = SourceContext(spark, make_table(), system, platform, tmp_path)

    LocalFileSource().read(ctx)

    assert platform.calls == [("customer.csv", tmp_path)]


def test_raises_a_clear_error_when_path_is_missing(spark, tmp_path):
    system = System.model_validate({"type": "csv"})
    ctx = SourceContext(spark, make_table(), system, LocalPlatform(), tmp_path)

    with pytest.raises(ValueError, match="'path'"):
        LocalFileSource().read(ctx)


def test_resolves_a_secret_path(spark, tmp_path, monkeypatch):
    (tmp_path / "customer.csv").write_text("id,name\n1,Alice\n")
    monkeypatch.setenv("CUSTOMER_CSV_PATH", "customer.csv")
    system = System.model_validate({"type": "csv", "path": {"secret": "customer-csv-path"}})
    ctx = SourceContext(spark, make_table(), system, LocalPlatform(), tmp_path)

    df = LocalFileSource().read(ctx)

    assert df.collect()[0]["name"] == "Alice"
