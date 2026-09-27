import pytest

import pipetree.sources  # noqa: F401 - registers the built-in readers as a side effect
from pipetree.sources.base import get_reader
from pipetree.sources.kusto import KustoSource
from pipetree.sources.local_file import LocalFileSource
from pipetree.sources.sqlserver import SqlServerSource
from pipetree.sources.storage_stream import StorageStreamSource

pytestmark = pytest.mark.spark


@pytest.mark.parametrize("type_name", ["csv", "json", "parquet"])
def test_local_file_types_are_registered(type_name):
    assert isinstance(get_reader(type_name), LocalFileSource)


def test_sqlserver_is_registered():
    assert isinstance(get_reader("sqlserver"), SqlServerSource)


def test_kusto_is_registered():
    assert isinstance(get_reader("kusto"), KustoSource)


def test_storage_stream_is_registered():
    assert isinstance(get_reader("storage_stream"), StorageStreamSource)


@pytest.mark.parametrize("type_name", ["d365_export", "synapse_link"])
def test_file_landing_aliases_share_the_storage_stream_reader(type_name):
    # Thin wrappers, not separate subsystems - the plan is explicit that
    # these fall out of the same machinery as storage_stream.
    assert get_reader(type_name) is get_reader("storage_stream")
