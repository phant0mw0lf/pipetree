"""Importing this package registers every built-in SourceReader against
its `systems.<name>.type` value."""

from __future__ import annotations

from pipetree.sources.base import get_reader, register_reader
from pipetree.sources.kusto import KustoSource
from pipetree.sources.local_file import FILE_TYPES, LocalFileSource
from pipetree.sources.sqlserver import SqlServerSource
from pipetree.sources.storage_stream import StorageStreamSource

__all__ = ["get_reader", "register_reader"]

_local_file_reader = LocalFileSource()
for _file_type in FILE_TYPES:
    register_reader(_file_type, _local_file_reader)

register_reader("sqlserver", SqlServerSource())
register_reader("kusto", KustoSource())

# d365_export and synapse_link are file-landing readers that fall out of
# the same machinery as storage_stream - thin aliases, not separate
# subsystems.
_storage_stream_reader = StorageStreamSource()
for _stream_type in ("storage_stream", "d365_export", "synapse_link"):
    register_reader(_stream_type, _storage_stream_reader)
