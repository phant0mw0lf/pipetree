"""The dotted-path class loader for `systems.<name>.class` - the Python
Data Source API path from part 1, for something like a Microsoft Graph
reader (`sources.graph.SharePointListSource`) that this package doesn't
ship itself."""

from __future__ import annotations

import importlib

from pipetree.sources.base import SourceReader


def load_custom_reader(dotted_path: str) -> SourceReader:
    module_path, _, class_name = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(f"{dotted_path!r} is not a valid dotted class path")

    module = importlib.import_module(module_path)

    cls = getattr(module, class_name, None)
    if cls is None:
        raise ImportError(f"{module_path!r} has no attribute {class_name!r}")

    reader = cls()
    if not isinstance(reader, SourceReader):
        raise TypeError(f"{dotted_path!r} does not implement the SourceReader protocol")

    return reader
