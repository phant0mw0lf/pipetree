"""The Platform seam: where am I running, and what does that mean for
secrets, table names, storage paths and run metadata?

This is what lets the *same* YAML run against dev, test and prod, and the
same package run on a laptop, Fabric and Databricks: everything
environment-specific sits behind this protocol, never in the pipeline
config or the executor.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Platform(Protocol):
    name: str  # "local" | "databricks" | "fabric"

    def resolve_secret(self, name: str) -> str:
        """Resolve a `{secret: name}` reference to its actual value."""
        ...

    def qualify_table_name(self, fqn: str) -> str:
        """The package's `schema.table` fqn, turned into whatever
        identifier this platform's catalog actually needs - e.g.
        `catalog.schema.table` on Unity Catalog."""
        ...

    def resolve_path(self, relative_path: str, base_dir: Path) -> str:
        """A relative path from the config, turned into something this
        platform can actually read/write - a Volumes path, an OneLake
        `abfss://` URI, or just `base_dir / relative_path` locally."""
        ...

    def run_metadata(self) -> dict[str, str]:
        """Platform-specific context about this run (job id, run id,
        workspace, ...) - folded into the run log for traceability back
        to the job that produced it."""
        ...


def detect() -> str:
    """Best-effort runtime detection: 'databricks', 'fabric', or 'local'.

    Databricks sets `DATABRICKS_RUNTIME_VERSION` on every cluster - a
    real, documented signal. Fabric has no equivalent env var this package
    relies on; instead it checks whether `notebookutils` (which Fabric's
    runtime always makes importable) is actually available.
    """
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return "databricks"
    if importlib.util.find_spec("notebookutils") is not None:
        return "fabric"
    return "local"


def resolve_value(value: Any, platform: Platform) -> str:
    """A system/table connection property is either a literal or a
    `{secret: name}` reference - the same value resolves differently per
    environment. `value` is whatever pydantic's `extra="allow"` handed
    back (a plain str or a plain dict, never a SecretRef instance, since
    extra fields aren't coerced against a declared type)."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict) and "secret" in value:
        return platform.resolve_secret(value["secret"])
    raise TypeError(f"expected a literal string or {{'secret': name}}, got {value!r}")
