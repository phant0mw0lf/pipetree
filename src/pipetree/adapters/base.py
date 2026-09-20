"""The adapter seam: `run_table(table)` knows nothing about the engine.

This is the whole reason one package can run on a laptop, Fabric and
Databricks: the engine-specific parts sit behind this seam as adapters.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from pipetree.model import Table


@dataclass(frozen=True)
class Capabilities:
    """What an adapter can safely do. The executor consults this rather than
    assuming - e.g. it only retries a failed `append` write if the adapter
    can make that retry idempotent."""

    supports_delete_by_execution_id: bool = False


@runtime_checkable
class Adapter(Protocol):
    capabilities: Capabilities

    def run_table(
        self, table: Table, *, execution_id: int, init: bool = False
    ) -> dict[str, Any] | None:
        """Read the source (or run the logic file) and write the table.

        `init` is the run-level "treat this as a full reload" parameter
        from part 1 - seeding or full reloads aren't something a table
        declares about itself, they're something a run asks for.

        Returns adapter-specific details (e.g. `rows_written`,
        `duplicates_dropped`) to fold into the run log, or None.
        """
        ...

    def delete_by_execution_id(self, table: Table, execution_id: int) -> None:
        """Delete rows stamped with this run's execution id.

        Only called before retrying an `append` table, and only when
        `capabilities.supports_delete_by_execution_id` is True - it's what
        makes that retry idempotent instead of duplicating rows.
        """
        ...
