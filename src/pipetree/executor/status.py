"""Per-table status and the end-of-run failure digest."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class TableStatus(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRIED_SUCCEEDED = "retried→succeeded"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"


# A run only fails because something actually went wrong - a table
# deliberately left out of a --select never counts against it.
_FAILURE_STATUSES = frozenset({TableStatus.FAILED, TableStatus.UPSTREAM_FAILED})


@dataclass(frozen=True)
class TableResult:
    table_fqn: str
    status: TableStatus
    attempts: int
    started_at: int  # YYMMddHHmmssSSS UTC bigint
    ended_at: int
    duration_ms: int
    error_type: str | None = None
    error_message: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunDigest:
    execution_id: int
    results: dict[str, TableResult]

    @property
    def succeeded(self) -> bool:
        return not any(result.status in _FAILURE_STATUSES for result in self.results.values())

    @property
    def exit_code(self) -> int:
        return 0 if self.succeeded else 1
