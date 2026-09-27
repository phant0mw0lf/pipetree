"""Raised when schema drift can't be reconciled: `schema_policy: fail` saw
any difference at all, or `schema_policy: evolve` saw a retype that isn't
a safe widening. A schema error is never retried - the executor only
retries transient errors, and a schema mismatch isn't one."""

from __future__ import annotations

from collections.abc import Iterable

from pipetree.schema.diff import SchemaChange


class SchemaError(Exception):
    def __init__(
        self, table_fqn: str, changes: Iterable[SchemaChange], reason: str = "schema drift"
    ):
        self.table_fqn = table_fqn
        self.changes = list(changes)
        detail = "; ".join(str(change) for change in self.changes)
        super().__init__(f"{table_fqn}: {reason}: {detail}")
