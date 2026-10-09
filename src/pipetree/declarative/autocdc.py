"""Translates a pipetree Table into Databricks' AUTO CDC declarative flow:
`KEYS`, `SEQUENCE BY`, `APPLY AS DELETE WHEN`, `STORED AS SCD TYPE`,
`TRACK HISTORY ON * EXCEPT`. Part 1 already draws this comparison; this
makes it mechanical instead of by-hand.

A pure translation, not an executor: AUTO CDC only runs inside a Lakeflow
Declarative Pipeline (formerly DLT), where the *platform* owns the DAG -
pipetree has nothing to execute here, only something to compile to. Only
`scd1`/`scd2` tables translate; `replace`/`append` need no CDC apparatus
at all, just a plain streaming table or materialized view.

The flow reads `FROM STREAM <source>` (Databricks rejects a plain
`FROM <source>`: the source must be a streaming query), and every statement
ends with a semicolon so rendered tables can be concatenated into one
pipeline file. `delete_mode` is not modeled beyond `ignore` dropping the
delete signal entirely; AUTO CDC's own delete behavior differs by
`stored_as_scd_type`, so check it against current Databricks docs.

AUTO CDC semantics differ from pipetree's merge:

- scd2 history columns are `__START_AT`/`__END_AT`, carrying the SEQUENCE BY
  values; there are no `_valid_from/_valid_to/_is_current` and no audit
  columns.
- Duplicates of one key inside one micro-batch each become history versions
  (pipetree reduces them to one first).
- A row with an older sequence is treated by sequence, not by arrival, in
  the same micro-batch and in a later one: scd1 keeps the row with the
  higher sequence, scd2 inserts the late row as an earlier history version.
  pipetree follows the same rule when `sequence_by` is configured (its late
  scd2 version has an empty validity interval, see the pipetree README).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pipetree.model import Table

_TRANSLATABLE_STRATEGIES = {"scd1": 1, "scd2": 2}


@dataclass(frozen=True)
class AutoCdcFlow:
    target: str
    source: str
    keys: list[str]
    sequence_by: list[str]
    apply_as_delete_when: str | None
    stored_as_scd_type: int
    track_history_except_columns: list[str] = field(default_factory=list)


def translate_table(table: Table, *, source: str) -> AutoCdcFlow:
    """`source` is the upstream streaming table/view this flow reads from
    in the declarative pipeline - a pipeline-specific identifier the
    caller supplies, not something pipetree's own config names."""
    scd_type = _TRANSLATABLE_STRATEGIES.get(table.strategy)
    if scd_type is None:
        raise ValueError(
            f"{table.fqn}: AUTO CDC only applies to scd1/scd2 tables - strategy "
            f"{table.strategy!r} needs no CDC apparatus, just a plain streaming "
            "table or materialized view"
        )
    if not table.business_key:
        raise ValueError(f"{table.fqn}: AUTO CDC requires a business_key (-> KEYS)")

    if table.surrogate_key:
        raise ValueError(
            f"{table.fqn}: surrogate_key {table.surrogate_key!r} has no AUTO CDC translation "
            "here - an identity column on an AUTO CDC target isn't modeled; generate the "
            "surrogate key downstream of the flow instead"
        )

    delete_when = table.merge.delete_when if table.merge.delete_mode != "ignore" else None

    return AutoCdcFlow(
        target=table.fqn,
        source=source,
        keys=list(table.business_key),
        sequence_by=list(table.merge.sequence_by),
        apply_as_delete_when=delete_when,
        stored_as_scd_type=scd_type,
        track_history_except_columns=list(table.merge.ignore_columns),
    )


def render_sql(flow: AutoCdcFlow, *, flow_name: str | None = None) -> str:
    flow_name = flow_name or f"{flow.target.replace('.', '_')}_flow"

    lines = [
        f"CREATE OR REFRESH STREAMING TABLE {flow.target};",
        "",
        f"CREATE FLOW {flow_name} AS AUTO CDC INTO",
        f"  {flow.target}",
        f"FROM STREAM {flow.source}",
        "KEYS",
        f"  ({', '.join(flow.keys)})",
    ]

    if flow.apply_as_delete_when:
        lines += ["APPLY AS DELETE WHEN", f"  {flow.apply_as_delete_when}"]

    if flow.sequence_by:
        lines += ["SEQUENCE BY", f"  {', '.join(flow.sequence_by)}"]

    lines.append(f"STORED AS SCD TYPE {flow.stored_as_scd_type}")

    if flow.stored_as_scd_type == 2 and flow.track_history_except_columns:
        lines.append(f"TRACK HISTORY ON * EXCEPT ({', '.join(flow.track_history_except_columns)})")

    return "\n".join(lines) + ";"
