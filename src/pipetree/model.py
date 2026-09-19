"""The typed model: defaults resolved, every table given a fully qualified name.

Builds on a raw dict that has already passed ``pipetree.config.loader.validate_raw``
- this module doesn't re-check the business rules the loader already enforced,
it resolves defaults and shapes the result into objects the graph builder and
executor can use directly.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

DEFAULT_SYSTEM_COLUMNS = [
    "_inserted_at",
    "_updated_at",
    "_is_deleted",
    "_execution_id",
    "_source_system",
]

SchemaPolicy = Literal["evolve", "fail", "ignore"]
Strategy = Literal["scd2", "scd1", "replace", "append"]
DeleteMode = Literal["soft", "hard", "ignore"]

RESERVED_TOP_LEVEL_KEYS = {"defaults", "systems"}


class SecretRef(BaseModel):
    """A `{secret: name}` reference, resolved later through the Platform seam."""

    model_config = ConfigDict(frozen=True)

    secret: str


# A system/table connection property is either a literal value or a secret
# reference - the same value resolves differently per environment.
SecretOrLiteral = str | SecretRef


class Defaults(BaseModel):
    model_config = ConfigDict(frozen=True)

    system_columns: list[str] = Field(default_factory=lambda: list(DEFAULT_SYSTEM_COLUMNS))
    schema_policy: SchemaPolicy = "evolve"


class System(BaseModel):
    """A declared system. Connection properties beyond `type` vary per source
    type (host, database, landing_path, ...), so they're captured as extras
    rather than hardcoded here - `pipetree.sources` owns interpreting them."""

    model_config = ConfigDict(frozen=True, extra="allow")

    type: str


class Source(BaseModel):
    model_config = ConfigDict(frozen=True)

    system: str
    object: str


class Merge(BaseModel):
    model_config = ConfigDict(frozen=True)

    sequence_by: list[str] = Field(default_factory=list)
    delete_when: str | None = None
    delete_mode: DeleteMode = "soft"
    ignore_columns: list[str] = Field(default_factory=list)


class Table(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    layer: str
    table_schema: str
    fqn: str
    source: Source | None = None
    logic: str | None = None
    business_key: list[str] = Field(default_factory=list)
    strategy: Strategy
    merge: Merge = Field(default_factory=Merge)
    depends_on: Literal["auto"] | list[str] = Field(default_factory=list)
    unknown_member: bool = False
    schema_policy: SchemaPolicy = "evolve"


class PipelineConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    defaults: Defaults
    systems: dict[str, System]
    tables: dict[str, Table]

    @classmethod
    def from_validated_raw(cls, raw: dict[str, Any]) -> PipelineConfig:
        defaults = Defaults.model_validate(raw.get("defaults", {}))
        systems = {
            name: System.model_validate(system_raw)
            for name, system_raw in raw.get("systems", {}).items()
        }

        tables: dict[str, Table] = {}
        for layer_name, layer in raw.items():
            if layer_name in RESERVED_TOP_LEVEL_KEYS:
                continue
            for table_name, table_raw in layer["tables"].items():
                table = _build_table(layer_name, table_name, table_raw, defaults)
                tables[table.fqn] = table

        return cls(defaults=defaults, systems=systems, tables=tables)


def _build_table(
    layer_name: str, table_name: str, table_raw: dict[str, Any], defaults: Defaults
) -> Table:
    table_schema = table_raw.get("table_schema", layer_name)
    source_raw = table_raw.get("source")

    return Table(
        name=table_name,
        layer=layer_name,
        table_schema=table_schema,
        fqn=f"{table_schema}.{table_name}",
        source=Source.model_validate(source_raw) if source_raw is not None else None,
        logic=table_raw.get("logic"),
        business_key=table_raw.get("business_key", []),
        strategy=table_raw["strategy"],
        merge=Merge.model_validate(table_raw.get("merge", {})),
        depends_on=table_raw.get("depends_on", []),
        unknown_member=table_raw.get("unknown_member", False),
        schema_policy=table_raw.get("schema_policy", defaults.schema_policy),
    )
