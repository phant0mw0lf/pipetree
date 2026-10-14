"""The typed model: defaults resolved, every table given a fully qualified name.

Builds on a raw dict that has already passed ``pipetree.config.loader.validate_raw``
- this module doesn't re-check the business rules the loader already enforced,
it resolves defaults and shapes the result into objects the graph builder and
executor can use directly.
"""

from __future__ import annotations

import re
from typing import Annotated, Any, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

DEFAULT_SYSTEM_COLUMNS = [
    "_inserted_at",
    "_updated_at",
    "_is_deleted",
    "_execution_id",
    "_source_system",
]

# pipetree's own stamps on every table (+ the scd2 validity columns), and
# the internal per-row delete marker: never a user column name.
RESERVED_AUDIT_COLUMNS = frozenset(
    {
        "_inserted_at",
        "_updated_at",
        "_is_deleted",
        "_execution_id",
        "_source_system",
        "_valid_from",
        "_valid_to",
        "_is_current",
    }
)
DELETE_MARKER_COLUMN = "__pipetree_is_delete__"

SchemaPolicy = Literal["evolve", "fail", "ignore"]
Strategy = Literal["scd2", "scd1", "replace", "append"]
DeleteMode = Literal["soft", "hard", "ignore"]

RESERVED_TOP_LEVEL_KEYS = {"defaults", "systems", "notifications"}

# A tag: lowercase letters, digits, `-` and `_`; starts with a letter or digit.
TAG_MAX_LENGTH = 40
TAG_PATTERN = re.compile(rf"^[a-z0-9][a-z0-9_-]{{0,{TAG_MAX_LENGTH - 1}}}$")


class SecretRef(BaseModel):
    """A `{secret: name}` reference, resolved later through the Platform seam."""

    model_config = ConfigDict(frozen=True)

    secret: str = Field(description="Name of the secret, resolved by the platform at run time.")


# A system/table connection property is either a literal value or a secret
# reference - the same value resolves differently per environment.
SecretOrLiteral = str | SecretRef


_ADDRESS = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")


def _check_address(value: str) -> str:
    if not _ADDRESS.fullmatch(value):
        raise ValueError(f"{value!r} is not an email address")
    return value


EmailAddress = Annotated[str, AfterValidator(_check_address)]


class EmailNotification(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    endpoint: SecretOrLiteral = Field(
        description="Endpoint of the Azure Communication Services resource, "
        "`https://<resource>.communication.azure.com`. A literal or `{secret: name}`."
    )
    sender: EmailAddress = Field(
        description="Sender address of a domain connected to the resource, "
        "for example `DoNotReply@<guid>.azurecomm.net`."
    )
    to: list[EmailAddress] = Field(
        min_length=1,
        description="Recipient addresses. At least one.",
    )

    @model_validator(mode="before")
    @classmethod
    def _yaml_on_is_a_boolean(cls, data: Any) -> Any:
        # YAML 1.1 (PyYAML) reads an unquoted `on:` key as the boolean True.
        if isinstance(data, dict) and True in data and "on" not in data:
            data = {("on" if key is True else key): value for key, value in data.items()}
        return data

    on: Literal["failure"] = Field(
        default="failure",
        description="When to send. `failure` mails a run in which a table failed or "
        "was marked upstream_failed. No mail for a successful run.",
    )


class Notifications(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    email: EmailNotification | None = Field(
        default=None,
        description="Email a failed run through Azure Communication Services, "
        "authenticated with a token credential (no connection string).",
    )


class Defaults(BaseModel):
    model_config = ConfigDict(frozen=True)

    system_columns: list[str] = Field(
        default=DEFAULT_SYSTEM_COLUMNS,
        description="Names of the audit columns. Read from the config but not used to "
        "select columns: pipetree stamps its fixed audit set on every table.",
    )
    schema_policy: SchemaPolicy = Field(
        default="evolve",
        description="Schema drift policy for every table that does not set its own.",
    )


class System(BaseModel):
    """A declared system. Connection properties beyond `type` vary per source
    type (host, database, landing_path, ...), so they're captured as extras
    rather than hardcoded here - `pipetree.sources` owns interpreting them."""

    model_config = ConfigDict(frozen=True, extra="allow")

    type: str = Field(
        description="Source type: csv, json, parquet, sqlserver, kusto, storage_stream, "
        "d365_export, synapse_link or custom. Other keys are passed to the source reader."
    )


class Source(BaseModel):
    model_config = ConfigDict(frozen=True)

    system: str = Field(description="Name of a system declared under `systems`.")
    object: str = Field(
        description="What to read from the system: a table, entity or file name the reader "
        "understands."
    )


class Merge(BaseModel):
    model_config = ConfigDict(frozen=True)

    sequence_by: list[str] = Field(
        default_factory=list,
        description="Source columns that order the versions of a key. The highest value wins.",
    )
    delete_when: str | None = Field(
        default=None,
        description="Spark SQL expression, true for source rows that are deletes.",
    )
    delete_mode: DeleteMode = Field(
        default="soft",
        description="What a delete row does: soft marks the row deleted, hard removes it "
        "(scd1 only), ignore drops delete rows from the batch.",
    )
    ignore_columns: list[str] = Field(
        default_factory=list,
        description="Columns whose changes alone are written in place: no new scd2 version "
        "and no audit column update.",
    )
    snapshot: bool = Field(
        default=False,
        description="The batch is the complete current state of the source (scd1 and scd2 "
        "only). An active key of the table that is not in the batch is handled like a delete "
        "row, following delete_mode.",
    )
    snapshot_max_missing_ratio: float = Field(
        default=0.5,
        ge=0,
        le=1,
        description="With snapshot: the table fails and nothing is written when more than "
        "this share of its active keys is missing from the batch.",
    )
    snapshot_allow_empty: bool = Field(
        default=False,
        description="With snapshot: an empty batch is an error unless this is true (then every "
        "active key counts as missing, subject to snapshot_max_missing_ratio).",
    )


class Table(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str = Field(description="The table's key under `tables`. Set by the loader.")
    layer: str = Field(description="The layer the table is declared under. Set by the loader.")
    table_schema: str = Field(
        description="Schema the table is created in. Defaults to the layer name."
    )
    fqn: str = Field(description="`table_schema.name`. Set by the loader.")
    source: Source | None = Field(
        default=None,
        description="Read from a declared system. Exactly one of `source` and `logic`.",
    )
    logic: str | None = Field(
        default=None,
        description="Path to a .sql or .py file, relative to the config. "
        "Exactly one of `source` and `logic`.",
    )
    business_key: list[str] = Field(
        default_factory=list,
        description="Columns that identify a row. Required for scd1, scd2 and unknown_member.",
    )
    strategy: Strategy = Field(description="How the result is written to the table.")
    merge: Merge = Field(
        default_factory=Merge,
        description="Options for the scd1 and scd2 strategies.",
    )
    depends_on: Literal["auto"] | list[str] = Field(
        default_factory=list,
        description="`auto` reads the logic file; a list names parent tables "
        "(fqn or unambiguous bare name).",
    )
    unknown_member: bool = Field(
        default=False,
        description="Seed one row with a sentinel business key for facts to resolve "
        "a missing key to.",
    )
    schema_policy: SchemaPolicy = Field(
        default="evolve",
        description="Schema drift policy for this table. Defaults to `defaults.schema_policy`.",
    )
    # An identity column (Delta GENERATED BY DEFAULT AS IDENTITY) on an
    # scd1/scd2 dimension; the unknown member gets -1.
    surrogate_key: str | None = Field(
        default=None,
        description="Name of a generated integer key column (scd1 and scd2 only). "
        "The unknown member gets -1.",
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Labels for selecting tables with `pipetree run --tag`. Lowercase letters, "
        "digits, `-` and `_`, starting with a letter or digit, at most 40 characters, "
        "no duplicates within a table.",
    )

    @model_validator(mode="after")
    def _merge_options_fit_the_strategy(self) -> Table:
        if self.merge.snapshot and self.strategy not in ("scd1", "scd2"):
            raise ValueError("snapshot is only available for the scd1 and scd2 strategies")
        if self.strategy == "scd2" and self.merge.delete_mode == "hard":
            raise ValueError(
                "scd2 cannot be combined with delete_mode: hard (it would destroy history)"
            )
        return self


class PipelineConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    defaults: Defaults = Field(description="Resolved defaults.")
    systems: dict[str, System] = Field(description="Declared systems by name.")
    tables: dict[str, Table] = Field(description="Every table by fully qualified name.")
    notifications: Notifications | None = Field(
        default=None, description="Optional notification settings."
    )

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

        notifications_raw = raw.get("notifications")
        notifications = (
            Notifications.model_validate(notifications_raw)
            if notifications_raw is not None
            else None
        )
        return cls(defaults=defaults, systems=systems, tables=tables, notifications=notifications)


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
        surrogate_key=table_raw.get("surrogate_key"),
        tags=table_raw.get("tags", []),
    )
