"""Reads and validates a pipetree YAML config.

This module answers one question: is the raw YAML shaped the way the
package expects, and does it obey the rules from part 1 of the blog post?
It fails fast, with an error naming the offending key path. Defaults are
*not* resolved here (that belongs to the typed model in ``pipetree.model``)
- this module only rejects config that is structurally wrong or breaks an
explicit rule.
"""

from __future__ import annotations

import difflib
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from pipetree.config.errors import ConfigError
from pipetree.model import (
    DELETE_MARKER_COLUMN,
    RESERVED_AUDIT_COLUMNS,
    TAG_MAX_LENGTH,
    TAG_PATTERN,
    Merge,
    Notifications,
    Source,
    Table,
)

RESERVED_TOP_LEVEL_KEYS = {"defaults", "systems", "notifications"}
# The keys a YAML table, its `merge` and its `source` may carry, taken from the
# models so they cannot drift. `name`, `layer` and `fqn` are set by the loader.
TABLE_KEYS = frozenset(Table.model_fields) - {"name", "layer", "fqn"}
MERGE_KEYS = frozenset(Merge.model_fields)
SOURCE_KEYS = frozenset(Source.model_fields)
# Order matches the post's schema (`scd2 | scd1 | replace | append`,
# `soft | hard | ignore`), not alphabetical, so error messages read the way
# the docs do.
VALID_STRATEGIES = ("scd2", "scd1", "replace", "append")
VALID_DELETE_MODES = ("soft", "hard", "ignore")


def load_config(path: str | Path) -> dict[str, Any]:
    """Read and validate the YAML file at ``path``, returning the raw dict."""
    text = Path(path).read_text()

    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(path="<yaml>", reason=f"invalid YAML syntax: {exc}") from exc

    validate_raw(raw)
    return raw


def validate_raw(raw: Any) -> None:
    """Validate an already-parsed config dict. Raises ``ConfigError``."""
    if not isinstance(raw, dict):
        raise ConfigError(path="<root>", reason="config must be a mapping")

    systems = raw.get("systems", {})
    _validate_systems(systems)
    _validate_notifications(raw.get("notifications"))

    for layer_name, layer in raw.items():
        if layer_name in RESERVED_TOP_LEVEL_KEYS:
            continue
        _validate_layer(layer_name, layer, systems)


def _validate_notifications(block: Any) -> None:
    if block is None:
        return
    try:
        Notifications.model_validate(block)
    except ValidationError as exc:
        first = exc.errors()[0]
        location = ".".join(["notifications", *(str(part) for part in first["loc"])])
        raise ConfigError(path=location, reason=first["msg"]) from exc


def _validate_systems(systems: Any) -> None:
    if not isinstance(systems, dict):
        raise ConfigError(path="systems", reason="must be a mapping of system name to config")

    for name, system in systems.items():
        path = f"systems.{name}"
        if not isinstance(system, dict) or "type" not in system:
            raise ConfigError(path=path, reason="must be a mapping with a 'type' key")


def _validate_layer(layer_name: str, layer: Any, systems: dict[str, Any]) -> None:
    if not isinstance(layer, dict) or "tables" not in layer:
        raise ConfigError(path=layer_name, reason="a layer must be a mapping with a 'tables' key")

    tables = layer["tables"]
    if not isinstance(tables, dict):
        raise ConfigError(
            path=f"{layer_name}.tables",
            reason="must be a mapping of table name to table config",
        )

    for table_name, table in tables.items():
        _validate_table(f"{layer_name}.tables.{table_name}", table, systems)


def _validate_table(path: str, table: Any, systems: dict[str, Any]) -> None:
    if not isinstance(table, dict):
        raise ConfigError(path=path, reason="must be a mapping")
    _reject_unknown_keys(path, table, TABLE_KEYS)

    has_source = "source" in table
    has_logic = "logic" in table
    if has_source and has_logic:
        raise ConfigError(
            path=path, reason="has both 'source' and 'logic'; a table may have only one"
        )
    if not has_source and not has_logic:
        raise ConfigError(path=path, reason="must have either 'source' or 'logic'")

    if has_source:
        _validate_source(f"{path}.source", table["source"], systems)

    strategy = table.get("strategy")
    if strategy not in VALID_STRATEGIES:
        raise ConfigError(
            path=f"{path}.strategy",
            reason=f"{strategy!r} is not one of {'|'.join(VALID_STRATEGIES)}",
        )

    merge = table.get("merge")
    if merge is not None:
        _validate_merge(f"{path}.merge", merge, strategy)

    depends_on = table.get("depends_on")
    if depends_on is not None and depends_on != "auto" and not _is_str_list(depends_on):
        raise ConfigError(
            path=f"{path}.depends_on",
            reason="must be 'auto' or a list of table names",
        )

    unknown_member = table.get("unknown_member")
    if unknown_member and not table.get("business_key"):
        raise ConfigError(path=f"{path}.unknown_member", reason="requires 'business_key' to be set")

    if "surrogate_key" in table:
        _validate_surrogate_key(f"{path}.surrogate_key", table, strategy)

    if "tags" in table:
        _validate_tags(f"{path}.tags", table["tags"])

    # After unknown_member and surrogate_key, which name their own key when the
    # business key is missing.
    if strategy in ("scd1", "scd2") and not table.get("business_key"):
        raise ConfigError(path=f"{path}.business_key", reason="is required for scd1 and scd2")


def _reject_unknown_keys(path: str, block: dict[Any, Any], allowed: Iterable[str]) -> None:
    names = sorted(allowed)
    for key in block:
        if key in names:
            continue
        close = difflib.get_close_matches(str(key), names, n=1)
        hint = f"did you mean {close[0]!r}?" if close else f"allowed keys: {', '.join(names)}"
        raise ConfigError(path=f"{path}.{key}", reason=f"unknown key {key!r}; {hint}")


def _validate_tags(path: str, tags: Any) -> None:
    if not isinstance(tags, list):
        raise ConfigError(path=path, reason="must be a list of tag names")
    seen: set[str] = set()
    for i, tag in enumerate(tags):
        if not isinstance(tag, str):
            raise ConfigError(path=f"{path}[{i}]", reason=f"must be a string, not {tag!r}")
        if not TAG_PATTERN.fullmatch(tag):
            raise ConfigError(
                path=f"{path}[{i}]",
                reason=(
                    f"{tag!r} is not a valid tag: use lowercase letters, digits, '-' and '_', "
                    f"starting with a letter or digit, at most {TAG_MAX_LENGTH} characters"
                ),
            )
        if tag in seen:
            raise ConfigError(path=f"{path}[{i}]", reason=f"duplicate tag {tag!r}")
        seen.add(tag)


def _validate_surrogate_key(path: str, table: dict[str, Any], strategy: str) -> None:
    name = table["surrogate_key"]
    if not isinstance(name, str) or not name:
        raise ConfigError(path=path, reason="must be a non-empty column name")
    if strategy not in ("scd1", "scd2"):
        raise ConfigError(
            path=path,
            reason=(
                f"is only valid for scd1/scd2, not {strategy!r} (replace rewrites the "
                "table and would renumber every key; append has no business key)"
            ),
        )
    business_key = table.get("business_key") or []
    if not business_key:
        raise ConfigError(path=path, reason="requires 'business_key' to be set")
    if name in business_key:
        raise ConfigError(path=path, reason=f"{name!r} collides with business key column")
    if name in RESERVED_AUDIT_COLUMNS or name == DELETE_MARKER_COLUMN:
        raise ConfigError(path=path, reason=f"{name!r} is a reserved pipetree column")


def _validate_source(path: str, source: Any, systems: dict[str, Any]) -> None:
    if not isinstance(source, dict) or "system" not in source or "object" not in source:
        raise ConfigError(path=path, reason="must have 'system' and 'object'")
    _reject_unknown_keys(path, source, SOURCE_KEYS)

    system_name = source["system"]
    if system_name not in systems:
        raise ConfigError(
            path=f"{path}.system",
            reason=f"system {system_name!r} is not declared in 'systems'",
        )


def _validate_merge(path: str, merge: Any, strategy: str | None) -> None:
    if not isinstance(merge, dict):
        raise ConfigError(path=path, reason="must be a mapping")
    _reject_unknown_keys(path, merge, MERGE_KEYS)

    delete_mode = merge.get("delete_mode", "soft")
    if delete_mode not in VALID_DELETE_MODES:
        raise ConfigError(
            path=f"{path}.delete_mode",
            reason=f"{delete_mode!r} is not one of {'|'.join(VALID_DELETE_MODES)}",
        )
    if strategy == "scd2" and delete_mode == "hard":
        raise ConfigError(
            path=f"{path}.delete_mode",
            reason="scd2 cannot be combined with delete_mode: hard (it would destroy history)",
        )

    snapshot = merge.get("snapshot", False)
    if not isinstance(snapshot, bool):
        raise ConfigError(path=f"{path}.snapshot", reason="must be true or false")
    if snapshot and strategy not in ("scd1", "scd2"):
        raise ConfigError(
            path=f"{path}.snapshot", reason="is only available for the scd1 and scd2 strategies"
        )
    for key in ("snapshot_max_missing_ratio", "snapshot_allow_empty"):
        if key in merge and not snapshot:
            raise ConfigError(path=f"{path}.{key}", reason="needs snapshot: true")
    ratio = merge.get("snapshot_max_missing_ratio")
    if ratio is not None and (
        isinstance(ratio, bool) or not isinstance(ratio, int | float) or not 0 <= ratio <= 1
    ):
        raise ConfigError(
            path=f"{path}.snapshot_max_missing_ratio", reason="must be a number from 0 to 1"
        )
    allow_empty = merge.get("snapshot_allow_empty")
    if allow_empty is not None and not isinstance(allow_empty, bool):
        raise ConfigError(path=f"{path}.snapshot_allow_empty", reason="must be true or false")


def _is_str_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)
