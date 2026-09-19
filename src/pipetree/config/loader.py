"""Reads and validates a pipetree YAML config.

This module answers one question: is the raw YAML shaped the way the
package expects, and does it obey the rules from part 1 of the blog post?
It fails fast, with an error naming the offending key path. Defaults are
*not* resolved here (that belongs to the typed model in ``pipetree.model``)
- this module only rejects config that is structurally wrong or breaks an
explicit rule.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from pipetree.config.errors import ConfigError

RESERVED_TOP_LEVEL_KEYS = {"defaults", "systems"}
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

    for layer_name, layer in raw.items():
        if layer_name in RESERVED_TOP_LEVEL_KEYS:
            continue
        _validate_layer(layer_name, layer, systems)


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


def _validate_source(path: str, source: Any, systems: dict[str, Any]) -> None:
    if not isinstance(source, dict) or "system" not in source or "object" not in source:
        raise ConfigError(path=path, reason="must have 'system' and 'object'")

    system_name = source["system"]
    if system_name not in systems:
        raise ConfigError(
            path=f"{path}.system",
            reason=f"system {system_name!r} is not declared in 'systems'",
        )


def _validate_merge(path: str, merge: Any, strategy: str | None) -> None:
    if not isinstance(merge, dict):
        raise ConfigError(path=path, reason="must be a mapping")

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


def _is_str_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)
