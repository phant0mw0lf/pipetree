"""Errors raised while loading and validating a pipetree YAML config."""

from __future__ import annotations


class ConfigError(Exception):
    """A config problem tied to a specific key path in the YAML.

    ``path`` is the dotted key path a reader would find in their YAML file
    (e.g. ``"silver.tables.orders.merge.delete_mode"``), so an error message
    always points at the offending key instead of a generic "invalid config".
    """

    def __init__(self, path: str, reason: str) -> None:
        self.path = path
        self.reason = reason
        super().__init__(f"{path}: {reason}")
