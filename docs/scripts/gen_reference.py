"""Generate the reference pages and the JSON Schema from the code.

    uv run python docs/scripts/gen_reference.py

Writes (all generated, never edit by hand):

- docs/src/content/docs/reference/yaml.mdx   field tables from the pydantic models
- docs/src/content/docs/reference/cli.mdx    options from the click commands
- docs/public/pipetree.schema.json           JSON Schema for editor completion

`render_all()` returns the file contents as a dict; tests/test_docs_reference.py
compares it with the committed files.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click
from pydantic.json_schema import models_json_schema

from pipetree import __version__
from pipetree.cli import main
from pipetree.model import Defaults, Merge, Source, System, Table

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs" / "src" / "content" / "docs"
OUTPUTS = {
    "yaml": DOCS / "reference" / "yaml.mdx",
    "cli": DOCS / "reference" / "cli.mdx",
    "schema": ROOT / "docs" / "public" / "pipetree.schema.json",
}

# Table fields the loader sets itself; they are not part of the YAML a user writes.
_DERIVED_TABLE_FIELDS = ("name", "layer", "fqn")


# ---------------------------------------------------------------- JSON Schema


def build_schema() -> dict[str, Any]:
    """The schema of the YAML file: defaults, systems, and any number of layers."""
    _, defs_schema = models_json_schema(
        [(m, "validation") for m in (Defaults, System, Source, Merge, Table)],
        ref_template="#/$defs/{model}",
    )
    defs: dict[str, Any] = defs_schema["$defs"]

    table = defs["Table"]
    for name in _DERIVED_TABLE_FIELDS:
        table["properties"].pop(name)
    table["required"] = ["strategy"]
    table["title"] = "Table"
    table["description"] = "One table. Exactly one of `source` and `logic`."
    table["oneOf"] = [{"required": ["source"]}, {"required": ["logic"]}]
    # The effective default comes from `defaults.schema_policy`, not a fixed value.
    table["properties"]["schema_policy"].pop("default", None)
    table["properties"]["table_schema"]["description"] = (
        "Schema the table is created in. Defaults to the layer name."
    )
    table["properties"]["table_schema"].pop("default", None)

    defs["Layer"] = {
        "title": "Layer",
        "description": "A layer: a namespace for tables, not an execution barrier.",
        "type": "object",
        "properties": {
            "tables": {
                "type": "object",
                "description": "Tables by name.",
                "additionalProperties": {"$ref": "#/$defs/Table"},
            }
        },
        "required": ["tables"],
    }
    defs["System"]["required"] = ["type"]

    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "pipetree pipeline",
        "description": "A pipetree pipeline config. Every top-level key except `defaults` "
        "and `systems` is a layer.",
        "type": "object",
        "properties": {
            "defaults": {"$ref": "#/$defs/Defaults"},
            "systems": {
                "type": "object",
                "description": "Systems by name. Tables reference them in `source.system`.",
                "additionalProperties": {"$ref": "#/$defs/System"},
            },
        },
        "additionalProperties": {"$ref": "#/$defs/Layer"},
        "$defs": dict(sorted(defs.items())),
    }


# ----------------------------------------------------------------- formatting


def _escape(text: str) -> str:
    """Make text safe for MDX: escape braces, angle brackets and pipes outside code spans."""
    parts = text.split("`")
    for i in range(0, len(parts), 2):
        parts[i] = (
            parts[i]
            .replace("{", "\\{")
            .replace("}", "\\}")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
    return "`".join(parts).replace("|", "\\|")


def _type_of(schema: dict[str, Any]) -> str:
    if "$ref" in schema:
        return schema["$ref"].rsplit("/", 1)[-1]
    if "const" in schema:
        return json.dumps(schema["const"])
    if "enum" in schema:
        return " \\| ".join(json.dumps(v) for v in schema["enum"])
    for key in ("anyOf", "oneOf"):
        if key in schema:
            options = [_type_of(s) for s in schema[key] if s.get("type") != "null"]
            nullable = any(s.get("type") == "null" for s in schema[key])
            text = " \\| ".join(options)
            return f"{text} \\| null" if nullable else text
    kind = schema.get("type")
    if kind == "array":
        return f"list[{_type_of(schema.get('items', {}))}]"
    if kind == "object":
        extra = schema.get("additionalProperties")
        if isinstance(extra, dict):
            return f"map[string, {_type_of(extra)}]"
        return "object"
    return str(kind or "any")


def _default_of(schema: dict[str, Any], required: bool) -> str:
    if required:
        return "required"
    if "default" not in schema:
        # A default_factory list does not appear in the schema.
        is_list = schema.get("type") == "array" or any(
            s.get("type") == "array" for s in schema.get("anyOf", [])
        )
        return "`[]`" if is_list else "-"
    value = schema["default"]
    if value == []:
        return "`[]`"
    if value is None:
        return "`null`"
    return f"`{json.dumps(value)}`"


def _field_table(model: dict[str, Any], *, extra_rows: list[str] | None = None) -> str:
    required = set(model.get("required", []))
    rows = ["| Field | Type | Default | Description |", "| --- | --- | --- | --- |"]
    for name, prop in model["properties"].items():
        description = _escape(prop.get("description", ""))
        row = (
            f"| `{name}` | {_type_of(prop)} | {_default_of(prop, name in required)} "
            f"| {description} |"
        )
        rows.append(row)
    rows.extend(extra_rows or [])
    return "\n".join(rows)


def render_yaml(schema: dict[str, Any]) -> str:
    defs = schema["$defs"]

    def section(title: str, model: str, intro: str, extra: list[str] | None = None) -> str:
        return f"## {title}\n\n{intro}\n\n{_field_table(defs[model], extra_rows=extra)}\n"

    extra_system = [
        "| any other key | string \\| `{secret: name}` | - "
        "| Passed to the source reader, for example `path`, `host` or `class`. "
        "A `{secret: name}` value is resolved by the platform at run time. |"
    ]
    parts = [
        "---",
        "title: YAML reference",
        "description: Every key of the pipeline YAML, generated from the pydantic models.",
        "sidebar:",
        "  order: 1",
        "---",
        "",
        "{/* Generated by docs/scripts/gen_reference.py. Do not edit. */}",
        "",
        "Generated from `src/pipetree/model.py`. For editor completion, "
        "use the [JSON Schema](../../pipetree.schema.json).",
        "",
        "## Top level\n",
        "| Key | Type | Description |",
        "| --- | --- | --- |",
        "| `defaults` | Defaults | Optional. Values for every table. |",
        "| `systems` | map[string, System] | Declared systems. |",
        "| any other key | Layer | A layer with a `tables` map of name to Table. "
        "The layer name is the default schema of its tables. |",
        "",
        section("Defaults", "Defaults", "Under `defaults`."),
        section(
            "System",
            "System",
            "Under `systems.<name>`. Which other keys apply depends on `type`.",
            extra_system,
        ),
        section("Table", "Table", "Under `<layer>.tables.<name>`."),
        section("Source", "Source", "Under a table's `source`."),
        section(
            "Merge",
            "Merge",
            "Under a table's `merge`. See [Merge options](../../merge/merge-options/).",
        ),
    ]
    return "\n".join(parts).rstrip() + "\n"


# ------------------------------------------------------------------------ CLI


def _option_rows(cmd: click.Command) -> list[str]:
    rows = ["| Option | Default | Description |", "| --- | --- | --- |"]
    for param in cmd.params:
        if not isinstance(param, click.Option):
            continue
        names = ", ".join(f"`{n}`" for n in param.opts)
        if param.metavar or not param.is_flag:
            metavar = param.metavar or (
                "|".join(param.type.choices)
                if isinstance(param.type, click.Choice)
                else param.type.name.upper()
            )
            names += " `" + metavar.replace("|", "\\|") + "`"
        if param.required:
            default = "required"
        elif param.is_flag or param.default is None:
            default = "-" if not param.is_flag else "off"
        else:
            default = f"`{param.default}`"
        help_text = _escape((param.help or "").replace("\n", " "))
        rows.append(f"| {names} | {default} | {help_text} |")
    return rows


def _help_text(cmd: click.Command, name: str) -> str:
    ctx = click.Context(cmd, info_name=name, terminal_width=80, max_content_width=80)
    return cmd.get_help(ctx)


def render_cli() -> str:
    assert isinstance(main, click.Group)
    parts = [
        "---",
        "title: CLI reference",
        "description: The pipetree command and its subcommands, generated from the click commands.",
        "sidebar:",
        "  order: 2",
        "---",
        "",
        "{/* Generated by docs/scripts/gen_reference.py. Do not edit. */}",
        "",
        f"Generated from `src/pipetree/cli.py` (pipetree {__version__}).",
        "",
        "```text",
        _help_text(main, "pipetree"),
        "```",
        "",
        "`pipetree --version` prints the version.",
    ]
    for name in sorted(main.commands):
        cmd = main.commands[name]
        usage = " ".join(["pipetree", name, *cmd.collect_usage_pieces(click.Context(cmd))])
        parts += [
            "",
            f"## pipetree {name}",
            "",
            (cmd.help or "").strip(),
            "",
            "```text",
            usage,
            "```",
            "",
            *_option_rows(cmd),
        ]
    return "\n".join(parts) + "\n"


# --------------------------------------------------------------------- driver


def render_all() -> dict[str, str]:
    schema = build_schema()
    return {
        "yaml": render_yaml(schema),
        "cli": render_cli(),
        "schema": json.dumps(schema, indent=2) + "\n",
    }


def main_script() -> None:
    for key, text in render_all().items():
        path = OUTPUTS[key]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        print(f"wrote {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main_script()
