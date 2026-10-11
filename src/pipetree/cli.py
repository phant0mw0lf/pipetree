"""The pipetree CLI: `pipetree run --config pipeline.yaml`."""

from __future__ import annotations

from pathlib import Path

import click

from pipetree import __version__, run_pipeline
from pipetree.config.errors import ConfigError
from pipetree.config.loader import load_config
from pipetree.graph.builder import build_graph
from pipetree.graph.errors import GraphError
from pipetree.graph.render import render_graph
from pipetree.graph.select import count_table_tags, resolve_run_set, split_names
from pipetree.model import PipelineConfig

_CONFIG_OPTION = click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to the pipeline YAML config.",
)


def _selection_options(command):
    """The options that decide which tables a run (or `graph`) covers."""
    options = [
        click.option(
            "--select",
            "select_arg",
            default=None,
            help="Comma-separated table names to run (fqn or unambiguous bare name). "
            "Everything else is left alone.",
        ),
        click.option(
            "--tag",
            "tag_args",
            multiple=True,
            help="Run only tables carrying this tag. Repeat for several: a table needs at "
            "least one. Intersects with --select; --with-dependents and --with-ancestors "
            "extend the result.",
        ),
        click.option(
            "--exclude-tag",
            "exclude_tag_args",
            multiple=True,
            help="Never run tables carrying this tag, even when --with-dependents or "
            "--with-ancestors pulls them in. Repeatable.",
        ),
        click.option(
            "--with-dependents",
            is_flag=True,
            default=False,
            help="Extend the selection to every table downstream of it (the CI/CD subtree mode).",
        ),
        click.option(
            "--with-ancestors",
            is_flag=True,
            default=False,
            help="Extend the selection to every table upstream of it (what it transitively reads).",
        ),
    ]
    for option in reversed(options):
        command = option(command)
    return command


def _selection(
    select_arg: str | None, tag_args: tuple[str, ...], exclude_tag_args: tuple[str, ...]
) -> tuple[list[str] | None, list[str] | None, list[str] | None]:
    return (
        split_names(select_arg) or None,
        split_names(tag_args) or None,
        split_names(exclude_tag_args) or None,
    )


@click.group()
@click.version_option(version=__version__, prog_name="pipetree")
def main() -> None:
    """pipetree - metadata-driven pipeline orchestration."""


@main.command()
@_CONFIG_OPTION
@click.option(
    "--max-workers",
    default=4,
    show_default=True,
    type=int,
    help="Maximum number of tables running at the same time.",
)
@_selection_options
@click.option(
    "--init",
    is_flag=True,
    default=False,
    help="Full reload: treat every run table as if seeding fresh.",
)
@click.option(
    "--live-html",
    "live_html",
    default=None,
    type=click.Path(dir_okay=False, path_type=Path),
    help="Keep an HTML picture of the dependency tree at this path, rewritten as tables "
    "run and finish. Open it in a browser: it refreshes itself until the run ends.",
)
def run(
    config_path: Path,
    max_workers: int,
    select_arg: str | None,
    tag_args: tuple[str, ...],
    exclude_tag_args: tuple[str, ...],
    with_dependents: bool,
    with_ancestors: bool,
    init: bool,
    live_html: Path | None,
) -> None:
    """Run the pipeline described by --config."""
    select, tags, exclude_tags = _selection(select_arg, tag_args, exclude_tag_args)

    observer = None
    if live_html is not None:
        from pipetree.notebook import HtmlFileObserver

        observer = HtmlFileObserver(live_html, title=config_path.name)
        click.echo(f"live view: {live_html}", err=True)

    try:
        digest = run_pipeline(
            config_path,
            max_workers=max_workers,
            select=select,
            tags=tags,
            exclude_tags=exclude_tags,
            with_dependents=with_dependents,
            with_ancestors=with_ancestors,
            init=init,
            observer=observer,
        )
    except (ConfigError, GraphError) as exc:
        raise click.ClickException(str(exc)) from exc

    raise SystemExit(digest.exit_code)


@main.command()
@_CONFIG_OPTION
@click.option(
    "--list-tags",
    is_flag=True,
    default=False,
    help="Print every tag with the number of tables carrying it, instead of the OK line.",
)
def validate(config_path: Path, list_tags: bool) -> None:
    """Validate --config without running anything."""
    try:
        raw = load_config(config_path)
        config = PipelineConfig.from_validated_raw(raw)
    except ConfigError as exc:
        raise click.ClickException(str(exc)) from exc

    if not list_tags:
        click.echo(f"{config_path}: OK")
        return

    counts = count_table_tags(config.tables.values())
    if not counts:
        click.echo("no tags declared")
    width = max(map(len, counts), default=0)
    for tag, count in sorted(counts.items()):
        click.echo(f"{tag:<{width}}  {count}")


@main.command()
@_CONFIG_OPTION
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["text", "mermaid", "html"]),
    default="text",
    show_default=True,
    help="html: one self-contained page (inline CSS + SVG) with the tree laid out in "
    "waves - redirect it to a file and open it in a browser.",
)
@_selection_options
def graph(
    config_path: Path,
    fmt: str,
    select_arg: str | None,
    tag_args: tuple[str, ...],
    exclude_tag_args: tuple[str, ...],
    with_dependents: bool,
    with_ancestors: bool,
) -> None:
    """Print the dependency tree derived from --config.

    With --select, --tag or --exclude-tag, the tables a `run` with the same
    options would not touch are marked as not selected.
    """
    select, tags, exclude_tags = _selection(select_arg, tag_args, exclude_tag_args)
    try:
        raw = load_config(config_path)
        config = PipelineConfig.from_validated_raw(raw)
        built = build_graph(config, base_dir=config_path.parent)
        run_set = resolve_run_set(
            built,
            select=select,
            tags=tags,
            exclude_tags=exclude_tags,
            with_dependents=with_dependents,
            with_ancestors=with_ancestors,
        )
    except (ConfigError, GraphError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(render_graph(built, fmt=fmt, run_set=run_set))
