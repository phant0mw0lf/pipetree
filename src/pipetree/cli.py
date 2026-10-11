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
from pipetree.model import PipelineConfig

_CONFIG_OPTION = click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to the pipeline YAML config.",
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
@click.option(
    "--select",
    "select_arg",
    default=None,
    help="Comma-separated table names to run (fqn or unambiguous bare name). "
    "Everything else is left alone.",
)
@click.option(
    "--with-dependents",
    is_flag=True,
    default=False,
    help="Extend --select to every table downstream of it (the CI/CD subtree mode).",
)
@click.option(
    "--with-ancestors",
    is_flag=True,
    default=False,
    help="Extend --select to every table upstream of it (what it transitively reads).",
)
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
    with_dependents: bool,
    with_ancestors: bool,
    init: bool,
    live_html: Path | None,
) -> None:
    """Run the pipeline described by --config."""
    select = [name.strip() for name in select_arg.split(",")] if select_arg else None

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
def validate(config_path: Path) -> None:
    """Validate --config without running anything."""
    try:
        raw = load_config(config_path)
        PipelineConfig.from_validated_raw(raw)
    except ConfigError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"{config_path}: OK")


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
def graph(config_path: Path, fmt: str) -> None:
    """Print the dependency tree derived from --config."""
    try:
        raw = load_config(config_path)
        config = PipelineConfig.from_validated_raw(raw)
        built = build_graph(config, base_dir=config_path.parent)
    except (ConfigError, GraphError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(render_graph(built, fmt=fmt))
