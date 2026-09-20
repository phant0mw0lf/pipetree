"""The pipetree CLI: `pipetree run --config pipeline.yaml`."""

from __future__ import annotations

from pathlib import Path

import click

from pipetree import __version__, run_pipeline
from pipetree.config.errors import ConfigError
from pipetree.config.loader import load_config
from pipetree.graph.errors import GraphError
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
@click.option("--max-workers", default=4, show_default=True, type=int)
def run(config_path: Path, max_workers: int) -> None:
    """Run the pipeline described by --config."""
    try:
        digest = run_pipeline(config_path, max_workers=max_workers)
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
