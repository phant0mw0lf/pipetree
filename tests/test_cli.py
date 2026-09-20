from pathlib import Path

from click.testing import CliRunner

from pipetree.cli import main

VALID_CONFIG = """
systems:
  crm:
    type: synapse_link

bronze:
  tables:
    customer:
      source:
        system: crm
        object: account
      business_key: [id]
      strategy: scd1
"""

INVALID_CONFIG = """
bronze:
  tables:
    customer:
      strategy: bogus
"""


def write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "pipeline.yaml"
    path.write_text(text)
    return path


def test_validate_reports_ok_for_a_valid_config(tmp_path: Path):
    config_path = write(tmp_path, VALID_CONFIG)

    result = CliRunner().invoke(main, ["validate", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "OK" in result.output


def test_validate_reports_the_error_for_an_invalid_config(tmp_path: Path):
    config_path = write(tmp_path, INVALID_CONFIG)

    result = CliRunner().invoke(main, ["validate", "--config", str(config_path)])

    assert result.exit_code != 0
    assert "bronze.tables.customer" in result.output


def test_run_reports_a_config_error_cleanly_instead_of_a_traceback(tmp_path: Path):
    config_path = write(tmp_path, INVALID_CONFIG)

    result = CliRunner().invoke(main, ["run", "--config", str(config_path)])

    assert result.exit_code != 0
    assert "bronze.tables.customer" in result.output
    assert "Traceback" not in result.output


def test_run_rejects_a_missing_config_file(tmp_path: Path):
    result = CliRunner().invoke(main, ["run", "--config", str(tmp_path / "nope.yaml")])

    assert result.exit_code != 0
