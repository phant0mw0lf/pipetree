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

CHAIN_CONFIG = """
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

silver:
  tables:
    customer_enriched:
      logic: notebooks/customer_enriched.sql
      business_key: [id]
      strategy: replace
      depends_on: [customer]
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


def test_run_passes_select_with_dependents_and_init_through(tmp_path: Path, monkeypatch):
    config_path = write(tmp_path, VALID_CONFIG)
    calls: dict = {}

    class FakeDigest:
        exit_code = 0

    def fake_run_pipeline(path, **kwargs):
        calls["config_path"] = path
        calls.update(kwargs)
        return FakeDigest()

    monkeypatch.setattr("pipetree.cli.run_pipeline", fake_run_pipeline)

    result = CliRunner().invoke(
        main,
        [
            "run",
            "--config",
            str(config_path),
            "--select",
            "bronze.customer,silver.x",
            "--with-dependents",
            "--init",
        ],
    )

    assert result.exit_code == 0
    assert calls["select"] == ["bronze.customer", "silver.x"]
    assert calls["with_dependents"] is True
    assert calls["init"] is True


def test_run_without_select_passes_none(tmp_path: Path, monkeypatch):
    config_path = write(tmp_path, VALID_CONFIG)
    calls: dict = {}

    class FakeDigest:
        exit_code = 0

    def fake_run_pipeline(path, **kwargs):
        calls.update(kwargs)
        return FakeDigest()

    monkeypatch.setattr("pipetree.cli.run_pipeline", fake_run_pipeline)

    CliRunner().invoke(main, ["run", "--config", str(config_path)])

    assert calls["select"] is None
    assert calls["with_dependents"] is False
    assert calls["init"] is False


def test_graph_renders_text_by_default(tmp_path: Path):
    (tmp_path / "notebooks").mkdir()
    (tmp_path / "notebooks" / "customer_enriched.sql").write_text("SELECT 1")
    config_path = write(tmp_path, CHAIN_CONFIG)

    result = CliRunner().invoke(main, ["graph", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "bronze.customer" in result.output
    assert "depends on: bronze.customer" in result.output


def test_graph_renders_mermaid_on_request(tmp_path: Path):
    (tmp_path / "notebooks").mkdir()
    (tmp_path / "notebooks" / "customer_enriched.sql").write_text("SELECT 1")
    config_path = write(tmp_path, CHAIN_CONFIG)

    result = CliRunner().invoke(
        main, ["graph", "--config", str(config_path), "--format", "mermaid"]
    )

    assert result.exit_code == 0
    assert result.output.startswith("graph LR")


def test_graph_reports_a_config_error_cleanly(tmp_path: Path):
    config_path = write(tmp_path, INVALID_CONFIG)

    result = CliRunner().invoke(main, ["graph", "--config", str(config_path)])

    assert result.exit_code != 0
    assert "bronze.tables.customer" in result.output
