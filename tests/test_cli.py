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
            "--with-ancestors",
            "--init",
        ],
    )

    assert result.exit_code == 0
    assert calls["select"] == ["bronze.customer", "silver.x"]
    assert calls["with_dependents"] is True
    assert calls["with_ancestors"] is True
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
    assert calls["with_ancestors"] is False
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


def test_graph_renders_html_on_request_with_every_table_pending(tmp_path: Path):
    (tmp_path / "notebooks").mkdir()
    (tmp_path / "notebooks" / "customer_enriched.sql").write_text("SELECT 1")
    config_path = write(tmp_path, CHAIN_CONFIG)

    result = CliRunner().invoke(main, ["graph", "--config", str(config_path), "--format", "html"])

    assert result.exit_code == 0
    assert result.output.startswith("<!DOCTYPE html>")
    assert 'data-fqn="bronze.customer"' in result.output
    assert 'data-fqn="silver.customer_enriched"' in result.output
    assert 'data-status="pending"' in result.output
    assert "http-equiv" not in result.output


def test_run_live_html_passes_a_file_observer(tmp_path: Path, monkeypatch):
    from pipetree.notebook import HtmlFileObserver

    config_path = write(tmp_path, VALID_CONFIG)
    calls: dict = {}

    class FakeDigest:
        exit_code = 0

    def fake_run_pipeline(path, **kwargs):
        calls.update(kwargs)
        return FakeDigest()

    monkeypatch.setattr("pipetree.cli.run_pipeline", fake_run_pipeline)
    target = tmp_path / "live" / "run.html"

    result = CliRunner().invoke(
        main, ["run", "--config", str(config_path), "--live-html", str(target)]
    )

    assert result.exit_code == 0
    assert isinstance(calls["observer"], HtmlFileObserver)
    assert calls["observer"].path == target
    assert str(target) in result.output


def test_run_without_live_html_passes_no_observer(tmp_path: Path, monkeypatch):
    config_path = write(tmp_path, VALID_CONFIG)
    calls: dict = {}

    class FakeDigest:
        exit_code = 0

    def fake_run_pipeline(path, **kwargs):
        calls.update(kwargs)
        return FakeDigest()

    monkeypatch.setattr("pipetree.cli.run_pipeline", fake_run_pipeline)

    CliRunner().invoke(main, ["run", "--config", str(config_path)])

    assert calls["observer"] is None


def test_run_live_html_writes_a_final_page_for_a_real_run(tmp_path: Path, monkeypatch):
    import pipetree

    from .helpers import FakeAdapter

    config_path = write(tmp_path, VALID_CONFIG)
    real_run_pipeline = pipetree.run_pipeline

    def run_with_fake_adapter(path, **kwargs):
        return real_run_pipeline(path, adapter=FakeAdapter(), **kwargs)

    monkeypatch.setattr("pipetree.cli.run_pipeline", run_with_fake_adapter)
    target = tmp_path / "run.html"

    result = CliRunner().invoke(
        main, ["run", "--config", str(config_path), "--live-html", str(target)]
    )

    assert result.exit_code == 0, result.output
    page = target.read_text(encoding="utf-8")
    assert 'data-fqn="bronze.customer" data-status="succeeded"' in page
    assert "http-equiv" not in page


def test_graph_reports_an_unresolvable_dependency_cleanly(tmp_path: Path):
    (tmp_path / "notebooks").mkdir()
    (tmp_path / "notebooks" / "customer_enriched.py").write_text(
        "df = spark.table(get_table_name())\n"
    )
    config_path = write(
        tmp_path,
        CHAIN_CONFIG.replace("customer_enriched.sql", "customer_enriched.py").replace(
            "depends_on: [customer]", "depends_on: auto"
        ),
    )

    result = CliRunner().invoke(main, ["graph", "--config", str(config_path)])

    assert result.exit_code != 0
    assert "customer_enriched.py" in result.output
    assert "spark.table(get_table_name())" in result.output
    assert "depends_on" in result.output
    assert "Traceback" not in result.output


TAGGED_CONFIG = CHAIN_CONFIG.replace(
    "      strategy: scd1\n", "      strategy: scd1\n      tags: [daily, crm]\n"
).replace(
    "      depends_on: [customer]\n", "      depends_on: [customer]\n      tags: [reporting]\n"
)


def _capture_run(monkeypatch) -> dict:
    calls: dict = {}

    class FakeDigest:
        exit_code = 0

    def fake_run_pipeline(path, **kwargs):
        calls.update(kwargs)
        return FakeDigest()

    monkeypatch.setattr("pipetree.cli.run_pipeline", fake_run_pipeline)
    return calls


def test_run_passes_repeated_tags_and_exclude_tags_through(tmp_path: Path, monkeypatch):
    config_path = write(tmp_path, TAGGED_CONFIG)
    calls = _capture_run(monkeypatch)

    result = CliRunner().invoke(
        main,
        [
            "run",
            "--config",
            str(config_path),
            "--tag",
            "daily",
            "--tag",
            "crm",
            "--exclude-tag",
            "reporting",
        ],
    )

    assert result.exit_code == 0
    assert calls["tags"] == ["daily", "crm"]
    assert calls["exclude_tags"] == ["reporting"]


def test_run_tag_values_may_be_comma_separated_and_untidy(tmp_path: Path, monkeypatch):
    config_path = write(tmp_path, TAGGED_CONFIG)
    calls = _capture_run(monkeypatch)

    CliRunner().invoke(main, ["run", "--config", str(config_path), "--tag", "daily, crm"])

    assert calls["tags"] == ["daily", "crm"]


def test_run_without_tags_passes_none(tmp_path: Path, monkeypatch):
    config_path = write(tmp_path, TAGGED_CONFIG)
    calls = _capture_run(monkeypatch)

    CliRunner().invoke(main, ["run", "--config", str(config_path)])

    assert calls["tags"] is None
    assert calls["exclude_tags"] is None


def test_run_with_an_unknown_tag_is_a_clean_error_listing_the_known_tags(tmp_path: Path):
    config_path = write(tmp_path, TAGGED_CONFIG)

    result = CliRunner().invoke(main, ["run", "--config", str(config_path), "--tag", "weekly"])

    assert result.exit_code != 0
    assert "weekly" in result.output
    assert "crm, daily, reporting" in result.output
    assert "Traceback" not in result.output


def test_run_with_an_empty_selection_is_a_clean_error(tmp_path: Path):
    config_path = write(tmp_path, TAGGED_CONFIG)

    result = CliRunner().invoke(
        main,
        ["run", "--config", str(config_path), "--tag", "crm", "--exclude-tag", "daily"],
    )

    assert result.exit_code != 0
    assert "no table" in result.output


def test_validate_rejects_a_bad_tag_with_the_key_path(tmp_path: Path):
    config_path = write(
        tmp_path, VALID_CONFIG.replace("strategy: scd1", "strategy: scd1\n      tags: [Bad]")
    )

    result = CliRunner().invoke(main, ["validate", "--config", str(config_path)])

    assert result.exit_code != 0
    assert "bronze.tables.customer.tags[0]" in result.output


def test_validate_list_tags_prints_each_tag_with_its_table_count(tmp_path: Path):
    (tmp_path / "notebooks").mkdir()
    (tmp_path / "notebooks" / "customer_enriched.sql").write_text("SELECT 1")
    config_path = write(tmp_path, TAGGED_CONFIG)

    result = CliRunner().invoke(main, ["validate", "--config", str(config_path), "--list-tags"])

    assert result.exit_code == 0
    lines = [line.split() for line in result.output.strip().splitlines()]
    assert lines == [["crm", "1"], ["daily", "1"], ["reporting", "1"]]


def test_validate_list_tags_says_so_when_no_table_has_tags(tmp_path: Path):
    config_path = write(tmp_path, VALID_CONFIG)

    result = CliRunner().invoke(main, ["validate", "--config", str(config_path), "--list-tags"])

    assert result.exit_code == 0
    assert "no tags" in result.output


def _graph_fixture(tmp_path: Path) -> Path:
    (tmp_path / "notebooks").mkdir()
    (tmp_path / "notebooks" / "customer_enriched.sql").write_text("SELECT 1")
    return write(tmp_path, TAGGED_CONFIG)


def test_graph_text_marks_the_tables_a_tag_would_not_run(tmp_path: Path):
    config_path = _graph_fixture(tmp_path)

    result = CliRunner().invoke(main, ["graph", "--config", str(config_path), "--tag", "reporting"])

    assert result.exit_code == 0
    lines = {line.split()[0]: line for line in result.output.splitlines()}
    assert "not selected" in lines["bronze.customer"]
    assert "not selected" not in lines["silver.customer_enriched"]


def test_graph_without_filters_is_unchanged(tmp_path: Path):
    config_path = _graph_fixture(tmp_path)

    result = CliRunner().invoke(main, ["graph", "--config", str(config_path)])

    assert "not selected" not in result.output


def test_graph_select_and_exclude_tag_work_like_run(tmp_path: Path):
    config_path = _graph_fixture(tmp_path)

    result = CliRunner().invoke(
        main,
        [
            "graph",
            "--config",
            str(config_path),
            "--select",
            "silver.customer_enriched",
            "--with-ancestors",
            "--exclude-tag",
            "crm",
        ],
    )

    assert result.exit_code == 0
    lines = {line.split()[0]: line for line in result.output.splitlines()}
    assert "not selected" in lines["bronze.customer"]
    assert "not selected" not in lines["silver.customer_enriched"]


def test_graph_html_shows_skipped_tables_and_tags_in_the_tooltip(tmp_path: Path):
    config_path = _graph_fixture(tmp_path)

    result = CliRunner().invoke(
        main,
        ["graph", "--config", str(config_path), "--format", "html", "--tag", "reporting"],
    )

    assert result.exit_code == 0
    assert 'data-fqn="bronze.customer" data-status="skipped"' in result.output
    assert 'data-fqn="silver.customer_enriched" data-status="pending"' in result.output
    assert "tags: daily, crm" in result.output
    assert "not selected" in result.output


def test_graph_mermaid_marks_the_tables_that_would_not_run(tmp_path: Path):
    config_path = _graph_fixture(tmp_path)

    result = CliRunner().invoke(
        main,
        ["graph", "--config", str(config_path), "--format", "mermaid", "--tag", "reporting"],
    )

    assert result.exit_code == 0
    assert "classDef notselected" in result.output
    assert "class n0 notselected" in result.output


def test_graph_with_an_unknown_tag_is_a_clean_error(tmp_path: Path):
    config_path = _graph_fixture(tmp_path)

    result = CliRunner().invoke(main, ["graph", "--config", str(config_path), "--tag", "nope"])

    assert result.exit_code != 0
    assert "nope" in result.output
