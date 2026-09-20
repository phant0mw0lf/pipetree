import logging
from pathlib import Path

import pytest

from pipetree import run_pipeline
from pipetree.config.errors import ConfigError
from pipetree.executor.status import RunDigest, TableStatus
from pipetree.graph.errors import UnknownTableError
from pipetree.runlog.writer import InMemoryRunLogWriter

from .helpers import FakeAdapter

MINIMAL_CONFIG = """
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


def write_config(tmp_path: Path, text: str = MINIMAL_CONFIG) -> Path:
    path = tmp_path / "pipeline.yaml"
    path.write_text(text)
    return path


class RecordingNotifier:
    def __init__(self) -> None:
        self.notified: list[RunDigest] = []

    def notify(self, digest: RunDigest) -> None:
        self.notified.append(digest)


def test_run_pipeline_loads_config_builds_graph_and_executes(tmp_path: Path):
    config_path = write_config(tmp_path)
    adapter = FakeAdapter()

    digest = run_pipeline(config_path, adapter=adapter, execution_id=1)

    assert digest.results["bronze.customer"].status == TableStatus.SUCCEEDED
    assert adapter.calls == ["bronze.customer"]


def test_run_pipeline_writes_to_the_given_run_log_writer(tmp_path: Path):
    config_path = write_config(tmp_path)
    writer = InMemoryRunLogWriter()

    digest = run_pipeline(config_path, adapter=FakeAdapter(), execution_id=1, run_log_writer=writer)

    rows = writer.rows_for(digest.execution_id)
    assert len(rows) == 1
    assert rows[0]["table_fqn"] == "bronze.customer"
    assert rows[0]["status"] == "succeeded"


def test_run_pipeline_notifies_via_the_given_notifier(tmp_path: Path):
    config_path = write_config(tmp_path)
    notifier = RecordingNotifier()

    digest = run_pipeline(config_path, adapter=FakeAdapter(), execution_id=1, notifier=notifier)

    assert notifier.notified == [digest]


def test_run_pipeline_propagates_config_errors(tmp_path: Path):
    config_path = write_config(tmp_path, text="bronze:\n  tables:\n    x:\n      strategy: bogus\n")

    with pytest.raises(ConfigError):
        run_pipeline(config_path, adapter=FakeAdapter())


def test_run_pipeline_uses_sensible_defaults_when_not_given(tmp_path: Path):
    config_path = write_config(tmp_path)

    digest = run_pipeline(config_path, adapter=FakeAdapter())

    assert digest.succeeded
    assert digest.execution_id > 0


def test_select_runs_only_the_named_table(tmp_path: Path):
    config_path = write_config(tmp_path, CHAIN_CONFIG)
    adapter = FakeAdapter()

    digest = run_pipeline(config_path, adapter=adapter, execution_id=1, select=["customer"])

    assert digest.results["bronze.customer"].status == TableStatus.SUCCEEDED
    assert digest.results["silver.customer_enriched"].status == TableStatus.SKIPPED
    assert adapter.calls == ["bronze.customer"]


def test_select_with_dependents_runs_the_whole_subtree(tmp_path: Path):
    config_path = write_config(tmp_path, CHAIN_CONFIG)
    adapter = FakeAdapter()

    digest = run_pipeline(
        config_path,
        adapter=adapter,
        execution_id=1,
        select=["customer"],
        with_dependents=True,
    )

    assert digest.results["bronze.customer"].status == TableStatus.SUCCEEDED
    assert digest.results["silver.customer_enriched"].status == TableStatus.SUCCEEDED


def test_select_raises_for_an_unknown_table(tmp_path: Path):
    config_path = write_config(tmp_path, CHAIN_CONFIG)

    with pytest.raises(UnknownTableError):
        run_pipeline(config_path, adapter=FakeAdapter(), select=["nonexistent"])


def test_select_without_with_dependents_logs_the_execution_id_caveat(tmp_path: Path, caplog):
    config_path = write_config(tmp_path, CHAIN_CONFIG)

    with caplog.at_level(logging.WARNING, logger="pipetree"):
        run_pipeline(config_path, adapter=FakeAdapter(), select=["customer"])

    assert any("_execution_id" in r.message for r in caplog.records)


def test_init_flag_reaches_the_adapter(tmp_path: Path):
    config_path = write_config(tmp_path)
    adapter = FakeAdapter()

    run_pipeline(config_path, adapter=adapter, execution_id=1, init=True)

    assert adapter.init_calls == ["bronze.customer"]
