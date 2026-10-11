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


def test_select_with_ancestors_runs_the_table_and_everything_it_reads(tmp_path: Path):
    config_path = write_config(tmp_path, CHAIN_CONFIG)
    adapter = FakeAdapter()

    digest = run_pipeline(
        config_path,
        adapter=adapter,
        execution_id=1,
        select=["customer_enriched"],
        with_ancestors=True,
    )

    assert digest.results["bronze.customer"].status == TableStatus.SUCCEEDED
    assert digest.results["silver.customer_enriched"].status == TableStatus.SUCCEEDED
    assert digest.succeeded


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


class RecordingObserver:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def on_run_start(self, graph, run_set, execution_id) -> None:
        self.calls.append(f"start:{sorted(run_set)}")

    def on_table_event(self, event) -> None:
        self.calls.append(f"{event.fqn}:{event.status}")

    def on_run_end(self, digest) -> None:
        self.calls.append("end")


def test_run_pipeline_passes_the_observer_through_to_the_executor(tmp_path: Path):
    config_path = write_config(tmp_path)
    observer = RecordingObserver()

    run_pipeline(
        config_path,
        adapter=FakeAdapter(),
        execution_id=1,
        observer=observer,
        notifier=RecordingNotifier(),
    )

    assert observer.calls == [
        "start:['bronze.customer']",
        "bronze.customer:running",
        "bronze.customer:succeeded",
        "end",
    ]


TAGGED_CONFIG = """
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
      tags: [daily, crm]
    orders:
      source:
        system: crm
        object: orders
      business_key: [id]
      strategy: scd1
      tags: [hourly]

silver:
  tables:
    customer_enriched:
      logic: notebooks/customer_enriched.sql
      business_key: [id]
      strategy: replace
      depends_on: [customer, orders]
      tags: [daily]
"""


def test_tags_run_only_the_tables_carrying_one_of_them(tmp_path: Path):
    config_path = write_config(tmp_path, TAGGED_CONFIG)
    adapter = FakeAdapter()

    digest = run_pipeline(config_path, adapter=adapter, execution_id=1, tags=["crm", "hourly"])

    assert sorted(adapter.calls) == ["bronze.customer", "bronze.orders"]
    assert digest.results["silver.customer_enriched"].status == TableStatus.SKIPPED


def test_exclude_tags_leave_those_tables_out(tmp_path: Path):
    config_path = write_config(tmp_path, TAGGED_CONFIG)
    adapter = FakeAdapter()

    run_pipeline(config_path, adapter=adapter, execution_id=1, exclude_tags=["hourly"])

    assert sorted(adapter.calls) == ["bronze.customer", "silver.customer_enriched"]


def test_a_tag_run_notes_the_inputs_it_did_not_refresh(tmp_path: Path):
    config_path = write_config(tmp_path, TAGGED_CONFIG)

    digest = run_pipeline(config_path, adapter=FakeAdapter(), execution_id=1, tags=["daily"])

    assert digest.results["silver.customer_enriched"].notes == [
        "input not refreshed: bronze.orders"
    ]
    assert digest.exit_code == 0


def test_an_unknown_tag_raises_before_anything_runs(tmp_path: Path):
    from pipetree.graph.errors import UnknownTagError

    config_path = write_config(tmp_path, TAGGED_CONFIG)
    adapter = FakeAdapter()

    with pytest.raises(UnknownTagError):
        run_pipeline(config_path, adapter=adapter, execution_id=1, tags=["weekly"])

    assert adapter.calls == []


def test_an_empty_selection_raises_instead_of_a_silent_successful_run(tmp_path: Path):
    from pipetree.graph.errors import EmptySelectionError

    config_path = write_config(tmp_path, TAGGED_CONFIG)

    with pytest.raises(EmptySelectionError):
        run_pipeline(
            config_path,
            adapter=FakeAdapter(),
            execution_id=1,
            tags=["crm"],
            exclude_tags=["crm"],
        )


def test_the_run_log_records_the_requested_tags_on_every_row(tmp_path: Path):
    config_path = write_config(tmp_path, TAGGED_CONFIG)
    writer = InMemoryRunLogWriter()

    run_pipeline(
        config_path,
        adapter=FakeAdapter(),
        execution_id=7,
        tags=["daily"],
        exclude_tags=["crm"],
        run_log_writer=writer,
    )

    assert {tuple(row["run_tags"]) for row in writer.rows_for(7)} == {("daily",)}


def test_the_run_log_has_empty_run_tags_without_a_tag_filter(tmp_path: Path):
    config_path = write_config(tmp_path, TAGGED_CONFIG)
    writer = InMemoryRunLogWriter()

    run_pipeline(config_path, adapter=FakeAdapter(), execution_id=7, run_log_writer=writer)

    assert {tuple(row["run_tags"]) for row in writer.rows_for(7)} == {()}


def test_the_partial_run_warning_also_covers_a_tag_run(tmp_path: Path, caplog):
    config_path = write_config(tmp_path, TAGGED_CONFIG)

    with caplog.at_level(logging.WARNING, logger="pipetree"):
        run_pipeline(config_path, adapter=FakeAdapter(), execution_id=1, tags=["daily"])

    assert any("without --with-dependents" in r.getMessage() for r in caplog.records)


class RaisingNotifier:
    def notify(self, digest: RunDigest) -> None:
        raise RuntimeError("secret-body-and-credentials")


def test_a_raising_notifier_never_alters_the_run(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    config_path = write_config(tmp_path)
    with caplog.at_level(logging.WARNING, logger="pipetree"):
        digest = run_pipeline(
            config_path, adapter=FakeAdapter(), execution_id=1, notifier=RaisingNotifier()
        )

    assert digest.succeeded
    assert digest.exit_code == 0
    assert digest.results["bronze.customer"].status == TableStatus.SUCCEEDED
    warnings = [r.getMessage() for r in caplog.records if "notif" in r.getMessage().lower()]
    assert warnings and "RuntimeError" in warnings[0]
    assert "secret-body-and-credentials" not in warnings[0]


def test_a_raising_notifier_leaves_a_failed_digest_untouched(tmp_path: Path):
    config_path = write_config(tmp_path)

    def boom() -> None:
        raise ValueError("bad")

    digest = run_pipeline(
        config_path,
        adapter=FakeAdapter({"bronze.customer": boom}),
        execution_id=1,
        notifier=RaisingNotifier(),
    )
    assert digest.exit_code == 1
    assert digest.results["bronze.customer"].status == TableStatus.FAILED


EMAIL_CONFIG = (
    MINIMAL_CONFIG
    + """
notifications:
  email:
    endpoint: https://acs.communication.azure.com
    sender: DoNotReply@guid.azurecomm.net
    to: [info@example.com]
"""
)


def test_config_block_builds_a_composite_with_console_and_email(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    from pipetree.notify import acs_email

    sent: list[dict] = []

    class Poller:
        def result(self, timeout=None):
            return {}

        def done(self):
            return True

    class Client:
        def __init__(self, endpoint, credential):
            self.endpoint = endpoint

        def begin_send(self, message):
            sent.append(message)
            return Poller()

    monkeypatch.setattr(acs_email, "_create_email_client", lambda e, c: Client(e, c))
    monkeypatch.setattr(acs_email, "_sdk_available", lambda: True)
    monkeypatch.setattr("pipetree.notify.credential._default_credential", lambda: object())
    config_path = write_config(tmp_path, EMAIL_CONFIG)

    def boom() -> None:
        raise ValueError("bad")

    digest = run_pipeline(
        config_path, adapter=FakeAdapter({"bronze.customer": boom}), execution_id=5
    )

    assert digest.exit_code == 1
    assert "Run 5: FAILED" in capsys.readouterr().out  # console print stays
    assert len(sent) == 1
    assert "FAILED run 5" in sent[0]["content"]["subject"]


def test_explicit_notifier_wins_over_the_config_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    from pipetree.notify import acs_email

    monkeypatch.setattr(
        acs_email,
        "_create_email_client",
        lambda e, c: pytest.fail("email must not be built when notifier= is given"),
    )
    config_path = write_config(tmp_path, EMAIL_CONFIG)
    notifier = RecordingNotifier()

    def boom() -> None:
        raise ValueError("bad")

    digest = run_pipeline(
        config_path,
        adapter=FakeAdapter({"bronze.customer": boom}),
        execution_id=1,
        notifier=notifier,
    )
    assert notifier.notified == [digest]


def test_successful_run_never_builds_the_email_notifier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    from pipetree.notify import acs_email

    built: list[int] = []

    def forbidden(*args, **kwargs):
        built.append(1)
        raise AssertionError("must not be called")

    secret_config = EMAIL_CONFIG.replace(
        "endpoint: https://acs.communication.azure.com", "endpoint: {secret: acs-endpoint}"
    )
    monkeypatch.setattr(acs_email, "_sdk_available", lambda: False)  # would raise if checked
    monkeypatch.setattr(acs_email, "_create_email_client", forbidden)
    monkeypatch.setattr("pipetree.notify.build.credential_for", forbidden)
    monkeypatch.setattr("pipetree.platform.local.LocalPlatform.resolve_secret", forbidden)
    with caplog.at_level(logging.WARNING, logger="pipetree"):
        digest = run_pipeline(write_config(tmp_path, secret_config), adapter=FakeAdapter())

    assert digest.succeeded
    assert built == []
    assert not any("notification" in r.getMessage() for r in caplog.records)


def test_failed_run_builds_the_email_notifier_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from pipetree.notify import acs_email

    created: list[int] = []

    def create(endpoint, credential):
        created.append(1)
        return type("C", (), {"begin_send": _send})()

    monkeypatch.setattr(acs_email, "_sdk_available", lambda: True)
    monkeypatch.setattr(acs_email, "_create_email_client", create)
    monkeypatch.setattr("pipetree.notify.build.credential_for", lambda p: object())

    def boom() -> None:
        raise ValueError("bad")

    run_pipeline(
        write_config(tmp_path, EMAIL_CONFIG), adapter=FakeAdapter({"bronze.customer": boom})
    )
    assert created == [1]


def test_a_broken_email_config_does_not_fail_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    from pipetree.notify import acs_email

    monkeypatch.setattr(acs_email, "_sdk_available", lambda: False)
    config_path = write_config(tmp_path, EMAIL_CONFIG)

    def boom() -> None:
        raise ValueError("bad")

    with caplog.at_level(logging.WARNING, logger="pipetree"):
        digest = run_pipeline(
            config_path, adapter=FakeAdapter({"bronze.customer": boom}), execution_id=1
        )
    assert digest.exit_code == 1
    assert any("ValueError" in r.getMessage() for r in caplog.records)


def _send(self, message):
    class Poller:
        def result(self, timeout=None):
            return {}

        def done(self):
            return True

    return Poller()


def test_config_built_notifier_uses_the_platform_of_the_adapter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    from pipetree.notify import acs_email
    from pipetree.platform.local import LocalPlatform

    seen: list[object] = []
    monkeypatch.setattr(acs_email, "_sdk_available", lambda: True)
    monkeypatch.setattr(
        "pipetree.notify.build.credential_for", lambda platform: seen.append(platform) or object()
    )

    class PlatformAdapter(FakeAdapter):
        platform = LocalPlatform()

    monkeypatch.setattr(
        acs_email, "_create_email_client", lambda e, c: type("C", (), {"begin_send": _send})()
    )

    def boom() -> None:
        raise ValueError("bad")

    adapter = PlatformAdapter({"bronze.customer": boom})
    path = write_config(tmp_path, EMAIL_CONFIG)
    run_pipeline(path, adapter=adapter, execution_id=1)
    assert seen == [adapter.platform]

    seen.clear()
    explicit = LocalPlatform()
    run_pipeline(path, adapter=adapter, execution_id=1, platform=explicit)
    assert seen == [explicit]
