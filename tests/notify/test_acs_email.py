from __future__ import annotations

import logging
from typing import Any

import pytest

from pipetree.executor.status import RunDigest, TableResult, TableStatus
from pipetree.notify import acs_email
from pipetree.notify.acs_email import EmailNotifier, RunContext


def result(
    fqn: str,
    status: TableStatus = TableStatus.SUCCEEDED,
    *,
    attempts: int = 1,
    error_type: str | None = None,
    error_message: str | None = None,
    details: dict[str, Any] | None = None,
) -> TableResult:
    return TableResult(
        table_fqn=fqn,
        status=status,
        attempts=attempts,
        started_at=0,
        ended_at=0,
        duration_ms=5,
        error_type=error_type,
        error_message=error_message,
        details=details or {},
    )


def failed_digest() -> RunDigest:
    return RunDigest(
        execution_id=42,
        results={
            "bronze.customer": result(
                "bronze.customer", details={"duplicates_dropped": 2, "null_keys_dropped": 1}
            ),
            "bronze.orders": result(
                "bronze.orders", details={"schema_changes": ["added: channel (string)"]}
            ),
            "silver.dim_customer": result(
                "silver.dim_customer",
                TableStatus.FAILED,
                attempts=3,
                error_type="AnalysisException",
                error_message="column x not found",
            ),
            "gold.fact_sales": result(
                "gold.fact_sales",
                TableStatus.UPSTREAM_FAILED,
                attempts=0,
                details={"failed_ancestor": "silver.dim_customer"},
            ),
            "gold.report": result("gold.report", TableStatus.UPSTREAM_FAILED, attempts=0),
        },
    )


def ok_digest() -> RunDigest:
    return RunDigest(execution_id=7, results={"bronze.customer": result("bronze.customer")})


class FakePoller:
    def __init__(self, done: bool = True) -> None:
        self._done = done
        self.timeouts: list[float | None] = []

    def result(self, timeout: float | None = None) -> dict[str, str]:
        self.timeouts.append(timeout)
        return {"status": "Succeeded"}

    def done(self) -> bool:
        return self._done


class FakeEmailClient:
    def __init__(self, endpoint: str, credential: Any, poller: FakePoller) -> None:
        self.endpoint = endpoint
        self.credential = credential
        self.poller = poller
        self.messages: list[dict[str, Any]] = []

    def begin_send(self, message: dict[str, Any]) -> FakePoller:
        self.messages.append(message)
        return self.poller


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> list[FakeEmailClient]:
    created: list[FakeEmailClient] = []

    def fake_create(endpoint: str, credential: Any) -> FakeEmailClient:
        c = FakeEmailClient(endpoint, credential, FakePoller())
        created.append(c)
        return c

    monkeypatch.setattr(acs_email, "_create_email_client", fake_create)
    return created


def make_notifier(**overrides: Any) -> EmailNotifier:
    kwargs: dict[str, Any] = {
        "endpoint": "https://acs.communication.azure.com",
        "sender": "DoNotReply@guid.azurecomm.net",
        "to": ["info@example.com", "ops@example.com"],
        "credential": object(),
    }
    kwargs.update(overrides)
    return EmailNotifier(**kwargs)


def test_subject_names_counts_and_first_three_failed_tables():
    digest = RunDigest(
        execution_id=9,
        results={
            f"s.t{i}": result(f"s.t{i}", TableStatus.FAILED, error_type="E", error_message="m")
            for i in range(5)
        }
        | {"s.u": result("s.u", TableStatus.UPSTREAM_FAILED, attempts=0)},
    )
    assert acs_email.format_subject(digest) == (
        "[pipetree] FAILED run 9: 5 failed, 1 upstream_failed - s.t0, s.t1, s.t2"
    )


def test_text_body_has_failures_grouped_upstream_and_notes():
    text = acs_email.format_text(failed_digest(), RunContext())
    assert "Run 42" in text
    assert "silver.dim_customer" in text
    assert "AnalysisException" in text
    assert "column x not found" in text
    assert "attempts: 3" in text
    # upstream_failed grouped under its failed ancestor
    assert "gold.fact_sales" in text
    assert text.index("gold.fact_sales") > text.index("silver.dim_customer")
    # without a known ancestor it is still listed
    assert "gold.report" in text
    # notes of all tables
    assert "bronze.customer" in text and "2 duplicate row(s) dropped" in text
    assert "1 NULL-key row(s) dropped" in text
    assert "schema: added: channel (string)" in text


def test_text_body_includes_selection_and_platform_metadata():
    ctx = RunContext(
        select=["customer", "orders"],
        with_dependents=True,
        init=True,
        run_metadata={"job_run_url": "https://adb.example/run/1", "run_id": "1"},
    )
    text = acs_email.format_text(failed_digest(), ctx)
    assert "https://adb.example/run/1" in text
    assert "customer, orders" in text
    assert "with dependents" in text
    assert "init" in text
    assert "1 failed, 2 upstream_failed, 2 succeeded" in text


def test_error_message_is_truncated():
    digest = RunDigest(
        execution_id=1,
        results={
            "a.b": result("a.b", TableStatus.FAILED, error_type="E", error_message="x" * 5000)
        },
    )
    text = acs_email.format_text(digest, RunContext())
    assert "x" * 500 in text
    assert "x" * 600 not in text


def test_html_escapes_table_names_and_error_text():
    digest = RunDigest(
        execution_id=1,
        results={
            "a.<b>": result(
                "a.<b>",
                TableStatus.FAILED,
                error_type="E<x>",
                error_message="<script>alert('&')</script>",
            ),
        },
    )
    html = acs_email.format_html(digest, RunContext(run_metadata={"k": "<v>"}))
    assert "<script>" not in html
    assert "&lt;script&gt;" in html
    assert "a.&lt;b&gt;" in html
    assert "E&lt;x&gt;" in html
    assert "&lt;v&gt;" in html
    assert "http://" not in html and "src=" not in html


def test_html_contains_the_same_facts():
    html = acs_email.format_html(failed_digest(), RunContext())
    for needle in ("silver.dim_customer", "gold.fact_sales", "gold.report", "2 duplicate row(s)"):
        assert needle in html


def test_notify_sends_once_with_sender_recipients_subject_and_bodies(
    client: list[FakeEmailClient],
):
    credential = object()
    notifier = make_notifier(credential=credential)
    digest = failed_digest()

    notifier.notify(digest)

    assert len(client) == 1
    assert client[0].endpoint == "https://acs.communication.azure.com"
    assert client[0].credential is credential
    assert len(client[0].messages) == 1
    message = client[0].messages[0]
    assert message["senderAddress"] == "DoNotReply@guid.azurecomm.net"
    assert message["recipients"] == {
        "to": [{"address": "info@example.com"}, {"address": "ops@example.com"}]
    }
    assert message["content"]["subject"] == acs_email.format_subject(digest)
    assert message["content"]["plainText"] == acs_email.format_text(digest, RunContext())
    assert message["content"]["html"] == acs_email.format_html(digest, RunContext())


def test_on_failure_skips_successful_runs(client: list[FakeEmailClient]):
    make_notifier().notify(ok_digest())
    assert client == []


def test_skipped_tables_do_not_count_as_failure(client: list[FakeEmailClient]):
    digest = RunDigest(execution_id=1, results={"a.b": result("a.b", TableStatus.SKIPPED)})
    make_notifier().notify(digest)
    assert client == []


def test_wait_for_the_send_is_bounded(client: list[FakeEmailClient]):
    make_notifier(wait_seconds=3).notify(failed_digest())
    assert client[0].poller.timeouts == [3]


def test_default_wait_is_at_most_30_seconds(client: list[FakeEmailClient]):
    make_notifier().notify(failed_digest())
    (timeout,) = client[0].poller.timeouts
    assert timeout is not None and timeout <= 30


def test_unfinished_send_logs_a_warning_and_returns(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    poller = FakePoller(done=False)
    monkeypatch.setattr(
        acs_email, "_create_email_client", lambda e, c: FakeEmailClient(e, c, poller)
    )
    with caplog.at_level(logging.WARNING, logger="pipetree"):
        make_notifier(wait_seconds=1).notify(failed_digest())
    assert any("not confirmed" in r.message for r in caplog.records)


def test_endpoint_can_be_resolved_lazily_and_only_when_sending(
    client: list[FakeEmailClient],
):
    calls: list[int] = []

    def endpoint() -> str:
        calls.append(1)
        return "https://lazy.communication.azure.com"

    notifier = make_notifier(endpoint=endpoint)
    notifier.notify(ok_digest())
    assert calls == []
    notifier.notify(failed_digest())
    assert client[0].endpoint == "https://lazy.communication.azure.com"


def test_missing_sdk_raises_a_clear_error(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(acs_email, "_sdk_available", lambda: False)
    with pytest.raises(ValueError, match="azure-communication-email"):
        make_notifier()


def test_unsupported_on_value_is_rejected():
    with pytest.raises(ValueError, match="on"):
        make_notifier(on="always")
