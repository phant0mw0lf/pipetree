from __future__ import annotations

from typing import Any

import pytest

from pipetree.executor.status import RunDigest, TableResult, TableStatus
from pipetree.model import EmailNotification, Notifications, SecretRef
from pipetree.notify import acs_email
from pipetree.notify.acs_email import RunContext
from pipetree.notify.build import build_notifier
from pipetree.platform.local import LocalPlatform


class Platform(LocalPlatform):
    def __init__(self) -> None:
        super().__init__()
        self.resolved: list[str] = []

    def resolve_secret(self, name: str) -> str:
        self.resolved.append(name)
        return "https://from-secret.communication.azure.com"


def failed() -> RunDigest:
    result = TableResult("a.b", TableStatus.FAILED, 1, 0, 0, 1, "E", "m")
    return RunDigest(1, {"a.b": result})


def test_secret_endpoint_is_resolved_only_when_a_mail_is_sent(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    endpoints: list[str] = []

    class Client:
        def __init__(self, endpoint: str, credential: Any) -> None:
            endpoints.append(endpoint)

        def begin_send(self, message: dict[str, Any]) -> Any:
            return type("P", (), {"result": lambda s, timeout=None: {}, "done": lambda s: True})()

    monkeypatch.setattr(acs_email, "_sdk_available", lambda: True)
    monkeypatch.setattr(acs_email, "_create_email_client", Client)
    monkeypatch.setattr("pipetree.notify.build.credential_for", lambda p: object())
    platform = Platform()
    config = Notifications(
        email=EmailNotification(
            endpoint=SecretRef(secret="acs-endpoint"),
            sender="DoNotReply@g.azurecomm.net",
            to=["info@example.com"],
        )
    )
    notifier = build_notifier(config, platform, RunContext())

    ok = RunDigest(1, {"a.b": TableResult("a.b", TableStatus.SUCCEEDED, 1, 0, 0, 1)})
    notifier.notify(ok)
    assert platform.resolved == []

    notifier.notify(failed())
    assert platform.resolved == ["acs-endpoint"]
    assert endpoints == ["https://from-secret.communication.azure.com"]
    assert "Run 1: FAILED" in capsys.readouterr().out


def test_without_a_block_only_the_console_notifier_is_built():
    notifier = build_notifier(None, LocalPlatform(), RunContext())
    assert len(notifier._notifiers) == 1  # type: ignore[attr-defined]
