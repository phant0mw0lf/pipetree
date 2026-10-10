"""The spark fixture skips locally but fails when PIPETREE_REQUIRE_SPARK is set."""

from __future__ import annotations

import pytest

from tests import conftest


def test_unavailable_session_skips_by_default(monkeypatch):
    monkeypatch.delenv("PIPETREE_REQUIRE_SPARK", raising=False)
    with pytest.raises(pytest.skip.Exception):
        conftest._session_unavailable(RuntimeError("no jvm"))


@pytest.mark.parametrize("value", ["1", "true"])
def test_unavailable_session_fails_when_required(monkeypatch, value):
    monkeypatch.setenv("PIPETREE_REQUIRE_SPARK", value)
    with pytest.raises(pytest.fail.Exception, match="no jvm"):
        conftest._session_unavailable(RuntimeError("no jvm"))


def test_zero_does_not_require_spark(monkeypatch):
    monkeypatch.setenv("PIPETREE_REQUIRE_SPARK", "0")
    with pytest.raises(pytest.skip.Exception):
        conftest._session_unavailable(RuntimeError("no jvm"))
