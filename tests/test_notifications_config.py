from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from pipetree.config.errors import ConfigError
from pipetree.config.loader import load_config
from pipetree.model import PipelineConfig, SecretRef

BASE = """
systems:
  crm:
    type: synapse_link
bronze:
  tables:
    customer:
      source: {system: crm, object: account}
      business_key: [id]
      strategy: scd1
"""


def load(tmp_path: Path, notifications: dict[str, Any] | None) -> PipelineConfig:
    raw = yaml.safe_load(BASE)
    if notifications is not None:
        raw["notifications"] = notifications
    path = tmp_path / "p.yaml"
    path.write_text(yaml.safe_dump(raw))
    return PipelineConfig.from_validated_raw(load_config(path))


def email(**overrides: Any) -> dict[str, Any]:
    block = {
        "endpoint": "https://acs.communication.azure.com",
        "sender": "DoNotReply@guid.azurecomm.net",
        "to": ["info@example.com"],
    }
    block.update(overrides)
    return {"email": block}


def test_no_block_means_no_notifications(tmp_path: Path):
    assert load(tmp_path, None).notifications is None


def test_valid_block_is_parsed_and_notifications_is_not_a_layer(tmp_path: Path):
    config = load(tmp_path, email())
    assert config.notifications is not None
    assert config.notifications.email is not None
    assert config.notifications.email.on == "failure"
    assert config.notifications.email.to == ["info@example.com"]
    assert list(config.tables) == ["bronze.customer"]


def test_endpoint_may_be_a_secret_reference(tmp_path: Path):
    config = load(tmp_path, email(endpoint={"secret": "acs-endpoint"}))
    assert config.notifications is not None and config.notifications.email is not None
    assert config.notifications.email.endpoint == SecretRef(secret="acs-endpoint")


@pytest.mark.parametrize(
    "overrides",
    [
        {"to": []},
        {"to": ["not-an-address"]},
        {"to": ["a@b"]},
        {"sender": "nope"},
        {"on": "always"},
        {"unknown": 1},
    ],
)
def test_invalid_blocks_are_config_errors(tmp_path: Path, overrides: dict[str, Any]):
    with pytest.raises(ConfigError, match="notifications"):
        load(tmp_path, email(**overrides))


@pytest.mark.parametrize("missing", ["endpoint", "sender", "to"])
def test_required_keys(tmp_path: Path, missing: str):
    block = email()
    del block["email"][missing]
    with pytest.raises(ConfigError, match="notifications"):
        load(tmp_path, block)


def test_unknown_notification_channel_is_rejected(tmp_path: Path):
    with pytest.raises(ConfigError, match="notifications"):
        load(tmp_path, {"slack": {}})
