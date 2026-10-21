from __future__ import annotations

import base64
import json
import time
from typing import Any

import pytest

from pipetree.notify import acs_email, credential
from pipetree.notify.credential import ACS_RESOURCE, PlatformCredential, credential_for
from pipetree.platform.databricks import DatabricksPlatform
from pipetree.platform.fabric import FabricPlatform
from pipetree.platform.local import LocalPlatform


class FakePlatform:
    name = "fake"

    def __init__(self, token: str) -> None:
        self.token = token
        self.resources: list[str] = []

    def acquire_token(self, resource: str) -> str:
        self.resources.append(resource)
        return self.token


def jwt(exp: int) -> str:
    def part(data: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")

    return f"{part({'alg': 'none'})}.{part({'exp': exp})}.sig"


def test_get_token_returns_an_access_token_with_the_jwt_expiry():
    exp = int(time.time()) + 3000
    platform = FakePlatform(jwt(exp))
    token = PlatformCredential(platform, ACS_RESOURCE).get_token(
        "https://communication.azure.com//.default"
    )
    assert token.token == platform.token
    assert token.expires_on == exp
    assert platform.resources == [ACS_RESOURCE]


def test_opaque_token_gets_a_short_expiry():
    platform = FakePlatform("opaque")
    token = PlatformCredential(platform, ACS_RESOURCE).get_token("x")
    now = time.time()
    assert now < token.expires_on <= now + 600


def test_acs_resource_is_the_acs_audience():
    assert ACS_RESOURCE == "https://communication.azure.com"


class FakeProvider:
    def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        self.scopes = scopes
        return type("T", (), {"token": "dbx-token", "expires_on": 0})()


class FakeCredentials:
    def __init__(self) -> None:
        self.names: list[str] = []
        self.provider = FakeProvider()

    def getServiceCredentialsProvider(self, name: str) -> FakeProvider:  # noqa: N802
        self.names.append(name)
        return self.provider


class FakeDbutils:
    def __init__(self) -> None:
        self.credentials = FakeCredentials()


def test_databricks_with_a_service_credential_uses_the_platform():
    dbutils = FakeDbutils()
    platform = DatabricksPlatform(
        catalog="main", dbutils=dbutils, service_credentials={ACS_RESOURCE: "acs-cred"}
    )
    cred = credential_for(platform)
    assert isinstance(cred, PlatformCredential)
    assert cred.get_token("anything").token == "dbx-token"
    assert dbutils.credentials.names == ["acs-cred"]
    assert dbutils.credentials.provider.scopes == (f"{ACS_RESOURCE}/.default",)


def test_databricks_without_the_entry_falls_back_to_default_credential(
    monkeypatch: pytest.MonkeyPatch,
):
    sentinel = object()
    monkeypatch.setattr(credential, "_default_credential", lambda: sentinel)
    platform = DatabricksPlatform(catalog="main", dbutils=FakeDbutils())
    assert credential_for(platform) is sentinel


def test_fabric_and_local_and_none_use_the_default_credential(monkeypatch: pytest.MonkeyPatch):
    sentinel = object()
    monkeypatch.setattr(credential, "_default_credential", lambda: sentinel)
    fabric = FabricPlatform(object(), key_vault_url="https://kv.vault.azure.net")
    assert credential_for(fabric) is sentinel
    assert credential_for(LocalPlatform()) is sentinel
    assert credential_for(None) is sentinel


def test_acs_module_reexports_nothing_secret():
    assert not hasattr(acs_email, "connection_string")


def test_default_credential_is_the_azure_default_credential(monkeypatch: pytest.MonkeyPatch):
    import sys
    import types

    class FakeDefaultAzureCredential:
        pass

    module = types.ModuleType("azure.identity")
    module.DefaultAzureCredential = FakeDefaultAzureCredential  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "azure.identity", module)

    assert isinstance(credential._default_credential(), FakeDefaultAzureCredential)
