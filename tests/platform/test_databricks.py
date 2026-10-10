from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from pipetree.platform import databricks as databricks_module
from pipetree.platform.base import Platform
from pipetree.platform.databricks import DatabricksPlatform


class FakeDbutils:
    def __init__(self) -> None:
        self.credentials: FakeCredentialsAPI | None = None


class FakeSecret:
    def __init__(self, value: str) -> None:
        self.value = value


class FakeSecretClient:
    def __init__(self, vault_url: str, credential: Any, values: dict[str, str]) -> None:
        self.vault_url = vault_url
        self.credential = credential
        self.values = values
        self.requested: list[str] = []

    def get_secret(self, name: str) -> FakeSecret:
        self.requested.append(name)
        return FakeSecret(self.values[name])


VAULT = "https://kv-pipetree.vault.azure.net"


@pytest.fixture
def clients(monkeypatch: pytest.MonkeyPatch) -> list[FakeSecretClient]:
    created: list[FakeSecretClient] = []

    def factory(vault_url: str, credential: Any) -> FakeSecretClient:
        client = FakeSecretClient(vault_url, credential, {"erp-landing-path": "/mnt/erp"})
        created.append(client)
        return client

    monkeypatch.setattr(databricks_module, "_create_secret_client", factory)
    return created


def make_platform_with_vault() -> tuple[DatabricksPlatform, FakeCredentialsAPI]:
    dbutils = FakeDbutils()
    api = FakeCredentialsAPI({"kv-cred": FakeTokenProvider("unused")})
    dbutils.credentials = api
    platform = DatabricksPlatform(
        dbutils=dbutils, catalog="prod", key_vault_url=VAULT, key_vault_credential="kv-cred"
    )
    return platform, api


def make_platform() -> DatabricksPlatform:
    return DatabricksPlatform(catalog="prod")


def test_satisfies_the_platform_protocol():
    assert isinstance(make_platform(), Platform)


def test_name_is_databricks():
    assert make_platform().name == "databricks"


def test_resolve_secret_reads_key_vault_through_the_service_credential(clients):
    platform, api = make_platform_with_vault()

    assert platform.resolve_secret("erp-landing-path") == "/mnt/erp"
    assert api.requested_names == ["kv-cred"]
    assert clients[0].vault_url == VAULT
    assert clients[0].credential is api._providers["kv-cred"]


def test_resolve_secret_creates_one_client_and_reuses_it(clients):
    platform, api = make_platform_with_vault()

    platform.resolve_secret("erp-landing-path")
    platform.resolve_secret("erp-landing-path")

    assert len(clients) == 1
    assert clients[0].requested == ["erp-landing-path", "erp-landing-path"]
    assert api.requested_names == ["kv-cred"]


def test_no_client_is_created_until_a_secret_is_needed(clients):
    make_platform_with_vault()

    assert clients == []


def test_a_custom_resolver_wins(clients):
    platform = DatabricksPlatform(
        catalog="prod",
        dbutils=FakeDbutils(),
        key_vault_url=VAULT,
        key_vault_credential="kv-cred",
        secret_resolver=lambda name: f"custom:{name}",
    )

    assert platform.resolve_secret("a-b") == "custom:a-b"
    assert clients == []


@pytest.mark.parametrize("name", ["with_underscore", "dotted.name", "has space", "", "a/b"])
def test_bad_secret_names_are_rejected_before_any_call(clients, name):
    platform, api = make_platform_with_vault()

    with pytest.raises(ValueError, match="Key Vault secret name"):
        platform.resolve_secret(name)

    assert clients == []
    assert api.requested_names == []


def test_resolving_without_any_configuration_names_the_options():
    with pytest.raises(ValueError, match="secret_resolver.*key_vault_url.*key_vault_credential"):
        make_platform().resolve_secret("x")


@pytest.mark.parametrize(
    "kwargs",
    [{"key_vault_url": VAULT}, {"key_vault_credential": "kv-cred"}],
)
def test_partial_key_vault_configuration_is_rejected(kwargs):
    with pytest.raises(ValueError, match="both or neither"):
        DatabricksPlatform(catalog="prod", **kwargs)


@pytest.mark.parametrize("url", ["http://kv.vault.azure.net", "kv.vault.azure.net", "https://"])
def test_key_vault_url_must_be_https_with_a_host(url):
    with pytest.raises(ValueError, match="https"):
        DatabricksPlatform(catalog="prod", key_vault_url=url, key_vault_credential="kv-cred")


def test_key_vault_route_requires_dbutils(clients):
    platform = DatabricksPlatform(
        catalog="prod", key_vault_url=VAULT, key_vault_credential="kv-cred"
    )

    with pytest.raises(ValueError, match="dbutils"):
        platform.resolve_secret("erp-landing-path")


def test_qualify_table_name_prepends_the_catalog():
    assert make_platform().qualify_table_name("bronze.orders") == "prod.bronze.orders"


def test_resolve_path_uses_a_unity_catalog_volume_under_the_catalog():
    result = make_platform().resolve_path("data/customer.csv", Path("/ignored"))

    assert result == "/Volumes/prod/pipetree/files/data/customer.csv"


def test_run_metadata_defaults_to_empty():
    assert make_platform().run_metadata() == {}


def test_run_metadata_returns_what_it_was_given():
    platform = DatabricksPlatform(
        catalog="prod",
        run_metadata={"job_id": "42"},
    )

    assert platform.run_metadata() == {"job_id": "42"}


class FakeAccessToken:
    """Shaped like azure.core.credentials.AccessToken (token, expires_on)."""

    def __init__(self, token: str) -> None:
        self.token = token
        self.expires_on = 0


class FakeTokenProvider:
    """Shaped like the azure-core `TokenCredential` that
    `dbutils.credentials.getServiceCredentialsProvider(name)` returns."""

    def __init__(self, token: str) -> None:
        self._token = token
        self.requested_scopes: list[tuple[str, ...]] = []

    def get_token(self, *scopes: str) -> FakeAccessToken:
        self.requested_scopes.append(scopes)
        return FakeAccessToken(self._token)


class FakeCredentialsAPI:
    def __init__(self, providers: dict[str, FakeTokenProvider]) -> None:
        self._providers = providers
        self.requested_names: list[str] = []

    def getServiceCredentialsProvider(self, name: str) -> FakeTokenProvider:  # noqa: N802 - matches Databricks' real API
        self.requested_names.append(name)
        return self._providers[name]


def make_platform_with_service_credentials(
    providers: dict[str, FakeTokenProvider], service_credentials: dict[str, str]
) -> tuple[DatabricksPlatform, FakeCredentialsAPI]:
    dbutils = FakeDbutils()
    credentials_api = FakeCredentialsAPI(providers)
    dbutils.credentials = credentials_api
    platform = DatabricksPlatform(
        dbutils=dbutils,
        catalog="prod",
        service_credentials=service_credentials,
    )
    return platform, credentials_api


def test_acquire_token_resolves_through_the_named_service_credential_provider():
    provider = FakeTokenProvider("sql-token")
    platform, credentials_api = make_platform_with_service_credentials(
        providers={"sql-cred": provider},
        service_credentials={"https://database.windows.net/": "sql-cred"},
    )

    assert platform.acquire_token("https://database.windows.net/") == "sql-token"
    assert credentials_api.requested_names == ["sql-cred"]


def test_acquire_token_requests_the_resource_default_scope():
    # The provider is an azure-core TokenCredential: get_token(*scopes)
    # wants a v2 scope, `<resource>/.default` - same shape as LocalPlatform.
    provider = FakeTokenProvider("t")
    platform, _ = make_platform_with_service_credentials(
        providers={"sql-cred": provider, "kusto-cred": provider},
        service_credentials={
            "https://database.windows.net/": "sql-cred",
            "https://api.kusto.windows.net": "kusto-cred",
        },
    )

    platform.acquire_token("https://database.windows.net/")
    platform.acquire_token("https://api.kusto.windows.net")

    assert provider.requested_scopes == [
        ("https://database.windows.net/.default",),
        ("https://api.kusto.windows.net/.default",),
    ]


def test_acquire_token_raises_for_an_unconfigured_resource():
    platform, _ = make_platform_with_service_credentials(providers={}, service_credentials={})

    with pytest.raises(ValueError, match="database.windows.net"):
        platform.acquire_token("https://database.windows.net/")


def test_acquire_token_raises_without_dbutils():
    platform = DatabricksPlatform(
        catalog="prod",
        service_credentials={"https://database.windows.net/": "sql-cred"},
    )

    with pytest.raises(ValueError, match="dbutils"):
        platform.acquire_token("https://database.windows.net/")
