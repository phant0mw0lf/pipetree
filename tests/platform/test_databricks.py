from pathlib import Path

import pytest

from pipetree.platform.base import Platform
from pipetree.platform.databricks import DatabricksPlatform


class FakeSecrets:
    def __init__(self, values: dict[tuple[str, str], str]) -> None:
        self._values = values

    def get(self, scope: str, key: str) -> str:
        return self._values[(scope, key)]


class FakeDbutils:
    def __init__(self, secrets: FakeSecrets) -> None:
        self.secrets = secrets
        self.credentials: FakeCredentialsAPI | None = None


def make_platform(secret_values: dict[tuple[str, str], str] | None = None) -> DatabricksPlatform:
    dbutils = FakeDbutils(FakeSecrets(secret_values or {}))
    return DatabricksPlatform(dbutils=dbutils, catalog="prod", secret_scope="pipetree")


def test_satisfies_the_platform_protocol():
    assert isinstance(make_platform(), Platform)


def test_name_is_databricks():
    assert make_platform().name == "databricks"


def test_resolve_secret_uses_dbutils_secrets_with_the_configured_scope():
    # The scope is assumed Unity-Catalog-backed (`databricks secrets
    # create-scope --scope-backend-type UC`); dbutils.secrets.get() is the
    # same call site regardless of backend, so this exercises the default
    # wiring without needing a real UC-backed scope.
    platform = make_platform({("pipetree", "erp-landing-path"): "/mnt/erp"})

    assert platform.resolve_secret("erp-landing-path") == "/mnt/erp"


def test_resolve_secret_uses_a_custom_resolver_when_given():
    # The escape hatch for reading Key Vault directly through an Access
    # Connector for Azure Databricks instead of a secret scope at all -
    # a legacy Azure-Key-Vault-backed secret scope is exactly what this
    # avoids.
    platform = DatabricksPlatform(
        catalog="prod", secret_resolver=lambda name: f"from-key-vault:{name}"
    )

    assert platform.resolve_secret("erp-landing-path") == "from-key-vault:erp-landing-path"


def test_requires_either_a_secret_resolver_or_dbutils_and_scope():
    with pytest.raises(ValueError, match="secret_resolver"):
        DatabricksPlatform(catalog="prod")


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
        secret_resolver=lambda name: name,
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
    dbutils = FakeDbutils(FakeSecrets({}))
    credentials_api = FakeCredentialsAPI(providers)
    dbutils.credentials = credentials_api
    platform = DatabricksPlatform(
        dbutils=dbutils,
        catalog="prod",
        secret_scope="pipetree",
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
        secret_resolver=lambda name: name,
        service_credentials={"https://database.windows.net/": "sql-cred"},
    )

    with pytest.raises(ValueError, match="dbutils"):
        platform.acquire_token("https://database.windows.net/")
