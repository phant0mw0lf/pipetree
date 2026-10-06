from pathlib import Path

import pytest

from pipetree.platform.base import Platform
from pipetree.platform.fabric import FabricPlatform


class FakeCredentials:
    def __init__(self, values: dict[tuple[str, str], str]) -> None:
        self._values = values

    def getSecret(self, key_vault_url: str, secret_name: str) -> str:  # noqa: N802 - matches Fabric's real API
        return self._values[(key_vault_url, secret_name)]


class FakeNotebookUtils:
    def __init__(self, credentials: FakeCredentials) -> None:
        self.credentials = credentials


def make_platform(secret_values: dict[tuple[str, str], str] | None = None) -> FabricPlatform:
    notebookutils = FakeNotebookUtils(FakeCredentials(secret_values or {}))
    return FabricPlatform(notebookutils, key_vault_url="https://kv.vault.azure.net/")


def test_satisfies_the_platform_protocol():
    assert isinstance(make_platform(), Platform)


def test_name_is_fabric():
    assert make_platform().name == "fabric"


def test_resolve_secret_uses_notebookutils_credentials_with_the_configured_vault():
    platform = make_platform(
        {("https://kv.vault.azure.net/", "erp-landing-path"): "/lakehouse/erp"}
    )

    assert platform.resolve_secret("erp-landing-path") == "/lakehouse/erp"


def test_qualify_table_name_is_a_no_op():
    # Fabric lakehouse tables are already schema.table - no third level.
    assert make_platform().qualify_table_name("bronze.orders") == "bronze.orders"


def test_resolve_path_uses_the_attached_lakehouse_files_mount():
    result = make_platform().resolve_path("data/customer.csv", Path("/ignored"))

    assert result == "Files/data/customer.csv"


def test_run_metadata_defaults_to_empty():
    assert make_platform().run_metadata() == {}


def test_run_metadata_returns_what_it_was_given():
    notebookutils = FakeNotebookUtils(FakeCredentials({}))
    platform = FabricPlatform(
        notebookutils, key_vault_url="https://kv.vault.azure.net/", run_metadata={"run_id": "7"}
    )

    assert platform.run_metadata() == {"run_id": "7"}


class FakeCredentialsWithToken(FakeCredentials):
    def __init__(self, secret_values, token: str) -> None:
        super().__init__(secret_values)
        self._token = token
        self.requested_resource: str | None = None

    def getToken(self, resource: str) -> str:  # noqa: N802 - matches Fabric's real API
        self.requested_resource = resource
        return self._token


def make_token_platform(
    token: str = "fabric-token",
) -> tuple[FabricPlatform, FakeCredentialsWithToken]:
    credentials = FakeCredentialsWithToken({}, token=token)
    platform = FabricPlatform(
        FakeNotebookUtils(credentials), key_vault_url="https://kv.vault.azure.net/"
    )
    return platform, credentials


def test_acquire_token_delegates_to_notebookutils_get_token():
    platform, credentials = make_token_platform()

    result = platform.acquire_token("https://database.windows.net/")

    assert result == "fabric-token"
    assert credentials.requested_resource == "https://database.windows.net/"


@pytest.mark.parametrize(
    "resource", ["https://api.kusto.windows.net", "https://api.kusto.windows.net/"]
)
def test_acquire_token_maps_the_generic_adx_audience_to_the_kusto_key(resource):
    # `kusto` is getToken's documented audience key for Azure Data Explorer;
    # the cluster-independent `https://api.kusto.windows.net` URI isn't.
    platform, credentials = make_token_platform()

    platform.acquire_token(resource)

    assert credentials.requested_resource == "kusto"


@pytest.mark.parametrize(
    "resource",
    [
        # Microsoft's Synapse->Fabric migration guide passes the cluster URI as-is.
        "https://mycluster.westeurope.kusto.windows.net",
        "https://storage.azure.com/",
        "storage",
        "kusto",
    ],
)
def test_acquire_token_passes_other_resources_through_unchanged(resource):
    platform, credentials = make_token_platform()

    platform.acquire_token(resource)

    assert credentials.requested_resource == resource


def test_acquire_token_wraps_get_token_failures_with_the_resource_and_supported_set():
    class FailingCredentials(FakeCredentials):
        def getToken(self, resource: str) -> str:  # noqa: N802 - matches Fabric's real API
            raise Exception(f"{resource} is not a valid resource")  # noqa: TRY002 - stands in for a Py4J error

    platform = FabricPlatform(
        FakeNotebookUtils(FailingCredentials({})), key_vault_url="https://kv.vault.azure.net/"
    )

    with pytest.raises(RuntimeError, match="api://marketing-app") as excinfo:
        platform.acquire_token("api://marketing-app")

    message = str(excinfo.value)
    assert "limited set of audiences" in message
    assert "kusto" in message
    assert isinstance(excinfo.value.__cause__, Exception)
