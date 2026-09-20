from pathlib import Path

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
