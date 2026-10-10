"""The Databricks Platform: Unity Catalog naming, secret resolution, Volumes.

Secrets are read from Azure Key Vault directly, with the identity of a Unity
Catalog service credential. Nothing is stored in Databricks and no shared
Entra application is granted access to the vault. Provision once:

1. An Access Connector for Azure Databricks (a managed identity).
2. The role `Key Vault Secrets User` for that identity on the vault.
3. A Unity Catalog service credential created from the connector.
4. `ACCESS` on the service credential for the principal that runs the job.

Then pass `key_vault_url` and `key_vault_credential` (the credential's name),
plus `dbutils`. The platform creates one `SecretClient` on the first
`{secret: name}` and reuses it. `secret_resolver` replaces that route with
your own callable. Secret names must be Key Vault names: letters, digits and
hyphens only; anything else is rejected before a call is made. Reading
secrets needs `azure-keyvault-secrets` (the `azure` extra).

`acquire_token` is the token-based counterpart: `service_credentials`
maps each AAD resource to the name of a Unity Catalog service credential
(backed by the same Access Connector).
`dbutils.credentials.getServiceCredentialsProvider(name)` returns an
azure-core `TokenCredential`, and the token is
`provider.get_token("<resource>/.default").token`, the same scope shape
`LocalPlatform` requests. Requirements: Databricks Runtime 16.2+ (from
15.4 LTS, Python only), `ACCESS` on the credential, and not a SQL
warehouse. The `dbutils` provider is a driver-side API - it isn't
available inside UDFs, so a token minted here is a static string handed to
whatever runs on executors and isn't refreshed there. The same applies to
the service credential used for Key Vault.

`run_metadata` is supplied by the caller rather than pulled from
`dbutils` here: job and run id come from
`dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()`.
The job entrypoint (`examples/databricks/`) is the place to read those
tags and pass them in; this class just carries whatever it's given.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_SECRET_NAME = re.compile(r"[A-Za-z0-9-]+")


def _create_secret_client(vault_url: str, credential: Any) -> Any:
    """Build the Key Vault client. A module-level function so tests can replace it."""
    from azure.keyvault.secrets import SecretClient  # type: ignore[import-untyped]

    return SecretClient(vault_url=vault_url, credential=credential)


class DatabricksPlatform:
    name = "databricks"

    def __init__(
        self,
        *,
        catalog: str,
        dbutils: Any = None,
        key_vault_url: str | None = None,
        key_vault_credential: str | None = None,
        secret_resolver: Callable[[str], str] | None = None,
        service_credentials: dict[str, str] | None = None,
        run_metadata: dict[str, str] | None = None,
    ) -> None:
        if (key_vault_url is None) != (key_vault_credential is None):
            raise ValueError(
                "key_vault_url and key_vault_credential go together: set both or neither"
            )
        if key_vault_url is not None:
            parsed = urlparse(key_vault_url)
            if parsed.scheme != "https" or not parsed.hostname:
                raise ValueError(
                    f"key_vault_url must be an https URL with a host, got {key_vault_url!r}"
                )

        self._secret_resolver = secret_resolver
        self._key_vault_url = key_vault_url
        self._key_vault_credential = key_vault_credential
        self._secret_client: Any = None
        self._catalog = catalog
        self._dbutils = dbutils
        self._service_credentials = dict(service_credentials) if service_credentials else {}
        self._run_metadata = dict(run_metadata) if run_metadata else {}

    def resolve_secret(self, name: str) -> str:
        if self._secret_resolver is not None:
            return self._secret_resolver(name)
        if self._key_vault_url is None or self._key_vault_credential is None:
            raise ValueError(
                f"cannot resolve secret {name!r}: pass secret_resolver, or key_vault_url and "
                "key_vault_credential (with dbutils) to DatabricksPlatform"
            )
        if self._dbutils is None:
            raise ValueError(
                "reading Key Vault secrets requires dbutils (a real Databricks runtime)"
            )
        if not _SECRET_NAME.fullmatch(name):
            raise ValueError(
                f"secret name {name!r} is not a Key Vault secret name "
                "(letters, digits and hyphens only)"
            )
        if self._secret_client is None:
            credential = self._dbutils.credentials.getServiceCredentialsProvider(
                self._key_vault_credential
            )
            self._secret_client = _create_secret_client(self._key_vault_url, credential)
        return self._secret_client.get_secret(name).value

    def qualify_table_name(self, fqn: str) -> str:
        return f"{self._catalog}.{fqn}"

    def resolve_path(self, relative_path: str, base_dir: Path) -> str:  # noqa: ARG002 - base_dir is a local-only concept
        return f"/Volumes/{self._catalog}/pipetree/files/{relative_path}"

    def run_metadata(self) -> dict[str, str]:
        return dict(self._run_metadata)

    def acquire_token(self, resource: str) -> str:
        try:
            credential_name = self._service_credentials[resource]
        except KeyError:
            raise ValueError(
                f"no Unity Catalog service credential configured for resource "
                f"{resource!r} (configured: {sorted(self._service_credentials)})"
            ) from None
        if self._dbutils is None:
            raise ValueError("acquire_token requires dbutils (a real Databricks runtime)")
        provider = self._dbutils.credentials.getServiceCredentialsProvider(credential_name)
        return provider.get_token(f"{resource.rstrip('/')}/.default").token
