"""The credential the email notifier signs in with, and where it comes from.

`PlatformCredential` adapts `Platform.acquire_token` to the azure-core
`TokenCredential` shape, so the email client authenticates with the same
secretless identity the platform already uses for other resources. Platforms
without a token path for Azure Communication Services fall back to
`DefaultAzureCredential` (local `az login`, a managed identity, workload
identity).
"""

from __future__ import annotations

import base64
import json
import time
from typing import Any

# The audience of Azure Communication Services. The email SDK asks its
# credential for the scope "https://communication.azure.com//.default".
ACS_RESOURCE = "https://communication.azure.com"

# Platforms that can mint a token for ACS, by `Platform.name`.
#   databricks: a Unity Catalog service credential listed under
#     `service_credentials[ACS_RESOURCE]`.
# Fabric's `notebookutils.credentials.getToken` documents only the audience
# keys storage, pbi, keyvault and kusto, none of which is ACS, so Fabric and
# local use DefaultAzureCredential.
_PLATFORM_RESOURCES = {"databricks": ACS_RESOURCE}

_OPAQUE_TOKEN_LIFETIME_S = 300


class PlatformCredential:
    """A `TokenCredential` backed by `platform.acquire_token(resource)`."""

    def __init__(self, platform: Any, resource: str = ACS_RESOURCE) -> None:
        self._platform = platform
        self._resource = resource

    def get_token(self, *scopes: str, **kwargs: Any) -> Any:  # noqa: ARG002
        from azure.core.credentials import AccessToken

        token = self._platform.acquire_token(self._resource)
        expires_on = _jwt_expiry(token) or int(time.time()) + _OPAQUE_TOKEN_LIFETIME_S
        return AccessToken(token, expires_on)


def credential_for(platform: Any) -> Any:
    """The token credential for the email client on `platform`."""
    resource = _PLATFORM_RESOURCES.get(getattr(platform, "name", ""))
    can_acquire = getattr(platform, "can_acquire_token", None)
    if resource is not None and can_acquire is not None and can_acquire(resource):
        return PlatformCredential(platform, resource)
    return _default_credential()


def _default_credential() -> Any:
    from azure.identity import DefaultAzureCredential  # type: ignore[import-untyped]

    return DefaultAzureCredential()


def _jwt_expiry(token: str) -> int | None:
    try:
        payload = token.split(".")[1]
        data = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        return int(data["exp"])
    except Exception:
        return None
