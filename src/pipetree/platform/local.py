"""The local Platform: a laptop, or CI with no real secret store or catalog.

`acquire_token` uses `azure-identity`'s `DefaultAzureCredential` by
default - the operator's own `az login` locally, or workload identity in
CI - so the dev-tier loop stays as secretless as the real platforms.
`azure-identity` is only imported when a token is actually requested and
no credential was injected, keeping it an optional dependency (the
`azure` extra) for everyone who doesn't need it.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


class LocalPlatform:
    name = "local"

    def __init__(self, *, token_credential: Any = None) -> None:
        self._token_credential = token_credential

    def resolve_secret(self, name: str) -> str:
        env_var = _env_var_name(name)
        value = os.environ.get(env_var)
        if value is None:
            raise KeyError(
                f"secret {name!r}: environment variable {env_var} is not set "
                "(local platform resolves secrets from the environment)"
            )
        return value

    def qualify_table_name(self, fqn: str) -> str:
        return fqn

    def resolve_path(self, relative_path: str, base_dir: Path) -> str:
        return str(base_dir / relative_path)

    def run_metadata(self) -> dict[str, str]:
        return {}

    def acquire_token(self, resource: str) -> str:
        credential = self._token_credential
        if credential is None:
            from azure.identity import DefaultAzureCredential  # type: ignore[import-untyped]

            credential = DefaultAzureCredential()
        return credential.get_token(f"{resource.rstrip('/')}/.default").token


def _env_var_name(secret_name: str) -> str:
    return secret_name.upper().replace("-", "_").replace(" ", "_")
