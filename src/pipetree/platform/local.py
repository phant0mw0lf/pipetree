"""The local Platform: a laptop, or CI with no real secret store or catalog."""

from __future__ import annotations

import os
from pathlib import Path


class LocalPlatform:
    name = "local"

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


def _env_var_name(secret_name: str) -> str:
    return secret_name.upper().replace("-", "_").replace(" ", "_")
