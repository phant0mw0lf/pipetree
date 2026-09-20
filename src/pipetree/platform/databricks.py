"""The Databricks Platform: Unity Catalog naming, secret resolution, Volumes.

Secret resolution is pluggable, defaulting to `dbutils.secrets.get()`
against a **Unity-Catalog-backed** secret scope (`databricks secrets
create-scope --scope <name> --scope-backend-type UC`) - not a legacy
Azure-Key-Vault-backed secret scope, which Databricks itself now treats as
legacy and less secure. To read Key Vault directly instead, provision an
Access Connector for Azure Databricks (a UC-governed managed identity) and
pass a `secret_resolver` callable that reads through it - the default
dbutils path is convenience, not the only supported route.

`run_metadata` is supplied by the caller rather than pulled from
`dbutils` here - extracting job/run id reliably means reaching into
`dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()`,
Java-interop that's easy to get subtly wrong without a real workspace to
check it against. The DAB job entrypoint (`examples/databricks/`) is the
right place to pull those tags and pass them in; this class just carries
whatever it's given.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any


class DatabricksPlatform:
    name = "databricks"

    def __init__(
        self,
        *,
        catalog: str,
        dbutils: Any = None,
        secret_scope: str | None = None,
        secret_resolver: Callable[[str], str] | None = None,
        run_metadata: dict[str, str] | None = None,
    ) -> None:
        if secret_resolver is None:
            if dbutils is None or secret_scope is None:
                raise ValueError(
                    "provide secret_resolver, or both dbutils and secret_scope "
                    "(a Unity-Catalog-backed secret scope, not a legacy "
                    "Azure-Key-Vault-backed one)"
                )
            secret_resolver = _dbutils_secret_resolver(dbutils, secret_scope)

        self._secret_resolver = secret_resolver
        self._catalog = catalog
        self._run_metadata = dict(run_metadata) if run_metadata else {}

    def resolve_secret(self, name: str) -> str:
        return self._secret_resolver(name)

    def qualify_table_name(self, fqn: str) -> str:
        return f"{self._catalog}.{fqn}"

    def resolve_path(self, relative_path: str, base_dir: Path) -> str:  # noqa: ARG002 - base_dir is a local-only concept
        return f"/Volumes/{self._catalog}/pipetree/files/{relative_path}"

    def run_metadata(self) -> dict[str, str]:
        return dict(self._run_metadata)


def _dbutils_secret_resolver(dbutils: Any, secret_scope: str) -> Callable[[str], str]:
    def resolver(name: str) -> str:
        return dbutils.secrets.get(scope=secret_scope, key=name)

    return resolver
