"""The Fabric Platform: notebookutils secrets, the attached lakehouse's
Files mount, lakehouse table naming (already `schema.table` - no third
level to add, unlike Unity Catalog).

`resolve_path` returns a path relative to the default attached lakehouse
(`Files/...`), which works from inside the same notebook session. A
fully-qualified `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/
<lakehouse>.Lakehouse/Files/...` URI is for cross-workspace access - the
caller can build one if it needs that, this class doesn't assume it.

`run_metadata`, like on `DatabricksPlatform`, is supplied by the caller
rather than pulled from `notebookutils.runtime.context` here - the exact
shape of that context needs checking against a real workspace.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


class FabricPlatform:
    name = "fabric"

    def __init__(
        self,
        notebookutils: Any,
        *,
        key_vault_url: str,
        run_metadata: dict[str, str] | None = None,
    ) -> None:
        self._notebookutils = notebookutils
        self._key_vault_url = key_vault_url
        self._run_metadata = dict(run_metadata) if run_metadata else {}

    def resolve_secret(self, name: str) -> str:
        return self._notebookutils.credentials.getSecret(self._key_vault_url, name)

    def qualify_table_name(self, fqn: str) -> str:
        return fqn

    def resolve_path(self, relative_path: str, base_dir: Path) -> str:  # noqa: ARG002 - base_dir is a local-only concept
        return f"Files/{relative_path}"

    def run_metadata(self) -> dict[str, str]:
        return dict(self._run_metadata)
