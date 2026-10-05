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

`acquire_token` delegates to `notebookutils.credentials.getToken(audience)`
- no stored secret. Fabric documents `getToken` against a short, fixed set
of audience keys (`storage`, `pbi`, `keyvault`, `kusto`; NotebookUtils
credentials docs) and doesn't publish an allow-list of full URIs, so:

- `https://api.kusto.windows.net` (Azure Data Explorer's cluster-independent
  audience) is mapped to the `kusto` key;
- everything else is passed through unchanged - including a cluster URI
  like `https://<cluster>.kusto.windows.net`, which Microsoft's
  Synapse-to-Fabric migration guide passes to `getToken` as-is, and the
  bare keys themselves. No URI is mapped to `storage`/`keyvault`/`pbi`,
  because Fabric's docs don't say which URI each key stands for.

A `getToken` failure is re-raised as a `RuntimeError` naming the resource;
an arbitrary app-registration audience (a custom API's client id) is not
in Fabric's documented set and isn't expected to work.

**Which identity the token belongs to:** the notebook's *executing*
identity, not the workspace identity. Per Fabric's "Security context of
running notebook": an interactive run is the signed-in user, a scheduled
run is whoever created or last updated the schedule, and a pipeline
Notebook activity is the pipeline's last modifier - unless that activity's
Connection (documented as "the authentication method for the notebook
run") is the Workspace Identity, which needs the tenant setting "Service
principals can call Fabric public APIs" and the identity as Contributor on
the workspace. Only in that mode do grants made to the workspace identity
apply. Not yet exercised against a real workspace.
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

    def acquire_token(self, resource: str) -> str:
        audience = _AUDIENCE_KEYS.get(resource.rstrip("/"), resource)
        try:
            return self._notebookutils.credentials.getToken(audience)
        except Exception as exc:
            sent = "" if audience == resource else f" (sent to getToken as {audience!r})"
            raise RuntimeError(
                f"notebookutils.credentials.getToken failed for resource {resource!r}{sent}. "
                "Fabric's getToken supports a limited set of audiences - documented keys: "
                f"{', '.join(_DOCUMENTED_KEYS)}; a Kusto cluster URI is also documented to "
                "work. The token belongs to the notebook's executing identity, which must "
                f"also be authorized for the target. Cause: {exc}"
            ) from exc


# notebookutils.credentials.getToken's documented audience keys
# (learn.microsoft.com/fabric/data-engineering/notebookutils/notebookutils-credentials).
_DOCUMENTED_KEYS = ("storage", "pbi", "keyvault", "kusto")

# Resource URIs mapped onto a documented key. Only ADX's generic audience is
# mapped; anything else (cluster URIs included) goes to getToken unchanged.
_AUDIENCE_KEYS = {"https://api.kusto.windows.net": "kusto"}
