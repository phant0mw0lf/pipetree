"""Email a failed run through Azure Communication Services.

One mail per failed run: the subject names the failed tables, the body says
what ran into errors (error type, message, attempts), which tables were
skipped because of them, and the notes of every table. Authentication is a
token credential (see `pipetree.notify.credential`); there is no connection
string. `azure-communication-email` (the `azure` extra) is imported when the
mail is sent, never when pipetree is imported.
"""

from __future__ import annotations

import html
import importlib.util
import logging
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pipetree.executor.status import RunDigest, TableResult, TableStatus

_logger = logging.getLogger("pipetree")

MAX_ERROR_CHARS = 500
DEFAULT_WAIT_SECONDS = 30.0
_SUPPORTED_ON = ("failure",)


@dataclass(frozen=True)
class RunContext:
    """What the digest does not carry: how the run was requested and where it ran."""

    select: list[str] | None = None
    with_dependents: bool = False
    with_ancestors: bool = False
    init: bool = False
    run_metadata: dict[str, str] = field(default_factory=dict)


def _create_email_client(endpoint: str, credential: Any) -> Any:
    """Build the ACS email client. A module-level function so tests can replace it."""
    from azure.communication.email import EmailClient  # type: ignore[import-untyped]

    return EmailClient(endpoint, credential)


def _sdk_available() -> bool:
    try:
        return importlib.util.find_spec("azure.communication.email") is not None
    except ImportError:
        return False


def _failed(digest: RunDigest) -> list[TableResult]:
    return sorted(
        (r for r in digest.results.values() if r.status == TableStatus.FAILED),
        key=lambda r: r.table_fqn,
    )


def _upstream_failed(digest: RunDigest) -> list[TableResult]:
    return sorted(
        (r for r in digest.results.values() if r.status == TableStatus.UPSTREAM_FAILED),
        key=lambda r: r.table_fqn,
    )


def _truncate(text: str | None) -> str:
    text = text or ""
    return text if len(text) <= MAX_ERROR_CHARS else text[:MAX_ERROR_CHARS] + "..."


def format_subject(digest: RunDigest) -> str:
    failed = _failed(digest)
    upstream = _upstream_failed(digest)
    names = ", ".join(r.table_fqn for r in failed[:3])
    subject = (
        f"[pipetree] FAILED run {digest.execution_id}: "
        f"{len(failed)} failed, {len(upstream)} upstream_failed"
    )
    return f"{subject} - {names}" if names else subject


def _counts(digest: RunDigest) -> str:
    by_status: dict[TableStatus, int] = defaultdict(int)
    for result in digest.results.values():
        by_status[result.status] += 1
    succeeded = by_status[TableStatus.SUCCEEDED] + by_status[TableStatus.RETRIED_SUCCEEDED]
    parts = [
        f"{by_status[TableStatus.FAILED]} failed",
        f"{by_status[TableStatus.UPSTREAM_FAILED]} upstream_failed",
        f"{succeeded} succeeded",
    ]
    if skipped := by_status[TableStatus.SKIPPED]:
        parts.append(f"{skipped} skipped")
    return ", ".join(parts)


def _selection(context: RunContext) -> str | None:
    flags = []
    if context.with_dependents:
        flags.append("with dependents")
    if context.with_ancestors:
        flags.append("with ancestors")
    if context.init:
        flags.append("init (full reload)")
    if context.select is None and not flags:
        return None
    base = ", ".join(context.select) if context.select else "all tables"
    return f"{base} ({', '.join(flags)})" if flags else base


def _groups(digest: RunDigest) -> tuple[dict[str, list[str]], list[str]]:
    """Upstream-failed tables by the failed table that caused them, and the
    ones whose cause the digest does not record."""
    grouped: dict[str, list[str]] = defaultdict(list)
    ungrouped: list[str] = []
    for result in _upstream_failed(digest):
        ancestor = result.details.get("failed_ancestor")
        if ancestor in digest.results:
            grouped[ancestor].append(result.table_fqn)
        else:
            ungrouped.append(result.table_fqn)
    return grouped, ungrouped


def _all_notes(digest: RunDigest) -> list[tuple[str, list[str]]]:
    return [(fqn, result.notes) for fqn, result in sorted(digest.results.items()) if result.notes]


def format_text(digest: RunDigest, context: RunContext) -> str:
    grouped, ungrouped = _groups(digest)
    lines = [f"Run {digest.execution_id} FAILED", "", f"Tables: {_counts(digest)}"]
    if selection := _selection(context):
        lines.append(f"Selection: {selection}")
    lines += [f"{key}: {value}" for key, value in sorted(context.run_metadata.items())]

    lines += ["", "FAILED TABLES"]
    for result in _failed(digest):
        lines += [
            f"* {result.table_fqn}",
            f"  {result.error_type}: {_truncate(result.error_message)}",
            f"  attempts: {result.attempts}",
        ]
        if skipped := grouped.get(result.table_fqn):
            lines.append(f"  upstream_failed because of it: {', '.join(skipped)}")
    if ungrouped:
        lines += ["", f"UPSTREAM_FAILED (cause not recorded): {', '.join(ungrouped)}"]

    if notes := _all_notes(digest):
        lines += ["", "NOTES"]
        lines += [f"* {fqn}: {'; '.join(table_notes)}" for fqn, table_notes in notes]
    return "\n".join(lines) + "\n"


def format_html(digest: RunDigest, context: RunContext) -> str:
    esc = html.escape
    grouped, ungrouped = _groups(digest)
    parts = [
        '<html><body style="font-family: sans-serif; font-size: 14px;">',
        f"<h2>Run {esc(str(digest.execution_id))} FAILED</h2>",
        f"<p>Tables: {esc(_counts(digest))}</p>",
    ]
    meta = []
    if selection := _selection(context):
        meta.append(f"Selection: {esc(selection)}")
    meta += [f"{esc(k)}: {esc(v)}" for k, v in sorted(context.run_metadata.items())]
    if meta:
        parts.append("<p>" + "<br>".join(meta) + "</p>")

    parts.append("<h3>Failed tables</h3><ul>")
    for result in _failed(digest):
        item = (
            f"<li><b>{esc(result.table_fqn)}</b><br>"
            f"{esc(result.error_type or '')}: {esc(_truncate(result.error_message))}<br>"
            f"attempts: {result.attempts}"
        )
        if skipped := grouped.get(result.table_fqn):
            item += f"<br>upstream_failed because of it: {esc(', '.join(skipped))}"
        parts.append(item + "</li>")
    parts.append("</ul>")
    if ungrouped:
        parts.append(f"<p>upstream_failed (cause not recorded): {esc(', '.join(ungrouped))}</p>")

    if notes := _all_notes(digest):
        parts.append("<h3>Notes</h3><ul>")
        parts += [
            f"<li>{esc(fqn)}: {esc('; '.join(table_notes))}</li>" for fqn, table_notes in notes
        ]
        parts.append("</ul>")
    parts.append("</body></html>")
    return "\n".join(parts)


class EmailNotifier:
    """Sends one mail for a run that failed. `on="failure"` is the only mode."""

    def __init__(
        self,
        *,
        endpoint: str | Callable[[], str],
        sender: str,
        to: list[str],
        credential: Any | Callable[[], Any],
        on: str = "failure",
        context: RunContext | None = None,
        wait_seconds: float = DEFAULT_WAIT_SECONDS,
    ) -> None:
        if on not in _SUPPORTED_ON:
            raise ValueError(f"notification `on` must be one of {_SUPPORTED_ON}, got {on!r}")
        if not _sdk_available():
            raise ValueError(
                "email notifications need the azure-communication-email package: "
                "install pipetree with the `azure` extra (pip install 'pipetree[azure]')"
            )
        self._endpoint = endpoint
        self._sender = sender
        self._to = list(to)
        self._credential = credential
        self._context = context or RunContext()
        self._wait_seconds = min(wait_seconds, DEFAULT_WAIT_SECONDS)

    def notify(self, digest: RunDigest) -> None:
        if digest.succeeded:
            return
        endpoint = self._endpoint() if callable(self._endpoint) else self._endpoint
        credential = self._credential
        if callable(credential) and not hasattr(credential, "get_token"):
            credential = credential()  # built only when a mail is actually sent
        client = _create_email_client(endpoint, credential)
        message = {
            "senderAddress": self._sender,
            "recipients": {"to": [{"address": address} for address in self._to]},
            "content": {
                "subject": format_subject(digest),
                "plainText": format_text(digest, self._context),
                "html": format_html(digest, self._context),
            },
        }
        poller = client.begin_send(message)
        poller.result(timeout=self._wait_seconds)
        if not poller.done():
            _logger.warning(
                "failure email: delivery not confirmed within %ss; the run is unaffected",
                self._wait_seconds,
            )
