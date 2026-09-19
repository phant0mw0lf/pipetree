"""The end-of-run notification seam.

`ConsoleNotifier` is the default - it just prints the digest. A concrete
`WebhookNotifier` (Teams/Slack) is a real HTTP dependency this core package
deliberately doesn't take on; it's a ~20-line class against this same
protocol once there's a workspace to post to (parts 3/4).
"""

from __future__ import annotations

import sys
from typing import Protocol, TextIO, runtime_checkable

from pipetree.executor.status import RunDigest, TableStatus


@runtime_checkable
class Notifier(Protocol):
    def notify(self, digest: RunDigest) -> None: ...


def format_digest(digest: RunDigest) -> str:
    outcome = "SUCCEEDED" if digest.succeeded else "FAILED"
    lines = [f"Run {digest.execution_id}: {outcome} (exit code {digest.exit_code})"]

    for fqn, result in sorted(digest.results.items()):
        line = (
            f"  {result.status.value:<18} {fqn:<30} "
            f"attempts={result.attempts} {result.duration_ms}ms"
        )
        if result.status == TableStatus.FAILED:
            line += f"  {result.error_type}: {result.error_message}"
        lines.append(line)

    return "\n".join(lines)


class ConsoleNotifier:
    def __init__(self, *, stream: TextIO | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout

    def notify(self, digest: RunDigest) -> None:
        print(format_digest(digest), file=self._stream)
