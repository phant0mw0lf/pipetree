"""Build the notifier a pipeline's `notifications` block asks for."""

from __future__ import annotations

from collections.abc import Callable

from pipetree.executor.status import RunDigest
from pipetree.model import Notifications, SecretRef
from pipetree.notify.acs_email import EmailNotifier, RunContext
from pipetree.notify.credential import credential_for
from pipetree.platform.base import Platform
from pipetree.runlog.notifier import CompositeNotifier, ConsoleNotifier, Notifier


def build_notifier(
    notifications: Notifications | None, platform: Platform, context: RunContext
) -> Notifier:
    """The console digest, plus every channel the config enables."""
    notifiers: list[Notifier] = [ConsoleNotifier()]
    if notifications is not None and notifications.email is not None:
        email = notifications.email
        endpoint = _endpoint(email.endpoint, platform)

        def make_email() -> Notifier:
            return EmailNotifier(
                endpoint=endpoint,
                sender=email.sender,
                to=list(email.to),
                credential=lambda: credential_for(platform),
                on=email.on,
                context=context,
            )

        notifiers.append(_OnFailure(make_email))
    return CompositeNotifier(notifiers)


class _OnFailure:
    """Builds its notifier (SDK check, credential, secrets) only for a run that
    is not succeeded, so a successful run touches none of it."""

    def __init__(self, factory: Callable[[], Notifier]) -> None:
        self._factory = factory

    def notify(self, digest: RunDigest) -> None:
        if not digest.succeeded:
            self._factory().notify(digest)


def _endpoint(value: str | SecretRef, platform: Platform) -> str | Callable[[], str]:
    """A literal as is; a secret reference resolved only when a mail is sent."""
    if isinstance(value, SecretRef):
        name = value.secret
        return lambda: platform.resolve_secret(name)
    return value
