"""Build the notifier a pipeline's `notifications` block asks for."""

from __future__ import annotations

from collections.abc import Callable

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
        notifiers.append(
            EmailNotifier(
                endpoint=endpoint,
                sender=email.sender,
                to=list(email.to),
                credential=lambda: credential_for(platform),
                on=email.on,
                context=context,
            )
        )
    return CompositeNotifier(notifiers)


def _endpoint(value: str | SecretRef, platform: Platform) -> str | Callable[[], str]:
    """A literal as is; a secret reference resolved only when a mail is sent."""
    if isinstance(value, SecretRef):
        name = value.secret
        return lambda: platform.resolve_secret(name)
    return value
