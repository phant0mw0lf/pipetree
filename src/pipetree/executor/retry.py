"""Transient-error classification and backoff.

Only transient errors are retried: throttling/429, timeouts, connection
resets. Schema, config and logic errors never are - retrying those just
wastes the attempt budget on something that will never succeed.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field


class Throttled(Exception):
    """A source or sink pushed back with a rate limit / HTTP 429."""


_DEFAULT_TRANSIENT_EXCEPTIONS: tuple[type[Exception], ...] = (
    Throttled,
    TimeoutError,
    ConnectionError,
)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay: float = 0.5
    max_delay: float = 30.0
    transient_exceptions: tuple[type[Exception], ...] = field(
        default_factory=lambda: _DEFAULT_TRANSIENT_EXCEPTIONS
    )
    is_transient: Callable[[Exception], bool] | None = None

    def classify(self, exc: Exception) -> bool:
        if isinstance(exc, self.transient_exceptions):
            return True
        if self.is_transient is not None:
            return self.is_transient(exc)
        return False

    def can_retry(self, attempts_so_far: int, exc: Exception) -> bool:
        return self.classify(exc) and attempts_so_far < self.max_attempts


def backoff_delay(attempt: int, policy: RetryPolicy, *, rng: random.Random | None = None) -> float:
    """Exponential backoff with full jitter: uniform(0, min(max_delay, base * 2**attempt))."""
    rng = rng if rng is not None else random.Random()
    cap = min(policy.max_delay, policy.base_delay * (2**attempt))
    return rng.uniform(0, cap)
