import random

import pytest

from pipetree.executor.retry import RetryPolicy, Throttled, backoff_delay


def test_default_policy_classifies_timeout_as_transient():
    policy = RetryPolicy()
    assert policy.classify(TimeoutError("slow")) is True


def test_default_policy_classifies_connection_error_as_transient():
    policy = RetryPolicy()
    assert policy.classify(ConnectionError("reset")) is True


def test_default_policy_classifies_throttled_as_transient():
    policy = RetryPolicy()
    assert policy.classify(Throttled("429")) is True


def test_default_policy_classifies_value_error_as_not_transient():
    policy = RetryPolicy()
    assert policy.classify(ValueError("bad config")) is False


def test_custom_predicate_extends_classification():
    policy = RetryPolicy(is_transient=lambda exc: "retry-me" in str(exc))
    assert policy.classify(RuntimeError("please retry-me")) is True
    assert policy.classify(RuntimeError("nope")) is False


def test_custom_exception_types_extend_classification():
    class FlakyError(Exception):
        pass

    policy = RetryPolicy(transient_exceptions=(FlakyError,))
    assert policy.classify(FlakyError()) is True
    assert policy.classify(TimeoutError()) is False  # replaces, doesn't extend, the default set


def test_backoff_delay_is_exponential_and_capped():
    policy = RetryPolicy(base_delay=1.0, max_delay=10.0)
    rng = random.Random(0)

    # attempt 1: cap = min(10, 1*2^1) = 2 -> delay in [0, 2]
    assert 0 <= backoff_delay(1, policy, rng=rng) <= 2
    # attempt 5: cap = min(10, 1*2^5) = 10 (capped) -> delay in [0, 10]
    assert 0 <= backoff_delay(5, policy, rng=rng) <= 10


def test_backoff_delay_is_never_negative_or_above_cap():
    policy = RetryPolicy(base_delay=0.5, max_delay=5.0)
    rng = random.Random(42)

    for attempt in range(1, 10):
        delay = backoff_delay(attempt, policy, rng=rng)
        assert 0 <= delay <= 5.0


def test_max_attempts_defaults_to_three():
    assert RetryPolicy().max_attempts == 3


@pytest.mark.parametrize("attempts_so_far,max_attempts,expected", [(1, 3, True), (3, 3, False)])
def test_can_retry_respects_attempt_limit(attempts_so_far, max_attempts, expected):
    policy = RetryPolicy(max_attempts=max_attempts)
    assert policy.can_retry(attempts_so_far, TimeoutError()) is expected
