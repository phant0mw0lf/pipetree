import datetime as dt

from pipetree.executor.execution_id import generate_execution_id, timestamp_bigint


def test_timestamp_bigint_matches_the_post_example():
    # "250908143012456 is 2025-09-08, 14:30:12.456 UTC"
    when = dt.datetime(2025, 9, 8, 14, 30, 12, 456_000, tzinfo=dt.UTC)

    assert timestamp_bigint(when) == 250908143012456


def test_timestamp_bigint_converts_non_utc_timezones_to_utc():
    # 14:30:12 in UTC+2 is 12:30:12 UTC
    when = dt.datetime(2025, 9, 8, 14, 30, 12, 0, tzinfo=dt.timezone(dt.timedelta(hours=2)))

    assert timestamp_bigint(when) == 250908123012000


def test_timestamp_bigint_defaults_to_now_when_not_given():
    before = dt.datetime.now(dt.UTC)
    result = timestamp_bigint()
    after = dt.datetime.now(dt.UTC)

    assert timestamp_bigint(before) <= result <= timestamp_bigint(after)


def test_generate_execution_id_is_a_timestamp_bigint():
    when = dt.datetime(2025, 9, 8, 14, 30, 12, 456_000, tzinfo=dt.UTC)

    assert generate_execution_id(when) == timestamp_bigint(when)
