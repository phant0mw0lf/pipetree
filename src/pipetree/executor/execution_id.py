"""The `_execution_id` / timestamp format from part 1: UTC, `YYMMddHHmmssSSS`,
stored as a bigint. Not epoch millis - the point is that a row and the run
that wrote it are readable at a glance in a debug query, without converting
anything. Two-digit year, so don't treat the raw value as a unique id when
runs can start in the same millisecond.
"""

from __future__ import annotations

import datetime as dt


def timestamp_bigint(now: dt.datetime | None = None) -> int:
    """UTC `YYMMddHHmmssSSS` as an int. Naive datetimes are assumed UTC."""
    if now is None:
        now = dt.datetime.now(dt.UTC)
    elif now.tzinfo is not None:
        now = now.astimezone(dt.UTC)

    millis = now.microsecond // 1000
    return int(now.strftime("%y%m%d%H%M%S") + f"{millis:03d}")


def generate_execution_id(now: dt.datetime | None = None) -> int:
    """One `_execution_id` per run - the same bigint format as the timestamps,
    so a row and the run that wrote it line up in a query without converting."""
    return timestamp_bigint(now)
