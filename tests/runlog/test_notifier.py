import io

from pipetree.executor.status import RunDigest, TableResult, TableStatus
from pipetree.runlog.notifier import ConsoleNotifier, Notifier, format_digest


def digest_with(**statuses: TableStatus) -> RunDigest:
    return RunDigest(
        execution_id=250908143012456,
        results={
            fqn: TableResult(
                table_fqn=fqn,
                status=status,
                attempts=1,
                started_at=1,
                ended_at=2,
                duration_ms=100,
                error_type="ValueError" if status == TableStatus.FAILED else None,
                error_message="bad schema" if status == TableStatus.FAILED else None,
            )
            for fqn, status in statuses.items()
        },
    )


def test_console_notifier_satisfies_the_notifier_protocol():
    assert isinstance(ConsoleNotifier(), Notifier)


def test_format_digest_reports_overall_success():
    digest = digest_with(**{"bronze.orders": TableStatus.SUCCEEDED})

    text = format_digest(digest)

    assert "250908143012456" in text
    assert "SUCCEEDED" in text.upper()


def test_format_digest_reports_overall_failure():
    digest = digest_with(**{"silver.bad": TableStatus.FAILED})

    text = format_digest(digest)

    assert "FAILED" in text.upper()


def test_format_digest_lists_each_table_with_its_status():
    digest = digest_with(
        **{
            "bronze.orders": TableStatus.SUCCEEDED,
            "silver.orders": TableStatus.RETRIED_SUCCEEDED,
            "gold.dim_customer": TableStatus.UPSTREAM_FAILED,
        }
    )

    text = format_digest(digest)

    assert "bronze.orders" in text
    assert "silver.orders" in text
    assert "gold.dim_customer" in text


def test_format_digest_includes_error_message_for_failed_tables():
    digest = digest_with(**{"silver.bad": TableStatus.FAILED})

    text = format_digest(digest)

    assert "ValueError" in text
    assert "bad schema" in text


def test_console_notifier_writes_the_formatted_digest_to_its_stream():
    stream = io.StringIO()
    notifier = ConsoleNotifier(stream=stream)
    digest = digest_with(**{"bronze.orders": TableStatus.SUCCEEDED})

    notifier.notify(digest)

    assert format_digest(digest) in stream.getvalue()
