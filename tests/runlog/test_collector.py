from pipetree.executor.status import RunDigest, TableResult, TableStatus
from pipetree.runlog.collector import build_run_log_rows

from ..helpers import make_graph


def test_build_run_log_rows_includes_core_fields_from_graph_and_result():
    graph = make_graph({"bronze.orders": "scd1"})
    digest = RunDigest(
        execution_id=250908143012456,
        results={
            "bronze.orders": TableResult(
                table_fqn="bronze.orders",
                status=TableStatus.SUCCEEDED,
                attempts=1,
                started_at=250908143012456,
                ended_at=250908143012999,
                duration_ms=543,
            )
        },
    )

    rows = build_run_log_rows(digest, graph)

    assert rows == [
        {
            "_execution_id": 250908143012456,
            "table_fqn": "bronze.orders",
            "layer": "bronze",
            "strategy": "scd1",
            "status": "succeeded",
            "attempts": 1,
            "started_at": 250908143012456,
            "ended_at": 250908143012999,
            "duration_ms": 543,
            "rows_written": None,
            "duplicates_dropped": None,
            "schema_changes": None,
            "error_type": None,
            "error_message": None,
        }
    ]


def test_build_run_log_rows_pulls_details_from_adapter_result():
    graph = make_graph({"bronze.orders": "scd1"})
    digest = RunDigest(
        execution_id=1,
        results={
            "bronze.orders": TableResult(
                table_fqn="bronze.orders",
                status=TableStatus.SUCCEEDED,
                attempts=1,
                started_at=1,
                ended_at=2,
                duration_ms=1,
                details={
                    "rows_written": 42,
                    "duplicates_dropped": 3,
                    "schema_changes": ["added: x"],
                },
            )
        },
    )

    row = build_run_log_rows(digest, graph)[0]

    assert row["rows_written"] == 42
    assert row["duplicates_dropped"] == 3
    assert row["schema_changes"] == ["added: x"]


def test_build_run_log_rows_includes_error_info_for_failed_tables():
    graph = make_graph({"silver.bad": "replace"})
    digest = RunDigest(
        execution_id=1,
        results={
            "silver.bad": TableResult(
                table_fqn="silver.bad",
                status=TableStatus.FAILED,
                attempts=1,
                started_at=1,
                ended_at=2,
                duration_ms=1,
                error_type="ValueError",
                error_message="bad schema",
            )
        },
    )

    row = build_run_log_rows(digest, graph)[0]

    assert row["status"] == "failed"
    assert row["error_type"] == "ValueError"
    assert row["error_message"] == "bad schema"


def test_build_run_log_rows_covers_every_table_in_the_digest():
    graph = make_graph({"bronze.a": "scd1", "silver.b": "replace"})
    digest = RunDigest(
        execution_id=1,
        results={
            "bronze.a": TableResult(
                table_fqn="bronze.a",
                status=TableStatus.SUCCEEDED,
                attempts=1,
                started_at=1,
                ended_at=2,
                duration_ms=1,
            ),
            "silver.b": TableResult(
                table_fqn="silver.b",
                status=TableStatus.UPSTREAM_FAILED,
                attempts=0,
                started_at=1,
                ended_at=1,
                duration_ms=0,
            ),
        },
    )

    rows = build_run_log_rows(digest, graph)

    assert {row["table_fqn"] for row in rows} == {"bronze.a", "silver.b"}
